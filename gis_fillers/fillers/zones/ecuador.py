import os
import requests
import zipfile
import logging
import csv
from psycopg2 import extras
import shapefile
import json
import subprocess
import topojson as tp
import geopandas as gpd

from .. import fillers


class EcuadorFiller(fillers.Filler):
    """
    This class fills in geographical data for Austria, with structure:
    zaehlsprengel < gemeinde < bezirk < bundesland
    Source is from STAT austria, in three files:
    GIS info: http://data.statistik.gv.at/data/OGDEXT_ZSP_1_STATISTIK_AUSTRIA_{YEAR}0101.zip
    population info: https://statistik.at/fileadmin/pages/405/Bevoelkerung_am_1.1.{YEAR}_nach_Zaehlsprengel__Gebietsstand_1.1.{YEAR}_.ods
    bezirk names: http://www.statistik.at/verzeichnis/reglisten/polbezirke.csv

    Be attentive to the issue year of the different sources, they need to match (typically GIS info is ahead one year if you take the latest).

    The simplified attribute is used to tell the filler to preprocess the shapefile and simplify the edges with mapshaper/topojson
    """

    def __init__(
        self,
        gis_info="https://data.humdata.org/dataset/ab3c7592-3b0c-41cd-999a-2919a6b243f2/resource/5b65ea45-5946-4b73-b38e-702ad8ad8a59/download/ecu_adm_inec_20190724_shp.zip",
        gis_info_name="ecu_adm_inec_20190724_shp",
        simplified=False,
        force=False,
        simplify_engine="topojson",
        **kwargs,
    ):
        self.force = force
        self.gis_info = gis_info
        self.gis_info_name = gis_info_name
        self.simplified = simplified
        if self.simplified:
            self.gis_type = "ecuador_simplified"
            self.simplify_engine = simplify_engine
            if self.simplify_engine == "mapshaper":
                try:
                    subprocess.check_output("mapshaper --version".split(" "))
                except FileNotFoundError:
                    raise FileNotFoundError(
                        "Mapshaper is not installed, please install for node.js with: npm install -g mapshaper"
                    )
        else:
            self.gis_type = "ecuador"
        fillers.Filler.__init__(self, name=self.gis_type, **kwargs)

    def prepare(self):
        if self.data_folder is None:
            self.data_folder = self.db.data_folder
        data_folder = self.data_folder

        # create folder if needed
        if not os.path.exists(data_folder):
            os.makedirs(data_folder)

        self.db.cursor.execute(
            """
			SELECT 1
					FROM gis_data gd
						INNER JOIN zone_levels zl
						ON zl.name=%(gis_type)s AND gd.zone_level=zl.id
						INNER JOIN gis_types gt
						ON gd.gis_type=gt.id AND gt.name=%(gis_type)s
			;""",
            dict(
                gis_type=self.gis_type,
            ),
        )
        if self.db.cursor.fetchone() is not None and not self.force:
            self.done = True
        else:
            # GIS info
            if not os.path.exists(os.path.join(data_folder, self.gis_info_name)):
                if not os.path.exists(
                    os.path.join(data_folder, self.gis_info_name + ".zip")
                ):
                    self.download(
                        url=self.gis_info, destination=self.gis_info_name + ".zip"
                    )
                self.logger.info("Unzipping {}".format(self.gis_info_name + ".zip"))
                self.unzip(
                    orig_file=self.gis_info_name + ".zip",
                    destination=self.gis_info_name,
                )

            # Simplifying shapefile into geojson
            if self.simplified and not os.path.exists(
                os.path.join(self.data_folder, self.geojson_gis_info_name)
            ):
                self.logger.info("Converting Shapefile into GeoJSON with less edges")
                self.simplify_shapefile()

    def apply(self):
        # filling zones info at different levels
        self.fill_parishes()
        # self.fill_cantons()
        # self.fill_provinces()
        # self.fill_country()
        # filling parenthood between levels
        # self.fill_parents_pa_ca()
        # self.fill_parents_pa_pr()
        # self.fill_parents_pa_co()
        # self.fill_parents_ca_pr()
        # self.fill_parents_ca_co()
        # self.fill_parents_pr_co()
        # self.fill_parents_country()
        # filling gis data info
        self.fill_gis_pa()
        # self.fill_gis_g()
        # self.fill_gis_bz()
        # self.fill_gis_bl()
        # self.fill_gis_country()
        # filling population data
        # if self.include_population:
        #     self.fill_population()

    # @check_empty(table='zones')
    def fill_parishes(self, filename=None):
        self.logger.info("Filling parishes")
        if filename is None:
            filename = os.path.join(
                self.gis_info_name, "ecu_admbnda_adm3_inec_20190724.shp"
            )
        self.record_file(filename=filename, filecode="ecuador_parishes")
        self.db.cursor.execute(
            """INSERT INTO zone_levels(name,pretty_name) VALUES('ecuador_parishes','Ecuador parroquias') ON CONFLICT DO NOTHING;"""
        )
        self.db.connection.commit()

        with shapefile.Reader(os.path.join(self.data_folder, filename)) as sf:
            extras.execute_batch(
                self.db.cursor,
                """INSERT INTO zones(code,name,level) VALUES(%(code)s,%(name)s,(SELECT id FROM zone_levels WHERE name='ecuador_parishes')) ON CONFLICT DO NOTHING;""",
                (
                    dict(
                        code=r[1],
                        name=r[0],
                    )
                    for s, r in zip(sf.shapes(), sf.records())
                ),
            )
        self.db.connection.commit()

        # with open(os.path.join(self.data_folder, filename), "r") as f:
        #     reader = csv.reader(f)
        #     next(reader)  # remove header
        #     ans = [r for r in reader]
        #     try:
        #         int(ans[0][3])
        #     except ValueError:
        #         ans = ans[1:]
        #     ans = self.clean_reader(ans)  # two last lines are just empty/info
        # extras.execute_batch(
        #     self.db.cursor,
        #     """INSERT INTO zones(id,name,level) VALUES(%s,%s,(SELECT id FROM zone_levels WHERE name='ecuador_parishes')) ON CONFLICT DO NOTHING;""",
        #     ((int(r[3]), r[4]) for r in ans),
        # )
        # self.db.connection.commit()

    def fill_gis_pa(self, filename=None, gis_type=None):
        """
        distinguishing between raw shapefile (original highly detailed geoms), or processed geojsonfile (simplified via mapshaper)
        """
        if self.simplified:
            filetype = "geojson"
        else:
            filetype = "shapefile"

        if gis_type is None:
            gis_type = self.gis_type
        self.logger.info("Filling parishes GIS")
        self.db.cursor.execute(
            "INSERT INTO gis_types(name) VALUES(%s) ON CONFLICT DO NOTHING;",
            (gis_type,),
        )
        self.db.connection.commit()
        if filetype == "geojson":
            if filename is None:
                filename = filename = os.path.join(
                    self.gis_info_name, "ecu_admbnda_adm3_inec_20190724.geojson"
                )
            self.record_file(filename=filename, filecode="ecuador_parishes_geojson")
            with open(os.path.join(self.data_folder, filename), "r") as f:
                zs_geo = json.load(f)
            extras.execute_batch(
                self.db.cursor,
                """
				INSERT INTO gis_data(zone_id,zone_level,geom,center,gis_type) 
						VALUES (%s,
								(SELECT id FROM zone_levels WHERE name=%s),
								ST_SetSRID(ST_MakeValid(ST_GeomFromGeoJSON(%s)),4326),
								ST_SetSRID(ST_Centroid(ST_MakeValid(ST_GeomFromGeoJSON(%s))),4326),
								(SELECT id FROM gis_types WHERE name=%s))
								ON CONFLICT DO NOTHING
								;""",
                (
                    (
                        int(
                            gj["properties"][
                                ("id" if "id" in gj["properties"].keys() else "g_id")
                            ]
                        ),
                        "ecuador_parishes",
                        str(gj["geometry"]),
                        str(gj["geometry"]),
                        gis_type,
                    )
                    for gj in zs_geo["features"]
                ),
            )
        elif filetype == "shapefile":
            if filename is None:
                filename = filename = os.path.join(
                    self.gis_info_name, "ecu_admbnda_adm3_inec_20190724.shp"
                )
            self.record_file(filename=filename, filecode="ecuador_parishes_shapefile")
            with shapefile.Reader(os.path.join(self.data_folder, filename)) as sf:
                extras.execute_batch(
                    self.db.cursor,
                    """INSERT INTO gis_data(zone_id,zone_level,geom,center,gis_type)
                    	SELECT z.id,
                    			zl.id,
                    			ST_SetSRID(ST_GeomFromGeoJSON(%(geom)s),4326),
                    			ST_Centroid(ST_SetSRID(ST_GeomFromGeoJSON(%(geom)s),4326)),
                    			gt.id
                    		FROM zone_levels zl
                    		INNER JOIN zones z 
                    		ON zl.name=%(zone_level)s
                    		AND z.code=%(code)s
                    		AND z.level=zl.id
                    		INNER JOIN gis_types gt
                    		ON gt.name=%(gis_type)s
                    	ON CONFLICT DO NOTHING;""",
                    (
                        dict(
                            code=r[1],
                            zone_level="ecuador_parishes",
                            geom=json.dumps(s.__geo_interface__),
                            gis_type=gis_type,
                        )
                        for s, r in zip(sf.shapes(), sf.records())
                    ),
                )
        else:
            raise ValueError("ZS filetype unknown:", filetype)
        self.db.connection.commit()

    def simplify_shapefile(self, **kwargs):
        if self.simplify_engine == "mapshaper":
            self.simplify_shapefile_mapshaper(**kwargs)
        elif self.simplify_engine == "topojson":
            self.simplify_shapefile_topojson(**kwargs)
        else:
            raise NotImplementedError(
                f"Simplifying engine should be mapshaper or topojson. Not implemented: {self.simplify_engine}"
            )

    def simplify_shapefile_topojson(
        self, input_path=None, output_path=None, simplify_param=10
    ):
        if input_path is None:
            input_path = os.path.join(self.data_folder, self.gis_info_fullname)
        if output_path is None:
            output_path = os.path.join(self.data_folder, self.geojson_gis_info_name)
        gdf = gpd.read_file(input_path).to_crs(4326)
        topo = tp.Topology(gdf)
        del gdf
        topo.toposimplify(simplify_param).to_geojson(fp=output_path)
        del topo
        # with open(output_path,'w') as f:
        # 	f.write(output)
        # del output

    def simplify_shapefile_mapshaper(
        self,
        input_path=None,
        output_path=None,
        method="visvalingam",
        percentage=0.2,
        interval=None,
        weight=0.5,
    ):
        """
        Simplifying the shapefile
        Parameter choice is not really implemented in this class; in this case the name of the gis_type attribute used in the database should integrate them to avoid confusion
        """
        if input_path is None:
            input_path = os.path.join(self.data_folder, self.gis_info_fullname)
        if output_path is None:
            output_path = os.path.join(self.data_folder, self.geojson_gis_info_name)
        if method == "dp":
            options = " dp"
        elif method == "visvalingam":
            options = "visvalingam"
        else:
            raise NotImplementedError(
                "method should be dp or visvalingam, not " + str(method)
            )
        if weight is not None:
            options += " weighted weighting={}".format(weight)
        if percentage is not None:
            options += " percentage={}".format(percentage)
        if interval is not None:
            options += " interval={}".format(interval)
        cmd = "mapshaper {input_path} -simplify {options} keep-shapes -proj wgs84 -o {output_path}".format(
            input_path=input_path, output_path=output_path, options=options
        )
        self.logger.info("+ " + cmd)
        cmd_output = subprocess.check_output(cmd.split(" "))
        self.logger.info(cmd_output)

    def clean_reader(self, reader):
        ans = reader
        # while (
        #     ans[-1] == ""
        #     or ans[-1][0].startswith(
        #         "Q: STATISTIK AUSTRIA, Statistik des Bevölkerungsstandes."
        #     )
        #     or ans[-1][0].startswith(
        #         '"Q: STATISTIK AUSTRIA, Statistik des Bevölkerungsstandes.'
        #     )
        # ):
        #     ans.pop(-1)
        # # two last lines are just empty/info
        return ans
