from . import Getter, GISGetter

import numpy as np
import psycopg2
from matplotlib import pyplot as plt
import shapely
import time
import geopandas as gpd
import pandas as pd
import string


class PopulationGetter(GISGetter):
    """
    Returns a geopandas dataframe with population per defined area level
    """

    columns = ("Zone", "ZoneID", "population", "geometry", "area")

    def __init__(
        self,
        zone_level="bezirk",
        zone_attribute="population",
        simplified=True,
        **kwargs,
    ):
        GISGetter.__init__(self, **kwargs)
        self.zone_level = zone_level
        self.zone_attribute = zone_attribute
        if simplified:
            self.target_gt = "zaehlsprengel_simplified"
        else:
            self.target_gt = "zaehlsprengel"

    def query(self):
        return """
            SELECT q1.id,q1.level,q2.population,q1.name,q1.geometry, q1.area FROM
                (SELECT z.id,z.level,z.name, ST_AsText(gd.geom) AS geometry, ST_Area(gd.geom,false)/10^6 AS area
                    FROM zones z
                    INNER JOIN zone_levels zl
                    ON zl.name=%(zone_level)s AND zl.id=z."level"
                    INNER JOIN gis_data gd
                    ON gd.zone_id =z.id AND gd.zone_level =z."level"
                    INNER JOIN gis_types gt
                    ON gd.gis_type =gt.id AND gt."name" =%(target_gt)s) AS q1
            INNER JOIN
                (SELECT z.id,z.level,z.name,SUM(za.int_value::double precision*(COALESCE(zp.share,1.)::double precision)) AS population
                FROM zones z
                INNER JOIN zone_levels zl
                ON zl.name=%(zone_level)s AND zl.id=z."level"
                INNER JOIN zone_parents zp
                ON zp.parent=z.id AND zp.parent_level=z.level
                INNER JOIN zone_levels zl2
                ON zl2.id = zp.child_level AND zl2.name='zaehlsprengel'
                INNER JOIN zone_attributes za
                ON za.zone=zp.child AND za.zone_level=zp.child_level
                INNER JOIN zone_attribute_types zat
                ON zat.id=za.attribute AND zat.name='zs_population'
                GROUP BY z.id,z.level,z.name
                    UNION
                SELECT z.id,z.level,z.name,SUM(za.int_value::real) AS population
                FROM zones z
                INNER JOIN zone_levels zl
                ON zl.name=%(zone_level)s AND zl.id=z."level" AND 'zaehlsprengel'=%(zone_level)s
                INNER JOIN zone_attributes za
                ON za.zone=z.id AND za.zone_level=zl.id
                INNER JOIN zone_attribute_types zat
                ON zat.id=za.attribute AND zat.name='zs_population'
                GROUP BY z.id,z.level,z.name
                ) AS q2
            ON q1.id=q2.id AND q1.level=q2.level
        ;"""

    def query_as_table(self, tablename):
        for c in tablename:
            if c not in string.ascii_letters + string.digits + "_":
                raise ValueError(f"Unsafe table name: {tablename}")
        return (
            f"""CREATE TEMPORARY TABLE IF NOT EXISTS {tablename}
            (
            id BIGINT,
            level BIGINT,
            population REAL,
            name TEXT,
            geometry TEXT,
            area REAL,
            PRIMARY KEY (id,level)
            );
            INSERT INTO {tablename}
            (
            id ,
            level ,
            population,
            name,
            geometry,
            area
            ) 
            """
            + self.query()
            + f"""
            --CREATE INDEX IF NOT EXISTS {tablename}_idx ON {tablename}();
            """
        )

    def query_attributes(self):
        return {
            "zone_level": self.zone_level,
            "zone_attribute": self.zone_attribute,
            "target_gt": self.target_gt,
        }

    def parse_results(self, query_result):
        return [
            {
                "Zone": bez,
                "ZoneID": zid,
                "population": 0 if np.isnan(pop) else int(pop),
                "geometry": shapely.wkt.loads(geo),
                "area": area,
            }
            for (zid, zlvl, pop, bez, geo, area) in query_result
        ]


class PopulationDensityGetter(PopulationGetter):
    """
    Returns a geopandas dataframe with population density in people/km2 per defined area level
    """

    columns = ("Zone", "ZoneID", "population_density", "geometry", "area")

    def parse_results(self, query_result):
        return [
            {
                "Zone": bez,
                "ZoneID": zid,
                "population_density": 0 if np.isnan(pop) else int(pop) / area,
                "geometry": shapely.wkt.loads(geo),
                "area": area,
            }
            for (zid, zlvlv, pop, bez, geo, area) in query_result
        ]
