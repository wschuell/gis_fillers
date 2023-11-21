from db_fillers import Getter
import string
import random
from psycopg2 import extras
import shapely
import geopandas as gpd
from geopy.geocoders import Nominatim
from shapely.geometry import Point
from shapely.affinity import translate


class GISGetter(Getter):
    columns = ("geometry",)

    def get(self, db, raw_data=False, **kwargs):
        db.cursor.execute(self.query(), self.query_attributes())
        query_result = list(db.cursor.fetchall())
        if raw_data:
            return query_result
        else:
            gdf = gpd.GeoDataFrame(
                self.parse_results(query_result=query_result),
                crs="epsg:4326",
                columns=self.columns,
            )
            return gdf


class LocationPointsGetter(GISGetter):
    """
    Mother class for specific location type queries
    """

    columns = ("location", "geometry", "lat", "long")

    def __init__(self, location_list, add_noise=False, noise_size=0.01, **kwargs):
        self.location_list = location_list
        self.add_noise = add_noise
        self.noise_size = noise_size
        Getter.__init__(self, **kwargs)
        self.rnd_str = "".join(
            random.choice(string.ascii_letters + string.digits) for _ in range(10)
        )

    def get(self, **kwargs):
        gdf = GISGetter.get(self, **kwargs)
        if self.add_noise:
            tmp = []
            for index, poi in gdf.iterrows():
                geom = gdf.loc[index, "geometry"]
                if geom is None:
                    new_point = None
                else:
                    new_point = translate(
                        geom,
                        xoff=(random.random() - 0.5) * 2 * self.noise_size,
                        yoff=(random.random() - 0.5) * 2 * self.noise_size,
                    )
                tmp.append(
                    {
                        "geometry": new_point,
                    }
                )
            new_gdf = gpd.GeoDataFrame(tmp, columns=["geometry"])
            gdf["geometry"] = new_gdf["geometry"]
            gdf["lat"] = gdf["geometry"].y
            gdf["long"] = gdf["geometry"].x

        return gdf

    def prepare(self):
        Getter.prepare(self)
        self.db.cursor.execute(
            f"""
            CREATE TEMP TABLE IF NOT EXISTS temp_locations_{self.rnd_str}(
                id BIGSERIAL PRIMARY KEY,
                location text NOT NULL
                );
            """
        )
        extras.execute_batch(
            self.db.cursor,
            f"""
            INSERT INTO temp_locations_{self.rnd_str}(
                location
                )
            VALUES(%(loc)s)
            """,
            (dict(loc=loc) for loc in self.location_list),
        )

    def query(self):
        raise NotImplementedError

    def query_attributes(self):
        return dict()

    def parse_results(self, query_result):
        """
        returns list of elements to be used for pandas or geopandas
        """
        raise NotImplementedError

    def cleanup(self):
        self.db.cursor.execute(
            f"""
            DROP TABLE IF EXISTS temp_locations_{self.rnd_str};
            """
        )


class AreaPointsGetter(LocationPointsGetter):
    """
    Returns coordinates of points from a list of areas by code and zone level
    """

    def __init__(self, zone_level="bezirk", location_ref_type="code", **kwargs):
        self.zone_level = zone_level
        if location_ref_type not in (
            "code",
            "id",
            "name",
        ):
            raise ValueError(f"Unrecognized ref_type for location:{location_ref_type}")
        self.location_ref_type = location_ref_type
        LocationPointsGetter.__init__(self, **kwargs)

    def query_attributes(self):
        return dict(zone_level=self.zone_level, gis_type="zaehlsprengel")

    def query(self):
        return f"""
        WITH main_query AS(SELECT
            tl.location,
            CASE ST_NumGeometries(gd.geom)
                WHEN 0 THEN NULL
                WHEN 1 THEN ST_AsText(ST_GeometryN(ST_GeneratePoints(gd.geom,1),1))
                ELSE ST_AsText(ST_GeometryN(ST_GeneratePoints(gd.geom,100),(random()*100)::int+1))
            END AS geom
        FROM temp_locations_{self.rnd_str} tl
        LEFT OUTER JOIN zone_levels zl
                ON zl.name=%(zone_level)s
        LEFT OUTER JOIN zones z
                ON zl.id=z.level
                AND COALESCE(z.{self.location_ref_type}::text,z.id::text)=tl.location
        LEFT OUTER JOIN gis_types gt
                ON gt.name=%(gis_type)s
        LEFT OUTER JOIN gis_data gd
                ON gd.gis_type=gt.id
                AND gd.zone_id=z.id
                AND gd.zone_level=zl.id
        ORDER BY tl.id)
        SELECT location,ST_Y(geom) AS geo_lat,ST_X(geom) AS geo_long,geom FROM main_query;
        """

    def parse_results(self, query_result):
        return [
            {
                "location": loc,
                "lat": geo_lat,
                "long": geo_long,
                "geometry": shapely.wkt.loads(geo),
            }
            for (
                loc,
                geo_lat,
                geo_long,
                geo,
            ) in query_result
        ]


class AddressPointsGetter(LocationPointsGetter):
    """
    Returns coordinates of points from a list of addresses
    Using a nominatim instance
    """

    columns = ("location", "lat", "long", "geometry")

    def __init__(
        self,
        nominatim_host="localhost",
        nominatim_port=8080,
        nominatim_user_agent="gis_fillers",
        **kwargs,
    ):
        self.nominatim_host = nominatim_host
        self.nominatim_port = nominatim_port
        self.nominatim_user_agent = nominatim_user_agent
        AreaPointsGetter.__init__(self, **kwargs)
        self.set_geolocator()

    def set_geolocator(self):
        if self.nominatim_host is None:
            self.geolocator = Nominatim(user_agent=self.nominatim_user_agent)
        else:
            self.geolocator = Nominatim(
                user_agent=self.nominatim_user_agent,
                domain=f"{self.nominatim_host}:{self.nominatim_port}",
                scheme="http",
            )

    def query(self):
        return f"""
        SELECT tl.location,
                ST_Y(ca.geom) AS geo_lat,
                ST_X(ca.geom) AS geo_long,
                ST_AsText(ca.geom) AS geometry
        FROM temp_locations_{self.rnd_str} tl
        LEFT OUTER JOIN cached_addresses ca
        ON ca.address=tl.location
        ORDER BY tl.id
        ;
        """

    def parse_results(self, query_result):
        ans = [
            {
                "location": loc,
                "lat": geo_lat,
                "long": geo_long,
                "geometry": shapely.wkt.loads(geo),
            }
            for (
                loc,
                geo_lat,
                geo_long,
                geo,
            ) in query_result
        ]
        to_resolve = [a for a in ans if a["geometry"] is None]
        if len(to_resolve):
            self.logger.info(
                f"Resolving {len(set([r['location'] for r in to_resolve]))} addresses out of {len(ans)}."
            )
        temp_resolved = dict()
        for a in to_resolve:
            loc = a["location"]
            if loc in temp_resolved.keys():
                geoloc = temp_resolved[loc]
            else:
                geoloc = self.geolocator.geocode(loc)
                temp_resolved[loc] = geoloc
            a["geometry"] = shapely.wkt.loads(
                f"POINT({geoloc.longitude} {geoloc.latitude})"
            )
            a["lat"], a["long"] = geoloc.latitude, geoloc.longitude
            self.fill_cached_address(address=loc, geo_lat=a["lat"], geo_long=a["long"])
        return ans

    def fill_cached_address(self, address, geo_lat, geo_long):
        self.db.cursor.execute(
            """
            INSERT INTO cached_addresses(address,geom)
                SELECT %(address)s,ST_SetSRID(ST_MakePoint(%(geo_long)s::double precision,%(geo_lat)s::double precision),4326)
                On CONFLICT DO NOTHING;
            """,
            dict(address=address, geo_lat=geo_lat, geo_long=geo_long),
        )
        self.db.connection.commit()


class ZipPointsGetter(LocationPointsGetter):
    """
    Returns coordinates of points using zip codes and data from geonames
    """

    columns = ("location", "country_code", "lat", "long", "geometry")

    def prepare(self):
        Getter.prepare(self)
        self.rnd_str = "".join(
            random.choice(string.ascii_letters + string.digits) for _ in range(10)
        )
        self.db.cursor.execute(
            f"""
            CREATE TEMP TABLE IF NOT EXISTS temp_locations_{self.rnd_str}(
                id BIGSERIAL PRIMARY KEY,
                location text,
                country_code text
                );
            """
        )
        extras.execute_batch(
            self.db.cursor,
            f"""
            INSERT INTO temp_locations_{self.rnd_str}(
                location,country_code
                )
            VALUES(%(loc)s,%(country)s)
            """,
            (dict(loc=loc, country=country) for country, loc in self.location_list),
        )

    def query(self):
        return f"""
        SELECT tl.location,
                tl.country_code,
                ST_Y(gz.geom) AS geo_lat,
                ST_X(gz.geom) AS geo_long,
                ST_AsText(gz.geom) AS geometry
        FROM temp_locations_{self.rnd_str} tl
        LEFT OUTER JOIN geonames_zipcodes gz
        ON gz.country_code=tl.country_code
        AND gz.zip_code=tl.location
        ORDER BY tl.id
        ;
        """

    def parse_results(self, query_result):
        return [
            {
                "location": loc,
                "country_code": c_code,
                "lat": geo_lat,
                "long": geo_long,
                "geometry": shapely.wkt.loads(geo),
            }
            for (
                loc,
                c_code,
                geo_lat,
                geo_long,
                geo,
            ) in query_result
        ]
