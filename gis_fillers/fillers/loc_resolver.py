from db_fillers import Filler
import copy
from psycopg2 import extras

from ..getters import generic_getters


class LocationResolver(Filler):
    def __init__(
        self,
        source_db,
        id_col,
        loc_col,
        query_table,
        geom_col="geom",
        resolver_class="address",
        resolver_args=dict(),
        **kwargs,
    ):
        self.source_db = source_db
        if isinstance(id_col, str):
            self.id_columns = (id_col,)
        else:
            self.id_columns = id_col
        if isinstance(loc_col, str):
            self.loc_columns = (loc_col,)
        else:
            self.loc_columns = loc_col
        self.geom_col = geom_col
        self.query_table = query_table
        Filler.__init__(self, **kwargs)
        if resolver_class == "address":
            self.resolver_class = generic_getters.AddressPointsGetter
        elif resolver_class == "area":
            self.resolver_class = generic_getters.AreaPointsGetter
        elif resolver_class == "zipcode":
            self.resolver_class = generic_getters.ZipPointsGetter
        elif isinstance(resolver_class, str):
            raise ValueError(
                "resolver_class should be 'address', 'zipcode', 'area' or a class"
            )
        else:
            self.resolver_class = resolver_class
        self.resolver_args = copy.deepcopy(resolver_args)

    def check_sql(self):
        for var in (self.query_table, self.geom_col, *self.loc_columns):
            self.check_sql_safe(var)

    def prepare(self):
        self.check_sql()
        self.get_locations()
        resolver = self.resolver_class(
            db=self.source_db,
            location_list=self.location_list,
            **self.resolver_args,
        )
        results = resolver.get_result(raw_data=True)
        for i, col in enumerate(resolver.columns):
            if col == "lat":
                lat_idx = i
            elif col == "long":
                long_idx = i
        assert len(results) == len(
            self.location_list
        ), f"Mismatch between results length{len(results)} and expected length {len(self.location_list)}"
        for res, loc in zip(results, self.location_info):
            if isinstance(res, tuple):
                loc["geo_lat"] = res[lat_idx]
                loc["geo_long"] = res[long_idx]
            else:
                loc["geo_lat"] = res["lat"]
                loc["geo_long"] = res["long"]
            for idval, idc in zip(loc["id_list"], self.id_columns):
                loc[idc] = idval

    def get_locations(self):
        self.db.cursor.execute(
            f"""
    		SELECT {','.join(self.id_columns)},{','.join(self.loc_columns)} FROM {self.query_table}
    		WHERE {self.geom_col} IS NULL
    		"""
        )

        self.location_info = [
            dict(
                id_list=r[: len(self.id_columns)],
                loc_list=r[len(self.id_columns) :],
            )
            for r in self.db.cursor.fetchall()
        ]
        self.location_list = [l["loc_list"] for l in self.location_info]

    def apply(self):
        extras.execute_batch(
            self.db.cursor,
            f"""
    		UPDATE {self.query_table}
    		SET {self.geom_col}=ST_SetSRID(ST_MakePoint(%(geo_long)s,%(geo_lat)s),4326)
    		WHERE {' AND '.join([f'{idc}=%({idc})s' for idc in self.id_columns])}
    		AND {self.geom_col} IS NULL
    		""",
            self.location_info,
        )
        self.db.connection.commit()
