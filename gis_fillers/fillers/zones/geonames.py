from psycopg2 import extras
import zipfile
import os
from .. import fillers


class GeonamesFiller(fillers.Filler):
    def __init__(
        self,
        force=False,
        url_geonames="https://download.geonames.org/export/zip/allCountries.zip",
        zipname="geonames_allCountries.zip",
        **kwargs
    ):
        self.force = force
        self.url_geonames = url_geonames
        self.zipname = zipname
        fillers.Filler.__init__(self, **kwargs)

    def prepare(self):
        fillers.Filler.prepare(self)
        if not self.force and self.check_done():
            self.done = True
        elif not os.path.exists(os.path.join(self.data_folder, self.zipname)):
            self.download(url=self.url_geonames, destination=self.zipname)

    def gen_extract(self):
        with zipfile.ZipFile(os.path.join(self.data_folder, self.zipname), "r") as zf:
            with zf.open("allCountries.txt", "r") as f:
                for line in f:
                    elt = line.decode("utf8").split("\t")
                    yield dict(
                        country_code=elt[0],
                        zip_code=elt[1],
                        geo_lat=elt[-3],
                        geo_long=elt[-2],
                    )

    def check_done(self):
        self.db.cursor.execute("SELECT 1 FROM geonames_zipcodes LIMIT 1;")
        return self.db.cursor.fetchone() == (1,)

    def apply(self):
        extras.execute_batch(
            self.db.cursor,
            """
        	INSERT INTO geonames_zipcodes(country_code,zip_code,geom)
        	VALUES(
        			%(country_code)s,
        			%(zip_code)s,
        			ST_SetSRID(ST_MakePoint(%(geo_long)s::double precision,%(geo_lat)s::double precision),4326)
        			)
        	ON CONFLICT DO NOTHING;
        	""",
            self.gen_extract(),
            page_size=10**4,
        )
        self.db.connection.commit()
