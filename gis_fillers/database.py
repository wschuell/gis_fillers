import psycopg2
from psycopg2 import extras, sql
import os
import copy
import logging
import csv
import hashlib
import numpy as np
from . import MetaFiller

from db_fillers import Database as TemplateDatabase

logger = logging.getLogger(__name__)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
ch.setFormatter(formatter)
logger.addHandler(ch)
logger.setLevel(logging.INFO)

try:
    import psycopg2
    from psycopg2 import extras
    from psycopg2.extensions import register_adapter, AsIs

    register_adapter(np.float64, AsIs)
    register_adapter(np.int64, AsIs)
except ImportError:
    logger.info(
        "Psycopg2 not installed, pip install psycopg2 (or binary-psycopg2) if you want to use a PostgreSQL DB"
    )


def split_sql_init(script):
    lines = script.split("\n")
    formatted = "\n".join([l for l in lines if l[:2] != "--"])
    return formatted.split(";")[:-1]


class Database(TemplateDatabase):
    """
    This class creates a database object with the main structure, with a few methods  to manipulate it.
    To fill it, fillers are used (see Filler class).
    The object uses a specific data folder and a list of files used for the fillers, with name, keyword, and potential download link. (move to filler class?)
    """

    def clean_db(self, gis_data_stay=False, commit=True, extra_whitelist=[], **kwargs):
        if gis_data_stay:
            extra_whitelist += [
                "zones",
                "zone_parents",
                "zone_levels",
                "gis_data",
                "gis_types",
            ]
        TemplateDatabase.clean_db(
            self, commit=commit, extra_whitelist=extra_whitelist, **kwargs
        )

    def get_gis_db(self, schema="postgis", replace=False, fill_db=True):
        if not hasattr(self, "gis_db") or replace:
            conninfo = copy.deepcopy(self.db_conninfo)
            conninfo["db_schema"] = schema
            self.gis_db = Database(**conninfo)
            self.gis_db.init_db()
            if fill_db:
                self.gis_db.add_filler(MetaFiller())
                self.gis_db.fill_db()
                self.gis_db.connection.commit()
        return self.gis_db
