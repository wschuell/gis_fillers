import pytest
import os
import glob

import gis_fillers as gf
from gis_fillers import Database
from gis_fillers.fillers import zones
from gis_fillers.getters import zone_getters

conninfo = {
	'host':'localhost',
	'port':5432,
	'database':'test_gis_fillers',
	'user':'postgres',
	'data_folder': os.path.join(os.path.dirname(os.path.dirname(__file__)),'data_folder')
}


def test_connect():
	db = Database(**conninfo)

def test_init():
	db = Database(**conninfo)
	db.init_db()

def test_clean():
	db = Database(**conninfo)
	db.clean_db()
	db.init_db()

@pytest.fixture
def maindb():
	db = Database(**conninfo)
	db.init_db()
	yield db
	db.connection.close()


def test_countries(maindb):
	maindb.add_filler(zones.countries.CountriesFiller())
	maindb.fill_db()


def test_zs(maindb):
	maindb.add_filler(zones.zaehlsprengel.ZaehlsprengelFiller())
	maindb.fill_db()


def test_plz(maindb):
	maindb.add_filler(zones.zaehlsprengel.PLZFiller())
	maindb.fill_db()


def test_getters(maindb):
	zone_getters.PopulationGetter(db=maindb,zone_level='bezirk',simplified=False).get_result()
	zone_getters.PopulationDensityGetter(db=maindb,zone_level='bezirk',simplified=False).get_result()
