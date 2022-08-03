import gis_fillers as gf
from gis_fillers.getters import zone_getters

import os

from gis_fillers import Database
from gis_fillers.fillers import zones

import geopandas as gpd
from matplotlib import pyplot as plt

conninfo = {
	'host':'195.154.70.113',
	'port':64741,
	'database':'example_db', # CHANGE TO 'playground' to have write rights
	'user':'wschuell', # CHANGE TO YOUR USERNAME
	'data_folder': os.path.join(os.path.dirname(__file__),'data_folder')
}

# conninfo = {
# 	'host':'localhost',
# 	'port':5432,
# 	'database':'test_gis_fillers',
# 	'user':'postgres',
# 	'data_folder': os.path.join(os.path.dirname(__file__),'data_folder')
# }


db = Database(**conninfo)

######## Filling in the DB; all files automatically downloaded (not if data already present in the DB)
###### !!! You need write access to the DB to be able to execute that !!!


# db.clean_db() # Erases all data
db.init_db() # Creates the structure

db.add_filler(zones.zaehlsprengel.ZaehlsprengelFiller()) # Fills in data for AT
db.add_filler(zones.zaehlsprengel.SimplifiedZSFiller()) # Fills in data for AT but with lower precision for geometries (uses mapshaper - to be installed separately)
db.add_filler(zones.zaehlsprengel.PLZFiller()) # Fills in zip code data for AT
db.add_filler(zones.countries.CountriesFiller())

db.add_filler(zones.hexagons.HexagonsFiller(res=8,target_zone=922,target_zone_level='bezirk'))
# db.add_filler(zones.hexagons.HexagonsFiller(res=9,target_zone=922,target_zone_level='bezirk'))
db.add_filler(zones.hexagons.HexagonsFiller(res=4,target_zone='AT',target_zone_level='country'))
# db.add_filler(zones.hexagons.HexagonsFiller(res=5,target_zone='AT',target_zone_level='country'))

db.fill_db()

