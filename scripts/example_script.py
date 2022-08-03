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


######## Getter wrappers around complex queries (see the corresponding imported files)

zone_level = 'bezirk'
gdf = zone_getters.PopulationGetter(db=db,zone_level=zone_level).get_result() # gets a geopandas dataframe with various info -- super fast because SQL query behind

print(gdf)
gdf.plot(column='population', legend=True)
plt.title('Population at {} level'.format(zone_level))
plt.show()




# zone_level = 'zaehlsprengel'
# gdf = zone_getters.PopulationDensityGetter(db=db,zone_level=zone_level).get_result()
# # area for density is from the exact definition of the zone geometries, but display uses the simplified ones. Can especially impact the values for zaehlsprengel level in Vienna
# print(gdf)
# gdf.plot(column='population_density',legend=True)
# plt.title('Population density at {} level'.format(zone_level))
# plt.show()

# available zone levels: country, bundesland, bezirk, gemeinde, zaehlsprengel

## Extra: using hexagons H3 <ref_zone_level>_<ref_zone_code_or_id>_hexagons_<resolution(higher=more precise,0 to 15)>
# needs to be filled beforehand, see filldb script

zone_level = 'bezirk_922_hexagons_8'
# zone_level = 'bezirk_922_hexagons_9'
gdf = zone_getters.PopulationGetter(db=db,zone_level=zone_level,simplified=False).get_result() # gets a geopandas dataframe with various info -- super fast because SQL query behind

print(gdf)
gdf.plot(column='population', legend=True)
plt.title('Population at {} level'.format(zone_level))
plt.show()


zone_level = 'country_AT_hexagons_4'
# zone_level = 'country_AT_hexagons_5'
gdf = zone_getters.PopulationDensityGetter(db=db,zone_level=zone_level,simplified=False).get_result()
# area for density is from the exact definition of the zone geometries, but display uses the simplified ones. Can especially impact the values for zaehlsprengel level in Vienna
print(gdf)
gdf.plot(column='population_density',legend=True)
plt.title('Population density at {} level'.format(zone_level))
plt.show()


######### Raw queries example: with the countries queries can still take a long time given the detail level (original high precision)

for title,query in [
		('100 first zones in the DB (random)','''SELECT geom FROM gis_data LIMIT 100;'''),

		('20 first countries','''SELECT gd.geom FROM zone_levels zl
						INNER JOIN gis_types gt
						ON gt.name='zaehlsprengel'
						AND zl.name='country'
						INNER JOIN gis_data gd
						ON gd.gis_type=gt.id  AND gd.zone_level=zl.id
						LIMIT 20
							;'''),

		('all countries','''SELECT gd.geom FROM zone_levels zl
						INNER JOIN gis_types gt
						ON gt.name='zaehlsprengel'
						AND zl.name='country'
						INNER JOIN gis_data gd
						ON gd.gis_type=gt.id  AND gd.zone_level=zl.id
							;'''),

		('40 countries closest to Austria by center','''
			WITH at_gd AS (SELECT gd.center FROM zones z
							INNER JOIN zone_levels zl
							ON z.code ='AT'
							AND z.level=zl.id
							AND zl.name='country'
							INNER JOIN gis_types gt
							ON gt.name='zaehlsprengel'
							INNER JOIN gis_data gd
							ON gd.gis_type=gt.id  AND gd.zone_level=zl.id and gd.zone_id =z.id)
				SELECT gd.geom FROM zone_levels zl
						INNER JOIN gis_types gt
						ON gt.name='zaehlsprengel'
						AND zl.name='country'
						INNER JOIN gis_data gd
						ON gd.gis_type=gt.id  AND gd.zone_level=zl.id
						ORDER BY ST_Distance((SELECT center FROM at_gd),gd.center) ASC
						LIMIT 40
							;'''),

		('40 countries closest to Austria by geometry','''
			WITH at_gd AS (SELECT gd.geom FROM zones z
							INNER JOIN zone_levels zl
							ON z.code ='AT'
							AND z.level=zl.id
							AND zl.name='country'
							INNER JOIN gis_types gt
							ON gt.name='zaehlsprengel'
							INNER JOIN gis_data gd
							ON gd.gis_type=gt.id  AND gd.zone_level=zl.id and gd.zone_id =z.id)
				SELECT gd.geom FROM zone_levels zl
						INNER JOIN gis_types gt
						ON gt.name='zaehlsprengel'
						AND zl.name='country'
						INNER JOIN gis_data gd
						ON gd.gis_type=gt.id  AND gd.zone_level=zl.id
						ORDER BY ST_Distance((SELECT geom FROM at_gd),gd.geom) ASC
						LIMIT 40
							;'''),
		]:

	gdf = gpd.GeoDataFrame.from_postgis(sql=query,con=db.connection)

	gdf.plot()
	plt.title(title)
	plt.show()
