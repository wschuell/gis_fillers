import os
import requests
import zipfile
import logging
import csv
from psycopg2 import extras
import shapefile
import json
import subprocess
from .. import fillers



class CountriesFiller(fillers.Filler):
	"""
	Fills in countries GIS shapes in zaehlsprengel gis_type
	"""
	year_list = (2001,2006,2010,2013,2016,2020)
	def __init__(self,
					gis_info="https://gisco-services.ec.europa.eu/distribution/v2/countries/download/ref-countries-{YEAR}-01m.geojson.zip",
					gis_info_name="ref-countries-{YEAR}-01m.geojson",
					geojson_gis_info_name="ref-countries-{YEAR}-01m.geojson",
					fullgeojson_gis_info_name="ref-countries-{YEAR}-01m.geojson/CNTR_RG_01M_{YEAR}_4326.geojson",
					LBgeojson_gis_info_name="ref-countries-{YEAR}-01m.geojson/CNTR_LB_{YEAR}_4326.geojson",
					include_austria=False,
					force=False,
					year=None,
					**kwargs):
		self.force = force
		if year is None:
			self.year = self.year_list[-1]
		else:
			self.year = year
		self.gis_info = gis_info.format(YEAR=self.year)
		self.gis_info_name = gis_info_name.format(YEAR=self.year)
		self.geojson_gis_info_name = geojson_gis_info_name.format(YEAR=self.year)
		self.fullgeojson_gis_info_name = fullgeojson_gis_info_name.format(YEAR=self.year)
		self.LBgeojson_gis_info_name = LBgeojson_gis_info_name.format(YEAR=self.year)
		self.gis_type = 'zaehlsprengel'
		if self.year not in self.year_list:
			self.logger.warning('Year {} may not be available for countries GIS data; available years should be:{}'.format(self.year,self.year_list))
		fillers.Filler.__init__(self,name='countries',**kwargs)

	def prepare(self):
		if self.data_folder is None:
			self.data_folder = self.db.data_folder
		data_folder = self.data_folder

		#create folder if needed
		if not os.path.exists(data_folder):
			os.makedirs(data_folder)

		self.db.cursor.execute('''
			SELECT COUNT(*)
					FROM gis_data gd
						INNER JOIN zone_levels zl
						ON zl.name='country' AND gd.zone_level=zl.id
						INNER JOIN gis_types gt
						ON gd.gis_type=gt.id AND gt.name=%s
			;''',(self.gis_type,))
		query_ans = self.db.cursor.fetchone()
		if query_ans is not None and query_ans[0] >= 2:
			self.done = True
		else:
			#GIS info
			if not os.path.exists(os.path.join(data_folder,self.gis_info_name)):
				if not os.path.exists(os.path.join(data_folder,self.gis_info_name+'.zip')):
					self.download(url=self.gis_info,destination=os.path.join(data_folder,self.gis_info_name+'.zip'))
				self.logger.info('Unzipping {}'.format(self.gis_info_name+'.zip'))
				self.unzip(orig_file=os.path.join(data_folder,self.gis_info_name+'.zip'),destination=os.path.join(data_folder,self.gis_info_name))

	def apply(self):
		#filling zones info at different levels
		self.fill_countries()
		#filling gis data info
		self.fill_gis_countries_LB()
		self.fill_gis_countries()


	def fill_countries(self,filename=None):

		self.logger.info('Filling countries')
		if filename is None:
			filename = self.LBgeojson_gis_info_name # for children classes
		self.record_file(filename=filename,filecode='countries_geojsonLB')
		self.db.cursor.execute('''INSERT INTO zone_levels(name,pretty_name) VALUES('country','Country') ON CONFLICT DO NOTHING;''')
		self.db.connection.commit()
		with open(os.path.join(self.data_folder,filename),'r') as f:
			zs_geo = json.load(f)
		extras.execute_batch(self.db.cursor,'''INSERT INTO zones(code,name,level)
			VALUES(%(code)s,
					%(name)s,
					(SELECT id FROM zone_levels WHERE name='country'))
				 ON CONFLICT DO NOTHING;''',({'code':gj['id'],'name':gj['properties']['NAME_ENGL']} for gj in zs_geo['features']))
		self.db.connection.commit()


	def fill_gis_countries_LB(self,filename=None,gis_type=None):
		'''
		'''
		if gis_type is None:
			gis_type = self.gis_type
		self.logger.info('Filling countries GIS centers')
		self.db.cursor.execute('INSERT INTO gis_types(name) VALUES(%s) ON CONFLICT DO NOTHING;',(gis_type,))
		self.db.connection.commit()
		if filename is None:
			filename = self.LBgeojson_gis_info_name # for children classes
		self.record_file(filename=filename,filecode='countries_geojsonLB')
		with open(os.path.join(self.data_folder,filename),'r') as f:
			zs_geo = json.load(f)

		self.db.connection.commit()
		extras.execute_batch(self.db.cursor,'''INSERT INTO gis_data(
						zone_id,
						zone_level,
						-- geom,
						center,
						gis_type)
				VALUES ((SELECT id FROM zones WHERE code=%(zone_code)s),
					(SELECT id FROM zone_levels WHERE name=%(level)s),
					ST_SetSRID(ST_GeomFromGeoJSON(%(geom)s),4326),
					(SELECT id FROM gis_types WHERE name=%(gis_type)s))
					ON CONFLICT DO NOTHING
					;''',({'zone_code':gj['id'],'level':'country','geom':str(gj['geometry']),'gis_type':gis_type} for gj in zs_geo['features']))
		self.db.connection.commit()

	def fill_gis_countries(self,filename=None,gis_type=None):
		'''
		'''
		if gis_type is None:
			gis_type = self.gis_type
		self.logger.info('Filling countries GIS')
		self.db.cursor.execute('INSERT INTO gis_types(name) VALUES(%s) ON CONFLICT DO NOTHING;',(gis_type,))
		self.db.connection.commit()
		if filename is None:
			filename = self.fullgeojson_gis_info_name # for children classes
		self.record_file(filename=filename,filecode='countries_geojson')
		with open(os.path.join(self.data_folder,filename),'r') as f:
			zs_geo = json.load(f)

		extras.execute_batch(self.db.cursor,'''UPDATE gis_data
				SET geom=ST_SetSRID(ST_GeomFromGeoJSON(%(geom)s),4326)
				WHERE zone_id=(SELECT id FROM zones WHERE code=%(code)s)
				AND zone_level=(SELECT id FROM zone_levels WHERE name=%(level)s)
				AND gis_type=(SELECT id FROM gis_types WHERE name=%(gis_type)s)
					;''',({'geom':str(gj['geometry']),'code':gj['id'],'gis_type':gis_type,'level':'country'} for gj in zs_geo['features']))
		self.db.connection.commit()
