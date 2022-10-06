import os
import requests
import zipfile
import logging
import csv
import copy
from psycopg2 import extras
import shapefile
import json
import subprocess
from .. import fillers



class ZonesFiller(fillers.Filler):
	"""
	Fills in zones GIS shapes in a specified gis_type and with a specified zone_level
	expected file: csv with geometries as explicit string, in SRID 4326
	"""
	def __init__(self,
					filepath,
					zone_level,
					zone_level_pretty=None,
					columns={'name':1,'code':2,'geom':3},
					force=False,
					header = False,
					gis_type='zaehlsprengel',
					**kwargs):
		self.force = force
		self.columns = copy.deepcopy(columns)
		self.filepath = filepath
		self.header = header
		self.zone_level = zone_level
		if zone_level_pretty is None:
			self.zone_level_pretty = self.zone_level
		else:
			self.zone_level_pretty = zone_level_pretty
		self.gis_type = gis_type
		fillers.Filler.__init__(self,name=f'generic_{zone_level}',**kwargs)

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
						ON zl.name=%(zone_level)s AND gd.zone_level=zl.id
						INNER JOIN gis_types gt
						ON gd.gis_type=gt.id AND gt.name=%(gis_type)s
			;''',{'zone_level':self.zone_level,'gis_type':self.gis_type,})
		query_ans = self.db.cursor.fetchone()
		if query_ans is not None and query_ans[0] >= 2:
			self.done = True

	def apply(self):
		#filling zones info at different levels
		self.fill_zones()
		#filling gis data info
		self.fill_gis()


	def fill_zones(self,filename=None):

		self.logger.info(f'Filling {self.zone_level}')
		if filename is None:
			filename = self.filepath
		self.record_file(filename=filename,filecode=f'zones_{self.zone_level}')
		self.db.cursor.execute('''INSERT INTO zone_levels(name,pretty_name) VALUES(%(zone_level)s,%(zl_pretty)s) ON CONFLICT DO NOTHING;''',{'zone_level':self.zone_level,'zl_pretty':self.zone_level_pretty})
		self.db.connection.commit()
		with open(os.path.join(self.data_folder,filename),'r') as f:
			reader = csv.reader(f)
			if self.header:
				next(reader)
			extras.execute_batch(self.db.cursor,'''INSERT INTO zones(code,name,level)
				VALUES(%(code)s,
						%(name)s,
						(SELECT id FROM zone_levels WHERE name=%(zone_level)s))
					 ON CONFLICT DO NOTHING;''',({'code':r[self.columns['code']],'name':r[self.columns['name']],'zone_level':self.zone_level} for r in reader))
		self.db.connection.commit()


	def fill_gis(self,filename=None,gis_type=None):
		'''
		'''
		if gis_type is None:
			gis_type = self.gis_type
		self.logger.info(f'Filling {self.zone_level} GIS')
		self.db.cursor.execute('INSERT INTO gis_types(name) VALUES(%s) ON CONFLICT DO NOTHING;',(gis_type,))
		self.db.connection.commit()
		if filename is None:
			filename = self.filepath # for children classes
		self.record_file(filename=filename,filecode=self.zone_level)
		with open(os.path.join(self.data_folder,filename),'r') as f:
			reader = csv.reader(f)
			if self.header:
				next(reader)
			extras.execute_batch(self.db.cursor,'''INSERT INTO gis_data(
						zone_id,
						zone_level,
						geom,
						center,
						gis_type)
				VALUES ((SELECT id FROM zones WHERE code=%(zone_code)s),
					(SELECT id FROM zone_levels WHERE name=%(level)s),
					ST_SetSRID(ST_GeomFromText(%(geom)s),4326),
					ST_Centroid(ST_SetSRID(ST_GeomFromText(%(geom)s),4326)),
					(SELECT id FROM gis_types WHERE name=%(gis_type)s))
					ON CONFLICT DO NOTHING
					;''',({'zone_code':r[self.columns['code']],'level':self.zone_level,'geom':r[self.columns['geom']],'gis_type':gis_type} for r in reader))
		self.db.connection.commit()
