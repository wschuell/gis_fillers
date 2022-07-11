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



class ZaehlsprengelFiller(fillers.Filler):
	"""
	This class fills in geographical data for Austria, with structure:
	zaehlsprengel < gemeinde < bezirk < bundesland
	Source is from STAT austria, in three files:
	GIS info: http://data.statistik.gv.at/data/OGDEXT_ZSP_1_STATISTIK_AUSTRIA_20190101.zip
	population info: https://www.statistik.at/wcm/idc/idcplg?IdcService=GET_NATIVE_FILE&RevisionSelectionMethod=LatestReleased&dDocName=103418
	bezirk names: http://www.statistik.at/verzeichnis/reglisten/polbezirke.csv

	Be attentive to the issue year of the different sources, they need to match (typically GIS info is ahead one year if you take the latest).
	For the population info, direct download is not implemented, as some complex JS code is involved on the server side, and on top of that, it is an xlsx file.
	Download it, and save it as csv.

	The simplified attribute is used to tell the filler to preprocess the shapefile and simplify the edges with mapshaper
	"""

	def __init__(self,
					gis_info="https://data.statistik.gv.at/data/OGDEXT_ZSP_1_STATISTIK_AUSTRIA_{YEAR}0101.zip",
					gis_info_name="OGDEXT_ZSP_1_STATISTIK_AUSTRIA_{YEAR}0101",
					gis_info_fullname="OGDEXT_ZSP_1_STATISTIK_AUSTRIA_{YEAR}0101/STATISTIK_AUSTRIA_ZSP_{YEAR}0101.shp",
					geojson_gis_info_name="STATISTIK_AUSTRIA_ZSP_{YEAR}0101.geojson",
					pop_info="https://statistik.at/fileadmin/pages/405/Bevoelkerung_am_1.1.{YEAR}_nach_Zaehlsprengel__Gebietsstand_1.1.{YEAR}_.ods",
					### CAUTION!: the file available on the above link is the last year available. If not matching the other files years,
					### leads to mismatches as Zaehlsprengel are redefined each year with minor modification
					# pop_info_name='einwohnerzahl_nach_zaehlsprengel_1.1.2020_gebietsstand_1.1.2020',
					bezirk_info='https://www.statistik.at/verzeichnis/reglisten/polbezirke.csv',
					bezirk_info_name='polbezirke.csv',
					simplified=False,
					include_population=True,
					force=False,
					remove_bz_900=True,
					year=2022,
					**kwargs):
		self.force = force
		self.year = year
		self.gis_info = gis_info.format(YEAR=self.year)
		self.gis_info_name = gis_info_name.format(YEAR=self.year)
		self.gis_info_fullname = gis_info_fullname.format(YEAR=self.year)
		self.geojson_gis_info_name = geojson_gis_info_name.format(YEAR=self.year)
		self.pop_info = pop_info.format(YEAR=self.year)
		self.pop_info_name = '.'.join(self.pop_info.split('/')[-1].split('.')[:-1])
		self.bezirk_info = bezirk_info
		self.bezirk_info_name = bezirk_info_name
		self.simplified = simplified
		self.remove_bz_900 = remove_bz_900
		self.include_population = include_population
		if self.simplified:
			self.gis_type = 'zaehlsprengel_simplified'
			try:
				subprocess.check_output('mapshaper --version'.split(' '))
			except FileNotFoundError:
				raise FileNotFoundError('Mapshaper is not installed, please install for node.js with: npm install -g mapshaper')
		else:
			self.gis_type = 'zaehlsprengel'
		fillers.Filler.__init__(self,name=self.gis_type,**kwargs)

	def prepare(self):
		if self.data_folder is None:
			self.data_folder = self.db.data_folder
		data_folder = self.data_folder

		#create folder if needed
		if not os.path.exists(data_folder):
			os.makedirs(data_folder)

		self.db.cursor.execute('''
			SELECT 1
					FROM gis_data gd
						INNER JOIN zone_levels zl
						ON zl.name='zaehlsprengel' AND gd.zone_level=zl.id
						INNER JOIN gis_types gt
						ON gd.gis_type=gt.id AND gt.name=%s
			;''',(self.gis_type,))
		if self.db.cursor.fetchone() is not None and not self.force:
			self.done = True
		else:
			#GIS info
			if not os.path.exists(os.path.join(data_folder,self.gis_info_name)):
				if not os.path.exists(os.path.join(data_folder,self.gis_info_name+'.zip')):
					self.download(url=self.gis_info,destination=os.path.join(data_folder,self.gis_info_name+'.zip'))
				self.logger.info('Unzipping {}'.format(self.gis_info_name+'.zip'))
				self.unzip(orig_file=os.path.join(data_folder,self.gis_info_name+'.zip'),destination=os.path.join(data_folder,self.gis_info_name))

			#Simplifying shapefile into geojson
			if self.simplified and not os.path.exists(os.path.join(self.data_folder,self.geojson_gis_info_name)):
				self.logger.info('Converting Shapefile into GeoJSON with less edges')
				self.simplify_shapefile()

			#bezirk info
			if not os.path.exists(os.path.join(data_folder,self.bezirk_info_name)):
				self.download(url=self.bezirk_info,destination=os.path.join(data_folder,self.bezirk_info_name))

			#pop_info
			file_ext = self.pop_info.split('.')[-1]
			if not os.path.exists(os.path.join(data_folder,self.pop_info_name+'.csv')):
				if not os.path.exists(os.path.join(data_folder,self.pop_info_name+'.'+file_ext)):
					self.logger.info('Downloading {}'.format(self.pop_info))
					self.download(url=self.pop_info,destination=os.path.join(data_folder,self.pop_info_name)+'.'+file_ext,wget=True)
					# raise NotImplementedError('Complex JS query pattern, please download manually')
				self.convert_spreadsheet(orig_file=os.path.join(data_folder,self.pop_info_name+'.'+file_ext),destination=os.path.join(data_folder,self.pop_info_name+'.csv'))

	def apply(self):
		#filling zones info at different levels
		self.fill_zs()
		self.fill_gemeinde()
		self.fill_bezirk()
		self.fill_bundesland()
		self.fill_country()
		#filling parenthood between levels
		self.fill_parents_zs_g()
		self.fill_parents_zs_bz()
		self.fill_parents_zs_bl()
		self.fill_parents_g_bz()
		self.fill_parents_g_bl()
		self.fill_parents_bz_bl()
		self.fill_parents_country()
		#filling gis data info
		self.fill_gis_zs()
		self.fill_gis_g()
		self.fill_gis_bz()
		self.fill_gis_bl()
		self.fill_gis_country()
		#filling population data
		if self.include_population:
			self.fill_population()

	#@check_empty(table='zones')
	def fill_zs(self,filename=None):
		self.logger.info('Filling zaehlsprengel')
		if filename is None:
			filename = self.pop_info_name+'.csv'
		self.record_file(filename=filename,filecode='zaehlsprengel')
		self.db.cursor.execute('''INSERT INTO zone_levels(name,pretty_name) VALUES('zaehlsprengel','Zählsprengel') ON CONFLICT DO NOTHING;''')
		self.db.connection.commit()
		with open(os.path.join(self.data_folder,filename),'r') as f:
			reader = csv.reader(f)
			next(reader) #remove header
			ans = [r for r in reader]
			ans = self.clean_reader(ans) # two last lines are just empty/info
		extras.execute_batch(self.db.cursor,'''INSERT INTO zones(id,name,level) VALUES(%s,%s,(SELECT id FROM zone_levels WHERE name='zaehlsprengel')) ON CONFLICT DO NOTHING;''',((int(r[3]),r[4]) for r in ans))
		self.db.connection.commit()

	def fill_population(self,filename=None):
		self.logger.info('Filling population data')
		if filename is None:
			filename = self.pop_info_name+'.csv'
		self.db.cursor.execute('''INSERT INTO zone_attribute_types(name) VALUES('zs_population') ON CONFLICT DO NOTHING;''')
		#self.db.cursor.execute('''INSERT INTO scenarios(name) VALUES('nothing') ON CONFLICT DO NOTHING;''')
		self.db.connection.commit()
		with open(os.path.join(self.data_folder,filename),'r') as f:
			reader = csv.reader(f)
			next(reader) #remove header
			ans = [r for r in reader]
			
			ans = self.clean_reader(ans) # two last lines are just empty/info
		extras.execute_batch(self.db.cursor,'''INSERT INTO zone_attributes(zone,zone_level,attribute,int_value)--,scenario)
						SELECT %s, zl.id, zat.id,%s--,s.id
						FROM zone_levels zl
						INNER JOIN zone_attribute_types zat
						ON zl.name='zaehlsprengel' AND zat.name='zs_population'
						--INNER JOIN scenarios s
						--ON s.name='nothing'
						ON CONFLICT DO NOTHING;''',((int(r[3]),r[5]) for r in ans))
		self.db.cursor.execute('''
			INSERT INTO zone_attributes(zone,zone_level,attribute,int_value)--,scenario)
				SELECT z.id, z.level, zat.id,SUM(za.int_value)--,s.id
						FROM zones z
						INNER JOIN zone_attribute_types zat
						ON zat.name='zs_population'
						--INNER JOIN scenarios s
						--ON s.name='nothing'
						INNER JOIN zone_parents zp
						ON zp.parent=z.id AND zp.parent_level=z.level
						INNER JOIN zone_levels zl
						ON zl.name='zaehlsprengel' AND zl.id=zp.child_level
						INNER JOIN zone_attributes za
						ON za.zone=zp.child AND za.zone_level=zp.child_level
						AND za.attribute=zat.id
				GROUP BY z.id, z.level,zat.id--,s.id
			ON CONFLICT DO NOTHING
			;''')
		self.db.connection.commit()

	def simplify_shapefile(self,input_path=None,output_path=None,method='visvalingam',percentage=0.2,interval=None,weight=0.5):
		'''
		Simplifying the shapefile
		Parameter choice is not really implemented in this class; in this case the name of the gis_type attribute used in the database should integrate them to avoid confusion
		'''
		if input_path is None:
			input_path = os.path.join(self.data_folder,self.gis_info_fullname)
		if output_path is None:
			output_path = os.path.join(self.data_folder,self.geojson_gis_info_name)
		if method == 'dp':
			options = ' dp'
		elif method == 'visvalingam':
			options = 'visvalingam'
		else:
			raise ValueError('method should be dp or visvalingam, not '+str(method))
		if weight is not None:
			options += ' weighted weighting={}'.format(weight)
		if percentage is not None:
			options += ' percentage={}'.format(percentage)
		if interval is not None:
			options += ' interval={}'.format(interval)
		cmd = 'mapshaper {input_path} -simplify {options} keep-shapes -proj wgs84 -o {output_path}'.format(input_path=input_path,output_path=output_path,options=options)
		self.logger.info('+ '+cmd)
		cmd_output = subprocess.check_output(cmd.split(' '))
		self.logger.info(cmd_output)

	def clean_reader(self,reader):
		ans = reader
		while ans[-1]=='' or ans[-1][0].startswith('Q: STATISTIK AUSTRIA, Statistik des Bevölkerungsstandes.') or ans[-1][0].startswith('"Q: STATISTIK AUSTRIA, Statistik des Bevölkerungsstandes.'):
			ans.pop(-1)
		# two last lines are just empty/info 
		return ans


	def fill_gis_zs(self,filename=None,gis_type=None):
		'''
		distinguishing between raw shapefile (original highly detailed geoms), or processed geojsonfile (simplified via mapshaper)
		'''
		if self.simplified:
			filetype = 'geojson'
		else:
			filetype = 'shapefile'

		if gis_type is None:
			gis_type = self.gis_type
		self.logger.info('Filling zaehlsprengel GIS')
		self.db.cursor.execute('INSERT INTO gis_types(name) VALUES(%s) ON CONFLICT DO NOTHING;',(gis_type,))
		self.db.connection.commit()
		if filetype == 'geojson':
			if filename is None:
				filename = self.geojson_gis_info_name # for children classes
			self.record_file(filename=filename,filecode='zaehlsprengel_geojson')
			with open(os.path.join(self.data_folder,filename),'r') as f:
				zs_geo = json.load(f)
			extras.execute_batch(self.db.cursor,'''INSERT INTO gis_data(zone_id,zone_level,geom,center,gis_type) VALUES (%s,(SELECT id FROM zone_levels WHERE name=%s),ST_SetSRID(ST_GeomFromGeoJSON(%s),4326),ST_SetSRID(ST_Centroid(ST_GeomFromGeoJSON(%s)),4326),(SELECT id FROM gis_types WHERE name=%s)) ON CONFLICT DO NOTHING;''',((int(gj['properties']['id']),'zaehlsprengel',str(gj['geometry']),str(gj['geometry']),gis_type) for gj in zs_geo['features']))
		elif filetype == 'shapefile':
			if filename is None:
				filename = self.gis_info_fullname
			self.record_file(filename=filename,filecode='zaehlsprengel_shapefile')
			with shapefile.Reader(os.path.join(self.data_folder,filename)) as sf:
				extras.execute_batch(self.db.cursor,'''INSERT INTO gis_data(zone_id,zone_level,geom,center,gis_type) VALUES (%s,(SELECT id FROM zone_levels WHERE name=%s),ST_Transform(ST_SetSRID(ST_GeomFromGeoJSON(%s),31287),4326),ST_Centroid(ST_Transform(ST_SetSRID(ST_GeomFromGeoJSON(%s),31287),4326)),(SELECT id FROM gis_types WHERE name=%s)) ON CONFLICT DO NOTHING;''',((int(r[0]),'zaehlsprengel',json.dumps(s.__geo_interface__),json.dumps(s.__geo_interface__),gis_type) for s,r in zip(sf.shapes(),sf.records())))
		else:
			raise ValueError('ZS filetype unknown:',filetype)
		self.db.connection.commit()
	def fill_gemeinde(self,filename=None):
		self.logger.info('Filling Gemeinde')
		if filename is None:
			filename = self.pop_info_name+'.csv'
		self.record_file(filename=filename,filecode='zaehlsprengel')
		self.db.cursor.execute('''INSERT INTO zone_levels(name,pretty_name) VALUES('gemeinde','Gemeinde') ON CONFLICT DO NOTHING;''')
		self.db.connection.commit()
		with open(os.path.join(self.data_folder,filename),'r') as f:
			reader = csv.reader(f)
			next(reader) #remove header
			ans = [r for r in reader]
			ans = self.clean_reader(ans)
			
		extras.execute_batch(self.db.cursor,'''INSERT INTO zones(id,name,level) VALUES(%s,%s,(SELECT id FROM zone_levels WHERE name='gemeinde')) ON CONFLICT DO NOTHING;''',((int(r[1]),r[2]) for r in ans))
		self.db.connection.commit()

	def fill_bezirk(self,filename=None):
		self.logger.info('Filling Bezirk')

		if filename is None:
			filename = self.bezirk_info_name
		self.record_file(filename=filename,filecode='bezirk')
		self.db.cursor.execute('''INSERT INTO zone_levels(name,pretty_name) VALUES('bezirk','Bezirk') ON CONFLICT DO NOTHING;''')
		self.db.connection.commit()
		with open(os.path.join(self.data_folder,filename),'r') as f:
			reader = csv.reader(f,delimiter=';')
			next(reader)
			next(reader)
			next(reader)
			ans = [r for r in reader]
			ans.pop(-1) # last line and 3 first lines are just empty/info
		extras.execute_batch(self.db.cursor,'''INSERT INTO zones(id,name,level) VALUES(%s,%s,(SELECT id FROM zone_levels WHERE name='bezirk')) ON CONFLICT DO NOTHING;''',((int(r[4]),r[3]) for r in ans if (not self.remove_bz_900 or int(r[4])!=900)))
		self.db.connection.commit()

	def fill_bundesland(self,filename=None):
		self.logger.info('Filling Bundesland')

		if filename is None:
			filename = self.bezirk_info_name
		self.record_file(filename=filename,filecode='bezirk')
		self.db.cursor.execute('''INSERT INTO zone_levels(name,pretty_name) VALUES('bundesland','Bundesland') ON CONFLICT DO NOTHING;''')
		self.db.connection.commit()
		with open(os.path.join(self.data_folder,filename),'r') as f:
			reader = csv.reader(f,delimiter=';')
			next(reader)
			next(reader)
			next(reader)
			ans = [r for r in reader]
			ans.pop(-1) # last line and 3 first lines are just empty/info
		extras.execute_batch(self.db.cursor,'''INSERT INTO zones(id,name,level) VALUES(%s,%s,(SELECT id FROM zone_levels WHERE name='bundesland')) ON CONFLICT DO NOTHING;''',((int(r[0]),r[1]) for r in ans))
		self.db.connection.commit()

	def fill_country(self):
		self.logger.info('Filling Country')

		self.db.cursor.execute('''INSERT INTO zone_levels(name,pretty_name) VALUES('country','Country') ON CONFLICT DO NOTHING;''')
		self.db.connection.commit()
		self.db.cursor.execute('''INSERT INTO zones(name,code,level) VALUES('Österreich','AT',(SELECT id FROM zone_levels WHERE name='country')) ON CONFLICT DO NOTHING;''')
		self.db.connection.commit()

	def fill_parents_zs_g(self):
		self.logger.info('Filling zaehlsprengel gemeinde parents')

		self.db.cursor.execute('''
			INSERT INTO zone_parents(parent_level,parent,child_level,child)
							(SELECT zg.level,zg.id,zz.level,zz.id FROM
								zones zg
								INNER JOIN zones zz
									ON zg.level=(SELECT id FROM zone_levels WHERE name='gemeinde')
										AND zz.level=(SELECT id FROM zone_levels WHERE name='zaehlsprengel')
										AND zz.id/1000=zg.id
							)
								ON CONFLICT DO NOTHING
								;''')
		self.db.connection.commit()

	def fill_parents_zs_bz(self):
		self.logger.info('Filling zaehlsprengel bezirk parents')
		self.db.cursor.execute('''
			INSERT INTO zone_parents(parent_level,parent,child_level,child)
							(SELECT zg.level,zg.id,zz.level,zz.id FROM
								zones zg
								INNER JOIN zones zz
									ON zg.level=(SELECT id FROM zone_levels WHERE name='bezirk')
										AND zz.level=(SELECT id FROM zone_levels WHERE name='zaehlsprengel')
										AND zz.id/100000=zg.id
							)
								ON CONFLICT DO NOTHING
								;''')
		self.db.connection.commit()

	def fill_parents_zs_bl(self):
		self.logger.info('Filling zaehlsprengel bundesland parents')
		self.db.cursor.execute('''
			INSERT INTO zone_parents(parent_level,parent,child_level,child)
							(SELECT zg.level,zg.id,zz.level,zz.id FROM
								zones zg
								INNER JOIN zones zz
									ON zg.level=(SELECT id FROM zone_levels WHERE name='bundesland')
										AND zz.level=(SELECT id FROM zone_levels WHERE name='zaehlsprengel')
										AND zz.id/10000000=zg.id
							)
								ON CONFLICT DO NOTHING
								;''')
		self.db.connection.commit()

	def fill_parents_g_bz(self):
		self.logger.info('Filling gemeinde bezirk parents')
		self.db.cursor.execute('''
			INSERT INTO zone_parents(parent_level,parent,child_level,child)
							(SELECT zg.level,zg.id,zz.level,zz.id FROM
								zones zg
								INNER JOIN zones zz
									ON zg.level=(SELECT id FROM zone_levels WHERE name='bezirk')
										AND zz.level=(SELECT id FROM zone_levels WHERE name='gemeinde')
										AND zz.id/100=zg.id
							)
								ON CONFLICT DO NOTHING
								;''')
		self.db.connection.commit()

	def fill_parents_g_bl(self):
		self.logger.info('Filling gemeinde bundesland parents')
		self.db.cursor.execute('''
			INSERT INTO zone_parents(parent_level,parent,child_level,child)
							(SELECT zg.level,zg.id,zz.level,zz.id FROM
								zones zg
								INNER JOIN zones zz
									ON zg.level=(SELECT id FROM zone_levels WHERE name='bundesland')
										AND zz.level=(SELECT id FROM zone_levels WHERE name='gemeinde')
										AND zz.id/10000=zg.id
							)
								ON CONFLICT DO NOTHING
								;''')
		self.db.connection.commit()

	def fill_parents_bz_bl(self):
		self.logger.info('Filling bezirk bundesland parents')
		self.db.cursor.execute('''
			INSERT INTO zone_parents(parent_level,parent,child_level,child)
							(SELECT zg.level,zg.id,zz.level,zz.id FROM
								zones zg
								INNER JOIN zones zz
									ON zg.level=(SELECT id FROM zone_levels WHERE name='bundesland')
										AND zz.level=(SELECT id FROM zone_levels WHERE name='bezirk')
										AND zz.id/100=zg.id
							)
								ON CONFLICT DO NOTHING
								;''')
		self.db.connection.commit()

	def fill_parents_country(self):
		self.logger.info('Filling all country parents')
		self.db.cursor.execute('''
			INSERT INTO zone_parents(parent_level,parent,child_level,child)
							(SELECT zg.level,zg.id,zz.level,zz.id FROM
								zones zg
								INNER JOIN zones zz
									ON zg.level=(SELECT id FROM zone_levels WHERE name='country')
									AND zg.code = 'AT'
								INNER JOIN (VALUES  ('zaehlsprengel'),('bezirk'),('bundesland'),('gemeinde')) as l(lev_name)
										ON zz.level=(SELECT id FROM zone_levels WHERE name=l.lev_name)
							)
								ON CONFLICT DO NOTHING
								;''')
		self.db.connection.commit()

	def fill_gis_g(self,gis_type=None):
		'''
		Should be executed after fill_gis_zs
		'''
		if gis_type is None:
			gis_type = self.gis_type
		self.logger.info('Filling gemeinde GIS')

		self.db.cursor.execute('''
			INSERT INTO gis_data(zone_id,zone_level,geom,center,gis_type)
				SELECT zp.parent,zp.parent_level,ST_Union(gd.geom),ST_Centroid(ST_Union(gd.geom)),(SELECT id FROM gis_types WHERE name=%s)
					FROM gis_data gd
						INNER JOIN zone_parents zp
							ON zp.child=gd.zone_id AND zp.child_level=gd.zone_level
							AND gd.gis_type=(SELECT id FROM gis_types WHERE name=%s)
							AND zp.parent_level=(SELECT id FROM zone_levels WHERE name='gemeinde')
							AND zp.child_level=(SELECT id FROM zone_levels WHERE name='zaehlsprengel')
					GROUP BY zp.parent,zp.parent_level
				ON CONFLICT DO NOTHING
			;''',(gis_type,gis_type,))
		self.db.connection.commit()

	def fill_gis_bz(self,gis_type=None):
		'''
		Should be executed after fill_gis_g
		'''
		if gis_type is None:
			gis_type = self.gis_type

		self.logger.info('Filling bezirk GIS')

		self.db.cursor.execute('''
			INSERT INTO gis_data(zone_id,zone_level,geom,center,gis_type)
				SELECT zp.parent,zp.parent_level,ST_Union(gd.geom),ST_Centroid(ST_Union(gd.geom)),(SELECT id FROM gis_types WHERE name=%s)
					FROM gis_data gd
						INNER JOIN zone_parents zp
							ON zp.child=gd.zone_id AND zp.child_level=gd.zone_level
							AND gd.gis_type=(SELECT id FROM gis_types WHERE name=%s)
							AND zp.parent_level=(SELECT id FROM zone_levels WHERE name='bezirk')
							AND zp.child_level=(SELECT id FROM zone_levels WHERE name='gemeinde')
					GROUP BY zp.parent,zp.parent_level
				 ON CONFLICT DO NOTHING
			;''',(gis_type,gis_type,))
		self.db.connection.commit()

	def fill_gis_bl(self,gis_type=None):
		'''
		Should be executed after fill_gis_bz
		'''
		if gis_type is None:
			gis_type = self.gis_type

		self.logger.info('Filling bundesland GIS')

		self.db.cursor.execute('''
			INSERT INTO gis_data(zone_id,zone_level,geom,center,gis_type)
				SELECT zp.parent,zp.parent_level,ST_Union(gd.geom),ST_Centroid(ST_Union(gd.geom)),(SELECT id FROM gis_types WHERE name=%s)
					FROM gis_data gd
						INNER JOIN zone_parents zp
							ON zp.child=gd.zone_id AND zp.child_level=gd.zone_level
							AND gd.gis_type=(SELECT id FROM gis_types WHERE name=%s)
							AND zp.parent_level=(SELECT id FROM zone_levels WHERE name='bundesland')
							AND zp.child_level=(SELECT id FROM zone_levels WHERE name='bezirk')
					GROUP BY zp.parent,zp.parent_level
				 ON CONFLICT DO NOTHING
			;''',(gis_type,gis_type,))
		self.db.connection.commit()


	def fill_gis_country(self,gis_type=None):
		if gis_type is None:
			gis_type = self.gis_type

		self.logger.info('Filling country GIS')

		self.db.cursor.execute('''
			INSERT INTO gis_data(zone_id,zone_level,geom,center,gis_type)
				SELECT zp.parent,zp.parent_level,ST_Union(gd.geom),ST_Centroid(ST_Union(gd.geom)),(SELECT id FROM gis_types WHERE name=%s)
					FROM gis_data gd
						INNER JOIN zone_parents zp
							ON zp.child=gd.zone_id AND zp.child_level=gd.zone_level
							AND gd.gis_type=(SELECT id FROM gis_types WHERE name=%s)
							AND zp.parent_level=(SELECT id FROM zone_levels WHERE name='country')
							AND zp.child_level=(SELECT id FROM zone_levels WHERE name='bundesland')
					GROUP BY zp.parent,zp.parent_level
				 ON CONFLICT DO NOTHING
			;''',(gis_type,gis_type,))
		self.db.connection.commit()


class SimplifiedZSFiller(ZaehlsprengelFiller):
	def __init__(self,**kwargs):
		ZaehlsprengelFiller.__init__(self,simplified=True,**kwargs)

class PopulationZSFiller(ZaehlsprengelFiller):
	def __init__(self,force=False,**kwargs):
		ZaehlsprengelFiller.__init__(self,**kwargs)
		self.force = force
		self.name = 'population_zs'

	def apply(self):
		if self.force or not self.check_done():
			self.fill_population()

	def check_done(self):
		self.db.cursor.execute('''
				SELECT z.id, z.level, zat.id--,s.id
						FROM zones z
						INNER JOIN zone_attribute_types zat
						ON zat.name='zs_population'
						--INNER JOIN scenarios s
						--ON s.name='nothing'
						INNER JOIN zone_parents zp
						ON zp.parent=z.id AND zp.parent_level=z.level
						INNER JOIN zone_levels zl
						ON zl.name='zaehlsprengel' AND zl.id=zp.child_level
						INNER JOIN zone_attributes za
						ON za.zone=zp.child AND za.zone_level=zp.child_level
						AND za.attribute=zat.id
				LIMIT 1
				;''')
		return (self.db.cursor.fetchone() is not None)



class PLZFiller(fillers.Filler):
	'''
	Filling in PLZ info from statistik.at
	'''

	def __init__(self,
					file_info="http://www.statistik.at/verzeichnis/reglisten/gemliste_knz.csv",
					file_info_name="gemliste_knz.csv",
					force=False,
					**kwargs):
		self.force = force
		self.file_info = file_info
		self.file_info_name = file_info_name
		fillers.Filler.__init__(self,name='PLZ_gemeinde',**kwargs)

	def prepare(self):
		if self.data_folder is None:
			self.data_folder = self.db.data_folder
		data_folder = self.data_folder

		#create folder if needed
		if not os.path.exists(data_folder):
			os.makedirs(data_folder)

		# self.db.cursor.execute('''
		# 	SELECT * FROM plz_gemeinde
		# 	LIMIT 1
		# 	;''')
		# query_ans = self.db.cursor.fetchone()
		query_ans = None
		if query_ans is not None:
			self.done = True
		else:
			if not os.path.exists(os.path.join(data_folder,self.file_info_name)):
				if not os.path.exists(os.path.join(data_folder,self.file_info_name)):
					self.download(url=self.file_info,destination=os.path.join(data_folder,self.file_info_name))

	def apply(self):
		self.fill_plz()

	def fill_plz(self,filename=None):

		self.logger.info('Filling countries')
		if filename is None:
			filename = self.file_info_name # for children classes
		self.record_file(filename=filename,filecode='plz_gemeinde')
		with open(os.path.join(self.data_folder,filename),'r') as f:
			reader = csv.reader(f,delimiter=';')
			next(reader) #remove header
			next(reader) #remove header
			next(reader) #remove header
			ans = [r for r in reader]
			ans.pop(-1)
		insert_input = []
		for (gd,gd_n,gd2,st,plz,plz_other) in ans:
			insert_input.append((gd2,gd_n,plz))
			if plz_other != '':
				for plz_o in set(plz_other.split(' ')):
					insert_input.append((gd2,gd_n,plz_o))
		extras.execute_batch(self.db.cursor,'''INSERT INTO plz_gemeinde(plz,gemeinde,gemeinde_name)
				VALUES(%(plz)s,
					%(gemeinde)s,
					%(gemeinde_name)s)
				 ON CONFLICT DO NOTHING
				 ;''',({'plz':plz,'gemeinde':gd,'gemeinde_name':gd_n} for (gd,gd_n,plz) in insert_input))
		self.db.connection.commit()
