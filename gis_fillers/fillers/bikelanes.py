import os
import logging
import csv
import copy
from psycopg2 import extras
from . import fillers

import osmnx as ox
import datetime

from shapely.ops import unary_union as shapely_uu

class RoadsFiller(fillers.Filler):
	"""
	Fills in countries GIS shapes in zaehlsprengel gis_type
	"""

	def __init__(self,
				road_type='drive',
				timestamp=None,
				retain_all=True,
				simplify=True,
				bounding_box = [[16.181830439863365, 16.577514090292098],
								[48.117903306831614, 48.32266657781503]],
					**kwargs):
		self.retain_all = retain_all
		self.simplify = simplify
		self.bbox = copy.deepcopy(bounding_box)
		if timestamp is None:
			timestamp = datetime.datetime.now()
		self.timestamp = timestamp

		self.road_type = road_type
		fillers.Filler.__init__(self,name='roads_'+self.road_type,**kwargs)

	def prepare(self):
		if self.data_folder is None:
			self.data_folder = self.db.data_folder
		data_folder = self.data_folder

		#create folder if needed
		if not os.path.exists(data_folder):
			os.makedirs(data_folder)

		self.db.cursor.execute('''
			CREATE TABLE IF NOT EXISTS batch_roads(
				road_type TEXT NOT NULL,
				data_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
				geom GEOMETRY,
				PRIMARY KEY(road_type,data_timestamp)
				);
			CREATE INDEX IF NOT EXISTS batchroads_idx2 ON batch_roads(data_timestamp,road_type);
			CREATE INDEX IF NOT EXISTS batchroads_gist_idx ON batch_roads USING GIST (geom);
			''')
		self.db.connection.commit()

		self.db.cursor.execute('''
			SELECT COUNT(*)
					FROM batch_roads
					WHERE road_type=%(road_type)s
					AND data_timestamp=%(t)s
			;''',{'road_type':self.road_type,'t':self.timestamp})
		query_ans = self.db.cursor.fetchone()
		self.db.connection.commit()
		if query_ans is not None and query_ans[0] > 0:
			self.done = True
		else:
			settings_str = "[out:json][timeout:{{timeout}}]{{maxsize}}[date:'{t}']".format(t=self.timestamp.strftime('%Y-%m-%dT%H:%M:%SZ'))
			ox.utils.config(overpass_settings = settings_str) 
			x,y = self.bbox
			if self.road_type == 'cycleway':
				G = ox.graph_from_bbox( y[0], y[1], x[0], x[1], custom_filter = '["cycleway"]',network_type='drive', simplify=self.simplify, retain_all=self.retain_all)
			else:
				G = ox.graph_from_bbox( y[0], y[1], x[0], x[1], network_type=self.road_type, simplify=self.simplify, retain_all=self.retain_all)
			self.edges = ox.graph_to_gdfs(G, nodes=False, edges=True)

	def apply(self):
		self.fill_roads()


	def fill_roads(self):

		self.db.cursor.execute('''
			INSERT INTO batch_roads(road_type,data_timestamp,geom)
			VALUES(%(road_type)s,%(t)s,ST_SetSRID(%(geom)s::geometry, %(srid)s))
			ON CONFLICT DO NOTHING
			;''',{'road_type':self.road_type,'t':self.timestamp,'geom':shapely_uu(self.edges['geometry']).wkt,'srid':4326})

		self.db.connection.commit()


class RoadLengthFiller(fillers.Filler):
	def __init__(self,zone_level='zaehlsprengel',gis_type='zaehlsprengel',**kwargs):
		self.zone_level = zone_level
		self.gis_type = gis_type
		fillers.Filler.__init__(self,**kwargs)

	def prepare(self):

		if self.data_folder is None:
			self.data_folder = self.db.data_folder
		data_folder = self.data_folder

		#create folder if needed
		if not os.path.exists(data_folder):
			os.makedirs(data_folder)

		self.db.cursor.execute('''
			CREATE TABLE IF NOT EXISTS road_lengths(
				road_type TEXT NOT NULL,
				data_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
				zone_level INT,
				zone_id BIGINT,
				road_length DOUBLE PRECISION,
				FOREIGN KEY (zone_level,zone_id) REFERENCES zones(level,id) ON DELETE CASCADE,
				FOREIGN KEY (road_type,data_timestamp) REFERENCES batch_roads(road_type,data_timestamp) ON DELETE CASCADE,
				PRIMARY KEY(road_type,data_timestamp,zone_level,zone_id)
				);
			CREATE INDEX IF NOT EXISTS lenroads_idx2 ON road_lengths(zone_level,zone_id,data_timestamp,road_type);
			''')
		self.db.connection.commit()

	def apply(self):
		self.db.cursor.execute('''
			INSERT INTO road_lengths(road_type,data_timestamp,zone_level,zone_id,road_length)
			SELECT br.road_type,br.data_timestamp,z.level,z.id,COALESCE(rl.road_length,ST_Length(ST_Intersection(gd.geom,br.geom),true))
				FROM batch_roads br
				INNER JOIN zone_levels zl
				ON zl.name=%(zone_level)s
				INNER JOIN zones z
				ON z.level=zl.id
				INNER JOIN gis_types gt
				ON gt.name=%(gis_type)s
				INNER JOIN gis_data gd
				ON gd.gis_type=gt.id
				AND gd.zone_level=z.level
				AND gd.zone_id=z.id
				LEFT OUTER JOIN road_lengths rl
				ON rl.road_type=br.road_type
				AND rl.data_timestamp = br.data_timestamp
				AND rl.zone_level=z.level
				AND rl.zone_id=z.id
			ON CONFLICT DO NOTHING
			;''',{'zone_level':self.zone_level,'gis_type':self.gis_type})
		# self.db.cursor.execute('''
		# 	DELETE FROM road_lengths
		# 	WHERE road_length = 0
		# 	;''')
		self.db.connection.commit()
