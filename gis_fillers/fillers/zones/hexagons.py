import os
import requests
import zipfile
import logging
import csv
from psycopg2 import extras
import shapefile
import json
import subprocess
import h3
import geopandas as gpd
import pandas as pd

import shapely
from shapely.ops import unary_union
from shapely.geometry import mapping, Polygon
from .. import fillers


class HexagonsFiller(fillers.Filler):
	def __init__(self,
			res=10,
			target_zone='AT',
			target_zone_level='country',
			gis_type='zaehlsprengel',
			buffer=True,
			truncate_shapes=True,
			**kwargs):
		fillers.Filler.__init__(self,**kwargs)
		self.res = res
		self.buffer = buffer
		self.buffer_meters = h3.edge_length(self.res,unit='m')*2.
		self.truncate_shapes = truncate_shapes
		self.target_zone = target_zone
		self.target_zone_level = target_zone_level
		self.zone_level = f'{target_zone_level}_{target_zone}_hexagons_{res}'
		self.gis_type = gis_type

	def get_hexagons(self):
		if not hasattr(self,'hexagons'):

			gdf_buffered = gpd.GeoDataFrame.from_postgis(con=self.db.connection,crs=4326,sql='''
				SELECT ST_Buffer(gd.geom::geography,%(buffer)s) AS geom FROM zone_levels zl
				INNER JOIN zones z
				ON zl.id=z.level AND COALESCE(z.code,z.id::text)=%(target_zone)s::text
				AND zl.name=%(target_zone_level)s
				INNER JOIN gis_types gt
				ON gt.name=%(gis_type)s
				INNER JOIN gis_data gd
				ON gd.gis_type=gt.id
				AND gd.zone_id=z.id AND gd.zone_level=z.level
				;
				''',params={'target_zone':self.target_zone,'gis_type':self.gis_type,'target_zone_level':self.target_zone_level,'buffer':(self.buffer_meters if self.buffer else 0)})


			gdf = gpd.GeoDataFrame.from_postgis(con=self.db.connection,crs=4326,sql='''
				SELECT gd.geom::geography AS geom FROM zone_levels zl
				INNER JOIN zones z
				ON zl.id=z.level AND COALESCE(z.code,z.id::text)=%(target_zone)s::text
				AND zl.name=%(target_zone_level)s
				INNER JOIN gis_types gt
				ON gt.name=%(gis_type)s
				INNER JOIN gis_data gd
				ON gd.gis_type=gt.id
				AND gd.zone_id=z.id AND gd.zone_level=z.level
				;
				''',params={'target_zone':self.target_zone,'gis_type':self.gis_type,'target_zone_level':self.target_zone_level,'buffer':(self.buffer_meters if self.buffer else 0)})


			# Get union of the shape (whole US)
			union_poly = unary_union(gdf_buffered.geometry)
			if self.buffer:
				orig_union_poly = unary_union(gdf.geometry)
			else:
				orig_union_poly = union_poly

			# Find the hexagons within the shape boundary using PolyFill
			hex_list=[]
			if hasattr(union_poly,'geoms'):
				for n,g in enumerate(union_poly.geoms):
					temp = mapping(g)
					temp['coordinates']=[[[j[1],j[0]] for j in i] for i in temp['coordinates']]
					hex_list.extend(h3.polyfill(temp,res=self.res))
			else:
				temp = mapping(union_poly)
				temp['coordinates']=[[[j[1],j[0]] for j in i] for i in temp['coordinates']]
				hex_list.extend(h3.polyfill(temp,res=self.res))

			# Create hexagon data frame
			ans_hex = pd.DataFrame(hex_list,columns=["hex_id"])

			# Create hexagon geometry and GeoDataFrame
			polygons = [Polygon(h3.h3_to_geo_boundary(x, geo_json=True)) for x in ans_hex["hex_id"]]
			polygons_truncated = [orig_union_poly.intersection(p) for p in polygons]
			if self.truncate_shapes:
				ans_hex['geometry'] = [(p if p.area>0 else None) for p in polygons_truncated]
			else:
				ans_hex['geometry'] = [(p if pt.area>0 else None) for pt,p in zip(polygons_truncated,polygons)]
			ans_hex = ans_hex.dropna()
			ans_hex = gpd.GeoDataFrame(ans_hex).set_index('hex_id')
			self.hexagons = ans_hex

	def apply(self):
		self.fill_hexagons()
		self.fill_parents()
		self.fill_children()

	def fill_hexagons(self):
		self.get_hexagons()
		hex_gdf = self.hexagons
		self.db.cursor.execute('''
			INSERT INTO gis_types(name) SELECT %(gis_type)s
			ON CONFLICT DO NOTHING;
			''',{'gis_type':self.gis_type})

		self.db.cursor.execute('''
			INSERT INTO zone_levels(name) SELECT %(zone_level)s
			ON CONFLICT DO NOTHING;
			''',{'zone_level':self.zone_level})

		extras.execute_batch(self.db.cursor,'''
			INSERT INTO zones(code,name,level)
			VALUES(%(hex_id)s,
					%(hex_id)s,
					(SELECT id FROM zone_levels WHERE name=%(zone_level)s))
				 ON CONFLICT DO NOTHING;
			''',({'hex_id':a[0],'geom':a[1],'zone_level':self.zone_level} for a in hex_gdf.itertuples()))
		extras.execute_batch(self.db.cursor,'''
			INSERT INTO gis_data(
						zone_id,
						zone_level,
						geom,
						center,
						gis_type)
				VALUES ((SELECT z.id FROM zones z INNER JOIN zone_levels zl ON z.code=%(hex_id)s AND zl.name=%(zone_level)s AND z.level=zl.id),
					(SELECT id FROM zone_levels WHERE name=%(zone_level)s),
					ST_SetSRID(ST_GeomFromWKB(%(geom)s::geometry),4326),
					ST_Centroid(ST_SetSRID(ST_GeomFromWKB(%(geom)s::geometry),4326)),
					(SELECT id FROM gis_types WHERE name=%(gis_type)s))
					ON CONFLICT DO NOTHING
			''',({'hex_id':a[0],'geom':a[1].wkb_hex,'zone_level':self.zone_level,'gis_type':self.gis_type} for a in hex_gdf.itertuples()))
		self.db.connection.commit()

	def fill_parents(self):
		self.logger.info(f'Filling {self.zone_level} parents')

		self.db.cursor.execute('''
			INSERT INTO zone_parents(parent_level,parent,child_level,child,share)
							(
SELECT gdp.zone_level,gdp.zone_id,gdc.zone_level,gdc.zone_id,ST_Area(ST_Intersection(gdc.geom,gdp.geom),false)/ST_Area(gdc.geom,false) AS share FROM zone_levels zlc
INNER JOIN zones zc
ON zlc.name=%(zone_level)s
AND zc.LEVEL=zlc.id
INNER JOIN gis_types gt
ON gt.name=%(gis_type)s
INNER JOIN gis_data gdc
ON gdc.zone_id =zc.id AND gdc.zone_level=zc.level AND gdc.gis_type=gt.id
INNER JOIN gis_data gdp
ON gdp.gis_type=gt.id AND ST_Intersects(gdc.geom,gdp.geom)
							)
								ON CONFLICT DO NOTHING
								;''',{'gis_type':self.gis_type,'zone_level':self.zone_level})
		self.db.connection.commit()

	def fill_children(self):
		self.logger.info(f'Filling {self.zone_level} children')

		self.db.cursor.execute('''
			INSERT INTO zone_parents(parent_level,parent,child_level,child,share)
							(
SELECT gdp.zone_level,gdp.zone_id,gdc.zone_level,gdc.zone_id,ST_Area(ST_Intersection(gdc.geom,gdp.geom),false)/ST_Area(gdc.geom,false) AS share FROM zone_levels zlp
INNER JOIN zones zp
ON zlp.name=%(zone_level)s
AND zp.LEVEL=zlp.id
INNER JOIN gis_types gt
ON gt.name=%(gis_type)s
INNER JOIN gis_data gdp
ON gdp.zone_id =zp.id AND gdp.zone_level=zp.level AND gdp.gis_type=gt.id
INNER JOIN gis_data gdc
ON gdc.gis_type=gt.id AND ST_Intersects(gdc.geom,gdp.geom)
							)
								ON CONFLICT DO NOTHING
								;''',{'gis_type':self.gis_type,'zone_level':self.zone_level})
		self.db.connection.commit()
