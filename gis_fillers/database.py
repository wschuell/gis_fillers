
import psycopg2
from psycopg2 import extras,sql
import os
import copy
import logging
import csv
import hashlib
import numpy as np


from db_fillers import Database as TemplateDatabase

logger = logging.getLogger(__name__)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
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
	logger.info('Psycopg2 not installed, pip install psycopg2 (or binary-psycopg2) if you want to use a PostgreSQL DB')



def split_sql_init(script):
	lines = script.split('\n')
	formatted = '\n'.join([l for l in lines if l[:2]!='--'])
	return formatted.split(';')[:-1]

class Database(TemplateDatabase):
	"""
	This class creates a database object with the main structure, with a few methods  to manipulate it.
	To fill it, fillers are used (see Filler class).
	The object uses a specific data folder and a list of files used for the fillers, with name, keyword, and potential download link. (move to filler class?)
	"""

	def clean_db(self,gis_data_stay=False,commit=True,**kwargs):
		TemplateDatabase.clean_db(self,commit=False,**kwargs)
		self.cursor.execute('DROP TABLE IF EXISTS plz_gemeinde CASCADE;')
		self.cursor.execute('DROP TABLE IF EXISTS data_sources CASCADE;')
		if not gis_data_stay:
			self.logger.info('Cleaning GIS info')
			self.cursor.execute('DROP TABLE IF EXISTS zones CASCADE;')
			self.cursor.execute('DROP TABLE IF EXISTS zone_parents CASCADE;')
			self.cursor.execute('DROP TABLE IF EXISTS zone_levels CASCADE;')
			self.cursor.execute('DROP TABLE IF EXISTS gis_data CASCADE;')
			self.cursor.execute('DROP TABLE IF EXISTS gis_types CASCADE;')
		self.cursor.execute('DROP TABLE IF EXISTS zone_attribute_types CASCADE;')
		self.cursor.execute('DROP TABLE IF EXISTS zone_attributes CASCADE;')
		if commit:
			self.connection.commit()
