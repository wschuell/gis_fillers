
import psycopg2
from psycopg2 import extras,sql
import os
import copy
import logging
import csv
import hashlib
import numpy as np


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



with open(os.path.join(os.path.dirname(__file__),'init_script.sql'),'r') as f:
	DB_INIT = f.read()


def split_sql_init(script):
	lines = script.split('\n')
	formatted = '\n'.join([l for l in lines if l[:2]!='--'])
	return formatted.split(';')[:-1]

class Database(object):
	"""
	This class creates a database object with the main structure, with a few methods  to manipulate it.
	To fill it, fillers are used (see Filler class).
	The object uses a specific data folder and a list of files used for the fillers, with name, keyword, and potential download link. (move to filler class?)
	"""

	def __init__(self,pre_initscript='',post_initscript='',data_folder='./datafolder',register_exec=True,**db_conninfo):
		self.logger = logger
		self.db_conninfo = copy.deepcopy(db_conninfo) # db_conninfo can be partly defined in ~/.pgpass, especially for passwords. See postgres doc for more info.
		if 'password' in self.db_conninfo.keys():
			logger.warning('You are providing your password directly, this could be a security concern, consider using solutions like .pgpass file.')
		try:
			self.connection = psycopg2.connect(**self.db_conninfo)
		except psycopg2.OperationalError as e:
			if 'FATAL:  database "{}" does not exist\n'.format(db_conninfo['database']) == str(e):
				pgpass_env = 'PGPASSFILE'
				default_pgpass = os.path.join(os.environ['HOME'],'.pgpass')
				if pgpass_env not in os.environ.keys():
					os.environ[pgpass_env] = default_pgpass
				conninfo_nodb = copy.deepcopy(self.db_conninfo)
				conninfo_nodb['database'] = 'postgres'
				conn = psycopg2.connect(**conninfo_nodb)
				conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
				cur = conn.cursor()
				cur.execute(psycopg2.sql.SQL(
					"CREATE DATABASE {};"
					).format(psycopg2.sql.Identifier(self.db_conninfo['database'])))
				cur.close()
				conn.close()
				self.connection = psycopg2.connect(**self.db_conninfo)
			else:
				pgpass_env = 'PGPASSFILE'
				default_pgpass = os.path.join(os.environ['HOME'],'.pgpass')
				if pgpass_env not in os.environ.keys():
					os.environ[pgpass_env] = default_pgpass
					self.logger.info('Password authentication failed,trying to set .pgpass env variable')
					self.connection = psycopg2.connect(**self.db_conninfo)
				else:
					raise
		self.cursor = self.connection.cursor()

		self.register_exec = register_exec

		self.fillers = []
		self.data_folder = data_folder
		if not os.path.exists(self.data_folder):
			os.makedirs(self.data_folder)
		self.pre_initscript = pre_initscript
		self.post_initscript = post_initscript

	def clean_db(self,gis_data_stay=False):
		self.logger.info('Cleaning DB')
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
		self.connection.commit()

	def init_db(self):
		for cmd in split_sql_init(DB_INIT)+split_sql_init(self.pre_initscript)+split_sql_init(self.post_initscript):
			self.logger.debug(cmd)
			self.cursor.execute(cmd)
		if self.register_exec:
			self.register_exec_content()
		self.connection.commit()


	def fill_db(self):
		for f in self.fillers:
			if not f.done:
				self.register_filler_content(filler_class=f.__class__.__name__,filler_args=f.get_relevant_attr_string(),status='init_prepare')
				f.prepare()
				self.logger.info('Prepared filler {}'.format(f.name))
				self.register_filler_content(filler_class=f.__class__.__name__,filler_args=f.get_relevant_attr_string(),status='end_prepare')
		# for f in self.fillers:
				if not f.done:
					self.register_filler_content(filler_class=f.__class__.__name__,filler_args=f.get_relevant_attr_string(),status='init_apply')
					f.apply()
					f.done = True
					self.register_filler_content(filler_class=f.__class__.__name__,filler_args=f.get_relevant_attr_string(),status='end_apply')
			self.logger.info('Filled with filler {}'.format(f.name))

	def add_filler(self,f):
		if f.name in [ff.name for ff in self.fillers if ff.unique_name]:
			self.logger.warning('Filler {} already present'.format(f.name))
		else:
			f.db = self
			self.fillers.append(f)
			f.logger = self.logger
			f.after_insert()
			self.logger.info('Added filler {}'.format(f.name))

	def check_empty(self,table):
		self.cursor.execute('SELECT * FROM {table} LIMIT 1;'.format(table=table))
		ans = self.cursor.fetchone()
		return (ans is None)



########### files management
	def record_file(self,filename,filecode,folder=None):
		self.cursor.execute('''CREATE TABLE IF NOT EXISTS file_hash(
								filecode TEXT PRIMARY KEY,
								filename TEXT,
								hashtype TEXT DEFAULT 'SHA256',
								updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
								filehash TEXT NOT NULL
								);''')
		self.connection.commit()
		if folder is None:
			folder = self.data_folder
		with open(os.path.join(folder,filename),"rb") as f:
			filehash = hashlib.sha256(f.read()).hexdigest()
		self.cursor.execute('INSERT INTO file_hash(filecode,filename,filehash) VALUES(%s,%s,%s) ON CONFLICT (filecode) DO UPDATE SET filecode=EXCLUDED.filecode,filename=EXCLUDED.filename,filehash=EXCLUDED.filehash;',(filecode,filename,filehash))

	def register_exec_content(self):
		import __main__
		with open(__main__.__file__,'r') as f:
			exec_content = f.read() 
		with open(__main__.__file__,"rb") as f:
			exec_hash = hashlib.sha256(f.read()).hexdigest()
		if 'password' in exec_content.lower():
			raise ValueError('Password should not be provided in exec file, especially if content is registered!')
		else:
			self.cursor.execute('''
				INSERT INTO _exec_info(content,content_hash)
				VALUES (%s,%s);
				''',(exec_content,exec_hash))
			self.connection.commit()

	def register_filler_content(self,filler_class,filler_args,status):
		self.cursor.execute('''
				INSERT INTO _fillers_info(class,args,status)
				VALUES (%s,%s,%s);
				''',(filler_class,filler_args,status))
		self.connection.commit()
