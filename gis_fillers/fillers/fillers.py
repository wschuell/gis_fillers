import os
import requests
import zipfile
import pandas as pd
import logging
import csv
from psycopg2 import extras
import shapefile
import json
import subprocess

logger = logging.getLogger('fillers')
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)
logger.setLevel(logging.INFO)

class Filler(object):
	"""
	The Filler class and its children provide methods to fill the database, potentially from different sources.
	They can be linked to the database either by mentioning the database at creation of the instance (see __init__),
	or by calling the 'add_filler' method of the database. Caution, the order may be important, e.g. when foreign keys are involved.

	For writing children, just change the 'apply' method, and do not forget the commit at the end.
	This class is just an abstract 'mother' class
	"""

	def __init__(self,db=None,name=None,data_folder=None,unique_name=False,encoding='utf-8',delimiter=','):#,file_info=None):
		if name is None:
			name = self.__class__.__name__
		self.name = name
		if db is not None:
			db.add_filler(self)
		self.data_folder = data_folder
		self.logger = logging.getLogger('fillers.'+self.__class__.__name__)
		self.logger.addHandler(ch)
		self.logger.setLevel(logging.INFO)
		self.done = False
		self.unique_name = unique_name
		self.encoding = encoding
		self.delimiter = delimiter
		# if file_info is not None:
		# 	self.set_file_info(file_info)
		self.relevant_attributes = ['data_folder']

	def get_relevant_attr_string(self):
		return '\n'.join(['{}:{}'.format(r,getattr(self,r)) for r in self.relevant_attributes])

	# def set_file_info(self,file_info): # deprecated, files are managed at filler level only
	# 	"""set_file_info should add the filename in self.db.fillers_shareddata['files'][filecode] = filename
	# 	while checking that the filecode is not present already in the relevant dict"""
	# 	raise NotImplementedError

	def apply(self):
		# filling script here
		self.db.connection.commit()

	def prepare(self,**kwargs):
		'''
		Method called to potentially do necessary preprocessings (downloading files, uncompressing, converting, ...)
		'''

		if self.data_folder is None:
			self.data_folder = self.db.data_folder
		data_folder = self.data_folder

		#create folder if needed
		if not os.path.exists(data_folder):
			os.makedirs(data_folder)
		pass


	def after_insert(self):
		pass


	def download(self,url,destination,wget=False):
		self.logger.info('Downloading {}'.format(url))
		if not wget:
			r = requests.get(url, allow_redirects=True)
			r.raise_for_status()
			with open(destination, 'wb') as f:
				f.write(r.content)
		else:
			try:
				subprocess.check_call('wget -O {} {}'.format(destination,url).split(' '))
			except:
				subprocess.check_call('curl -o {} -L {}'.format(destination,url).split(' '))

	def unzip(self,orig_file,destination,clean_zip=False):
		self.logger.info('Unzipping {}'.format(orig_file))
		with zipfile.ZipFile(orig_file, 'r') as zip_ref:
			zip_ref.extractall(destination)
		if clean_zip:
			os.remove(orig_file)

	def get_spreadsheet_engine(self,orig_file):
		file_ext = orig_file.split('.')[-1]
		if file_ext == 'xlsx':
			engine = 'openpyxl'
		elif file_ext == 'ods':
			engine = 'odf'
		else:
			raise ValueError(f'File extension not recognized for spreadsheet: {file_ext}')

	def convert_spreadsheet(self,orig_file,destination,clean_orig=False,engine=None):
		self.logger.info('Converting {} to CSV'.format(orig_file))
		if engine is None:
			engine = self.get_spreadsheet_engine(orig_file=orig_file)
		data = pd.read_excel(orig_file, index_col=None, engine=engine)
		data.to_csv(destination,index=False,header=None ,encoding='utf-8')
		if clean_orig:
			os.remove(orig_file)

	def convert_spreadhseet_sheets(self,orig_file,destination,sheet_names = None, clean_orig=False,engine=None):
		self.logger.info('Converting {} sheets to CSVs'.format(orig_file))
		if engine is None:
			engine = self.get_spreadsheet_engine(orig_file=orig_file)
		data = pd.read_excel(orig_file, index_col=None, engine=engine, sheet_name = sheet_names)

		names = list(data.keys())
		if not os.path.exists(destination):
			os.makedirs(destination)
		for name in names:
			data[name].to_csv(os.path.join(destination,'{}.csv'.format(name)),index=False ,encoding='utf-8')

		if clean_orig:
			os.remove(orig_file)

	def extract_spreadsheet_sheets(self,orig_file,sheet_names=None,engine=None):
		self.logger.info('Extracting {} sheets'.format(orig_file))
		
		if engine is None:
			engine = self.get_spreadsheet_engine(orig_file=orig_file)
		return pd.read_excel(orig_file, index_col=None, engine=engine, sheet_name = sheet_names)


	def record_file(self,**kwargs):
		'''
		Wrapper to solve data_folder mismatch with DB
		'''
		self.db.record_file(folder=self.data_folder,**kwargs)


