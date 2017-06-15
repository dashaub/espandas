import pandas as pd
import ujson as json
from elasticsearch import Elasticsearch, helpers
from elasticsearch.exceptions import NotFoundError

class espandas(object):
	def __init__(self, **kwargs):
		"""
		Construct an espandas reader/writer
		:params **kwargs: arguments to pass for establishing the connection to ElasticSearch
		"""
		self.client = Elasticsearch(**kwargs)

	def es_read(self, keys, index, doc_type):
		"""
		Read from an ElasticSearch index and return a DataFrame
		:param keys: a list of keys to extract in elasticsearch
		:param index: the ElasticSearch index to read
		:param doc_type: the ElasticSearch doc_type to read
		"""
		self.successful_ = 0
		self.failed_ = 0

		# Collect records for all of the keys
		records = []
		for key in keys:
			try:
				record = self.client.get(index = index, doc_type = doc_type, id = key)
				self.successful_ += 1
				records.append(pd.DataFrame([record.get('_source')]))
			except NotFoundError as nfe:
				print 'Key not found: %s' % nfe
				self.failed_ += 1

		# Prepare the records into a single DataFrame
		df = pd.concat(records)
		df.index = [i for i in xrange(df.shape[0])]
		df.fillna(value = np.nan, inplace = True)
		df = df.reindex_axis(sorted(df.columns), axis = 1)
		return df


	def es_write(self, df, index, doc_type):
		"""
		Insert a Pandas DataFrame into ElasticSearch
		:param df: the DataFrame, must contain the column 'eventId' for a unique identifier
		:param index: the ElasticSearch index
		:param doc_type: the ElasticSearch doc_type
		"""

		def generate_dict(df):
			"""
			Generator for creating a dict to be inserted into ElasticSearch
			for each row of a pd.DataFrame
			:param df: the input pd.DataFrame to use, must contain an '_id' column
			"""
			records = df.to_json(orient = 'records')
			records = json.loads(records)
			for record in records:
				yield record

		# The dataframe should be sorted by column name
		df = df.reindex_axis(sorted(df.columns), axis = 1)

		data = ({'_index': index, '_type': doc_type , '_id': record['eventId'], '_source': record} for record in generate_dict(df))
		helpers.bulk(self.client, data)
