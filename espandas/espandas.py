import pandas as pd
import ujson as json
from elasticsearch import Elasticsearch, helpers

class espandas(object):
	def __init__(self, **kwargs):
		self.client = Elasticsearch(**kwargs)

	def es_read(keys):
	pass

	def es_write(df, index, doc_type):
	"""
	Insert a Pandas DataFrame into ElasticSearch
	:param df: the DataFrame
	:param index: the ElasticSearch index
	:param _type: the ElasticSearch type
	:param es: the connection to ElasticSearch
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

	data = ({'_index': index, '_type': doc_type , 'eventId': record['eventId'], '_source': record} for record in generate_dict(df))
	helpers.bulk(self.client, data)
