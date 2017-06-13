import pandas as pd
import ujson as json
from elasticsearch import Elasticsearch, helpers

es = Elasticsearch()

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

def df_to_es(df, index, _type, es):
	"""
	Insert a Pandas DataFrame into ElasticSearch
	:param df: the DataFrame
	:param index: the ElasticSearch index
	:param _type: the ElasticSearch type
	:param es: the connection to ElasticSearch
	"""
	data = ({'_index': index, '_type': _type , '_id': record['_id']} for record in generate_dict(df))
	helpers.bulk(es, data)
