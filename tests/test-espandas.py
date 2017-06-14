import pandas as pd
import numpy as np
from elasticsearch import Elasticsearch, helpers
from elasticsearch.exceptions import RequestError, ConnectionError

# ES variables
INDEX = 'unit_tests_index'
TYPE = 'foo_bar'

# Example data frame
df = (100 * pd.DataFrame(np.round(np.random.rand(100, 5), 2))).astype(int)
df.columns = ['A', 'B', 'C', 'D', 'E']
df['eventId'] = df.index + 100


def test_es():
	"""
	Before running other tests, ensure connection to ES is established
	"""
	es = Elasticsearch()
	try:
		es.indices.create(INDEX)
		es.indices.delete(INDEX)
	except RequestError:
		print 'Index already exists skipping tests'
		assert True
		sys.exit(0)
	except ConnectionError:
		print  'The ElasticSearch backend is not running. Skipping tests.'
		assert True
		sys.exit(0)
	except Exception as e:
		print 'An unknown error occured connecting to ElasticSearch: %s' % e
		assert True
		sys.exit(0)


def test_es_client():
	"""
	Insert a DataFrame and test that is is correctly extracted
	"""

	es = Elasticsearch()
	es.indices.create(INDEX)

	esp = espandas()
	esp.es_write(df, INDEX, TYPE)
	k = list(df.index)
	res = es_read(k, INDEX, TYPE)
	assert res.shape == df.shape
	assert np.all(res.index == df.index)
	assert np.all(res.columns == np.columns)
	assert np.all(res == df)

	es.indices.delete(INDEX)
