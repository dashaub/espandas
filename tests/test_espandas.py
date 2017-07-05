"""Unit tests for the Espandas class"""
import pytest
import pandas as pd
import numpy as np
from espandas import Espandas
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import RequestError, ConnectionError

# ES variables
INDEX = 'unit_tests_index'
TYPE = 'foo_bar'

# Example data frame
df = (100 * pd.DataFrame(np.round(np.random.rand(100, 5), 2))).astype(int)
df.columns = ['A', 'B', 'C', 'D', 'E']
df['indexId'] = df.index + 100
df = df.astype('str')

def test_es():
    """
    Before running other tests, ensure connection to ES is established
    """
    es = Elasticsearch()
    try:
        es.indices.create(INDEX)
        es.indices.delete(INDEX)
        return True
    except RequestError:
        print('Index already exists: skipping tests.')
        return False
    except ConnectionError:
        print('The ElasticSearch backend is not running: skipping tests.')
        return False
    except Exception as e:
        print('An unknown error occured connecting to ElasticSearch: %s' % e)
        return False


def test_es_client():
    """
    Insert a DataFrame and test that is is correctly extracted
    """
    # Only run this test if the index does not already exist
    # and can be created and deleted
    if test_es():
        try:
            print('Connection to ElasticSearch established: testing write and read.')
            es = Elasticsearch()
            es.indices.create(INDEX)

            esp = Espandas()
            esp.es_write(df, INDEX, TYPE)
            k = list(df['indexId'].astype('str'))
            res = esp.es_read(k, INDEX, TYPE)

            # The returned DataFrame should match the original
            assert res.shape == df.shape
            assert np.all(res.index == df.index)
            assert np.all(res.columns == df.columns)
            assert np.all(res == df)
            
            # Bogus keys should not match anything
            res = esp.es_read(['bar'], INDEX, TYPE)
            assert res is None
            num_sample = 3
            present = list(df.sample(num_sample)['indexId'].astype('str'))
            present.append('bar')
            res = esp.es_read(present, INDEX, TYPE)
            assert res.shape[0] == num_sample

            # Test for invalid inputs
            # Input must be a DataFrame
            with pytest.raises(ValueError):
                esp.es_write('foobar', INDEX, TYPE)
            # uid_name must exist in the DataFrame
            with pytest.raises(ValueError):
                esp.es_write(df, INDEX, TYPE, uid_name='foo_index')

            # Values in uid_name must be unique
            df2 = df.copy()
            df2.ix[0, 'indexId'] = df.ix[1, 'indexId']
            with pytest.raises(ValueError):
                esp.es_write(df2, INDEX, TYPE)
        finally:
            # Cleanup
            es.indices.delete(INDEX)
