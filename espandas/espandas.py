"""Reading and writing pandas DataFrames to ElasticSearch"""
import pandas as pd
import numpy as np
from elasticsearch import Elasticsearch, helpers
from elasticsearch.exceptions import NotFoundError

class Espandas(object):
    """Reading and writing pandas DataFrames to ElasticSearch"""
    def __init__(self, **kwargs):
        """
        Construct an espandas reader/writer
        :params **kwargs: arguments to pass for establishing the connection to ElasticSearch
        """
        self.client = Elasticsearch(**kwargs)
        self.successful_ = None
        self.failed_ = None
        self.uid_name = None

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
                record = self.client.get(index=index, doc_type=doc_type, id=key)
                self.successful_ += 1
                if '_source' in record:
                    records.append(record['_source'])
            except NotFoundError as nfe:
                print('Key not found: %s' % nfe)
                self.failed_ += 1

        # Prepare the records into a single DataFrame
        df = None
        if records:
            df = pd.DataFrame(records).fillna(value=np.nan)
            df = df.reindex_axis(sorted(df.columns), axis=1)
        return df


    def es_write(self, df, index, doc_type, uid_name='indexId'):
        """
        Insert a Pandas DataFrame into ElasticSearch
        :param df: the DataFrame, must contain the column 'indexId' for a unique identifier
        :param index: the ElasticSearch index
        :param doc_type: the ElasticSearch doc_type
        """
        if not isinstance(df, pd.DataFrame):
            raise ValueError('df must be a pandas DataFrame')

        if not self.client.indices.exists(index=index):
            print('index does not exist, creating index')
            self.client.indices.create(index)

        if not uid_name in df.columns:
            raise ValueError('the uid_name must be a column in the DataFrame')

        if len(df[uid_name]) != len(set(df[uid_name])):
            message = 'the values in uid_name must be unique to use as an ElasticSearch _id'
            raise ValueError(message)
        self.uid_name = uid_name

        def generate_dict(df):
            """
            Generator for creating a dict to be inserted into ElasticSearch
            for each row of a pd.DataFrame
            :param df: the input pd.DataFrame to use, must contain an '_id' column
            """
            records = df.to_dict(orient='records')
            for record in records:
                yield record

        # The dataframe should be sorted by column name
        df = df.reindex_axis(sorted(df.columns), axis=1)
        df = df.astype('str')

        data = ({'_index': index,
                 '_type': doc_type,
                 '_id': record[uid_name],
                 '_source': record}
                for record in generate_dict(df))
        helpers.bulk(self.client, data)
