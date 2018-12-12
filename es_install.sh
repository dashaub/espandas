#!/bin/bash

ES_VERSION=2.4.6
wget https://download.elastic.co/elasticsearch/release/org/elasticsearch/distribution/tar/elasticsearch/${ES_VERSION}/elasticsearch-${ES_VERSION}.tar.gz
tar xzvf elasticsearch-${ES_VERSION}.tar.gz

./elasticsearch-${ES_VERSION}/bin/elasticsearch &
