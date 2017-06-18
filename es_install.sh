#!/bin/bash

ES_VERSION=2.4.5
wget https://download.elastic.co/elasticsearch/release/org/elasticsearch/distribution/tar/elasticsearch/2.4.5/elasticsearch-${ES_VERSION}.tar.gz
tar xzvf elasticsearch-${ES_VERSION}.tar.gz

./elasticsearch-${ES_VERSION}/bin/elasticsearch &
