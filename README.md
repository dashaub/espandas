# espandas
[![PyPI version](https://badge.fury.io/py/espandas.svg)](https://badge.fury.io/py/espandas)
[![Coverage Status](https://coveralls.io/repos/github/dashaub/espandas/badge.svg?branch=master)](https://coveralls.io/github/dashaub/espandas?branch=master)

Inserting and querying Pandas DataFrames in ElasticSearch

## Usage
First, ensure ElasticSearch is running on localhost.
```
import pandas as pd
import numpy as np
import espandas

# Example data frame
df = (100 * pd.DataFrame(np.round(np.random.rand(100, 5), 2))).astype(int)
df.columns = ['A', 'B', 'C', 'D', 'E']
df['eventId'] = df.index + 100

# Create a client and write the DataFrame. If necessary, connection
# information to the ES cluster can be passed in the espandas constructor
# as keyword arguments.
esp = espandas()
esp.es_write(df, INDEX, TYPE)


# Query for the first ten rows and see that they match the original
k = list(df.index)[0:10]
res = es_read(k, INDEX, TYPE)
res == df.iloc[0:10]
```

## License
(c) 2017 David Shaub

This package is free software released under the [GPL-3](http://www.gnu.org/licenses/gpl-3.0.en.html) license.
