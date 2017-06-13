import pandas as pd
import numpy as np

df = pd.DataFrame(np.random.rand(100, 5))
df.columns = ['featA', 'featB', 'featC', 'featD', 'featE']

