import pandas as pd
import pyarrow as pa
import statistics as stats
import timeit
import gc

# Path to source data
Path = "C:/Users/Bizon/Documents/GitHub/rappwd/"

# Create results table
BenchmarkResults = {
  'Framework': ['pandas']*3,
  'Method': ['melt'] * 3,
  'Experiment': [
    '1M 4N 1D 4G',
    '10M 4N 1D 4G',
    '100M 4N 1D 4G'
  ],
  'TimeInSeconds': [-0.1]*3
}
BenchmarkResults = pd.DataFrame(BenchmarkResults)
BenchmarkResults.to_csv(f'{Path}BenchmarkResultsPandas_Melt.csv')
del BenchmarkResults
gc.collect()

# Aggregation 1M

# Melt Numeric Variable:


## 1M 4N 1D 4G
data = pd.read_csv(f'{Path}FakeBevData1M.csv', engine = "pyarrow", keep_default_na=False)
BenchmarkResults = pd.read_csv(f'{Path}BenchmarkResultsPandas_Melt.csv')
rts = [1.1]*3
for i in range(0,3):
  start = timeit.default_timer()
  data.melt(id_vars = ['Date','Customer','Brand','Category','Beverage Flavor'], value_vars = ['Daily Liters','Daily Units','Daily Margin','Daily Revenue'])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults.at[0, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.to_csv(f'{Path}BenchmarkResultsPandas_Melt.csv')
del data, BenchmarkResults, end, start
gc.collect()

###################################################################################################
###################################################################################################
###################################################################################################
###################################################################################################
###################################################################################################

# Aggregation 10M

# Melt Numeric Variable:


## 10M 4N 1D 4G
data = pd.read_csv(f'{Path}FakeBevData10M.csv', engine = "pyarrow", keep_default_na=False)
BenchmarkResults = pd.read_csv(f'{Path}BenchmarkResultsPandas_Melt.csv')
rts = [1.1]*3
for i in range(0,3):
  start = timeit.default_timer()
  data.melt(id_vars = ['Date','Customer','Brand','Category','Beverage Flavor'], value_vars = ['Daily Liters','Daily Units','Daily Margin','Daily Revenue'])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults.at[1, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.to_csv(f'{Path}BenchmarkResultsPandas_Melt.csv')
del data, BenchmarkResults, end, start
gc.collect()


###################################################################################################
###################################################################################################
###################################################################################################
###################################################################################################
###################################################################################################

# Aggregation 100M

# Melt Numeric Variable:



## 100M 4N 1D 4G
data = pd.read_csv(f'{Path}FakeBevData100M.csv', engine = "pyarrow", keep_default_na=False)
BenchmarkResults = pd.read_csv(f'{Path}BenchmarkResultsPandas_Melt.csv')
rts = [1.1]*3
for i in range(0,3):
  start = timeit.default_timer()
  data.melt(id_vars = ['Date','Customer','Brand','Category','Beverage Flavor'], value_vars = ['Daily Liters','Daily Units','Daily Margin','Daily Revenue'])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults.at[2, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.to_csv(f'{Path}BenchmarkResultsPandas_Melt.csv')
del data, BenchmarkResults, end, start
gc.collect()
