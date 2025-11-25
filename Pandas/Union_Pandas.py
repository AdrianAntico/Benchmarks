import pandas as pd
import statistics as stats
import polars as pl
import timeit
import gc

# Path to source data
Path = "C:/Users/Bizon/Documents/GitHub/rappwd/"

# Create results table
BenchmarkResults = {
  'Framework': ['pandas']*3, 
  'Method': ['union'] * 3,
  'Experiment': [
    '1M 3N 1D 4G',
    '10M 3N 1D 4G',
    '100M 3N 1D 4G'
  ],
  'TimeInSeconds': [-0.1]*3
}
BenchmarkResults = pl.DataFrame(BenchmarkResults)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Union.csv')
del BenchmarkResults
gc.collect()

# Aggregation 1M

# Sum 1 Numeric Variable:
data = pl.read_csv(f'{Path}FakeBevData1M.csv', rechunk=True)
data = data.to_pandas(use_pyarrow_extension_array = True)


## 1M 3N 1D 4G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Union.csv')
cols = ['Date', 'Customer', 'Brand', 'Category', 'Beverage Flavor', 'Daily Liters', 'Daily Units', 'Daily Margin']
temp = data[cols]
rts = [1.1]*3
for i in range(0,3):  # i = 1
  print(i)
  start = timeit.default_timer()
  x=pd.concat([temp, temp])
  x.shape
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[0, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Union.csv')
del data, BenchmarkResults, end, start
gc.collect()

###################################################################################################
###################################################################################################
###################################################################################################
###################################################################################################
###################################################################################################

# Aggregation 10M

# Sum 1 Numeric Variable:

## 10M 1N 1D 0G
data = pl.read_csv(f'{Path}FakeBevData10M.csv', rechunk=True)
data = data.to_pandas(use_pyarrow_extension_array = True)


## 1M 3N 1D 4G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Union.csv')
cols = ['Date', 'Customer', 'Brand', 'Category', 'Beverage Flavor', 'Daily Liters', 'Daily Units', 'Daily Margin']
temp = data[cols]
rts = [1.1]*3
for i in range(0,3):
  print(i)
  start = timeit.default_timer()
  pd.concat([temp, temp])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[1, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Union.csv')
del data, BenchmarkResults, end, start
gc.collect()


###################################################################################################
###################################################################################################
###################################################################################################
###################################################################################################
###################################################################################################

# Aggregation 100M

# Sum 1 Numeric Variable:

## 100M 1N 1D 0G
data = pl.read_csv(f'{Path}FakeBevData100M.csv', rechunk=True)
data = data.to_pandas(use_pyarrow_extension_array = True)


## 1M 3N 1D 4G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Union.csv')
cols = ['Date', 'Customer', 'Brand', 'Category', 'Beverage Flavor', 'Daily Liters', 'Daily Units', 'Daily Margin']
temp = data[cols]
rts = [1.1]*3
for i in range(0,3):
  print(i)
  start = timeit.default_timer()
  pd.concat([temp, temp])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[2, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Union.csv')
del data, BenchmarkResults, end, start
gc.collect()
