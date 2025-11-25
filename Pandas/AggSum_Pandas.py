import pandas as pd
import pyarrow as pa
import timeit
import gc

# Path to source data
Path = "C:/Users/Bizon/Documents/GitHub/rappwd/"

# Create results table
BenchmarkResults = {
  'Framework': ['pandas']*3, 
  'Method': ['sum aggregation'] * 3,
  'Experiment': [
    '1M 3N 1D 4G',
    '10M 3N 1D 4G',
    '100M 3N 1D 4G'
  ],
  'TimeInSeconds': [-0.1]*3
}
BenchmarkResults = pd.DataFrame(BenchmarkResults)
BenchmarkResults.to_csv(f'{Path}BenchmarkResultsPandas.csv')
del BenchmarkResults
gc.collect()

# Aggregation 1M

# Sum 1 Numeric Variable:


## 1M 3N 1D 4G
data = pd.read_csv(f'{Path}FakeBevData1M.csv', engine = "pyarrow", keep_default_na=False)
BenchmarkResults = pd.read_csv(f'{Path}BenchmarkResultsPandas.csv')
start = timeit.default_timer()
data.groupby(['Date','Customer','Brand','Category','Beverage Flavor'], as_index=False, sort=False, observed=True, dropna=True).agg({'Daily Liters':'sum', 'Daily Units':'sum', 'Daily Margin':'sum'})
end = timeit.default_timer()
BenchmarkResults.at[0, 'TimeInSeconds'] = end - start
BenchmarkResults.to_csv(f'{Path}BenchmarkResultsPandas.csv')
del data, BenchmarkResults, end, start
gc.collect()

###################################################################################################
###################################################################################################
###################################################################################################
###################################################################################################
###################################################################################################

# Aggregation 10M

# Sum 1 Numeric Variable:



## 10M 3N 1D 4G
data = pd.read_csv(f'{Path}FakeBevData10M.csv', engine = "pyarrow", keep_default_na=False)
BenchmarkResults = pd.read_csv(f'{Path}BenchmarkResultsPandas.csv')
start = timeit.default_timer()
data.groupby(['Date','Customer','Brand','Category','Beverage Flavor'], as_index=False, sort=False, observed=True, dropna=True).agg({'Daily Liters':'sum', 'Daily Units':'sum', 'Daily Margin':'sum'})
end = timeit.default_timer()
BenchmarkResults.at[1, 'TimeInSeconds'] = end - start
BenchmarkResults.to_csv(f'{Path}BenchmarkResultsPandas.csv')
del data, BenchmarkResults, end, start
gc.collect()


###################################################################################################
###################################################################################################
###################################################################################################
###################################################################################################
###################################################################################################

# Aggregation 100M

# Sum 1 Numeric Variable:

## 100M 3N 1D 4G
data = pd.read_csv(f'{Path}FakeBevData100M.csv', engine = "pyarrow", keep_default_na=False)
BenchmarkResults = pd.read_csv(f'{Path}BenchmarkResultsPandas.csv')
start = timeit.default_timer()
data.groupby(['Date','Customer','Brand','Category','Beverage Flavor'], as_index=False, sort=False, observed=True, dropna=True).agg({'Daily Liters':'sum', 'Daily Units':'sum', 'Daily Margin':'sum'})
end = timeit.default_timer()
BenchmarkResults.at[2, 'TimeInSeconds'] = end - start
BenchmarkResults.to_csv(f'{Path}BenchmarkResultsPandas.csv')
del data, BenchmarkResults, end, start
gc.collect()

