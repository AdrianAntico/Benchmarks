import pandas as pd
import polars as pl
import timeit
import gc

# Path to source data
Path = "C:/Users/Bizon/Documents/GitHub/rappwd/"

# Create results table
BenchmarkResults = {
  'Framework': ['pandas']*46, 
  'Method': ['lags'] * 46,
  'Experiment': [
    '1M 1N 1D 0G',
    '1M 1N 1D 1G',
    '1M 1N 1D 2G',
    '1M 1N 1D 3G',
    '1M 1N 1D 4G',
    '1M 2N 1D 0G',
    '1M 2N 1D 1G',
    '1M 2N 1D 2G',
    '1M 2N 1D 3G',
    '1M 2N 1D 4G',
    '1M 3N 1D 0G',
    '1M 3N 1D 1G',
    '1M 3N 1D 2G',
    '1M 3N 1D 3G',
    '1M 3N 1D 4G',
    
    '10M 1N 1D 0G',
    '10M 1N 1D 1G',
    '10M 1N 1D 2G',
    '10M 1N 1D 3G',
    '10M 1N 1D 4G',
    '10M 2N 1D 0G',
    '10M 2N 1D 1G',
    '10M 2N 1D 2G',
    '10M 2N 1D 3G',
    '10M 2N 1D 4G',
    '10M 3N 1D 0G',
    '10M 3N 1D 1G',
    '10M 3N 1D 2G',
    '10M 3N 1D 3G',
    '10M 3N 1D 4G',
    
    '100M 1N 1D 0G',
    '100M 1N 1D 1G',
    '100M 1N 1D 2G',
    '100M 1N 1D 3G',
    '100M 1N 1D 4G',
    '100M 2N 1D 0G',
    '100M 2N 1D 1G',
    '100M 2N 1D 2G',
    '100M 2N 1D 3G',
    '100M 2N 1D 4G',
    '100M 3N 1D 0G',
    '100M 3N 1D 1G',
    '100M 3N 1D 2G',
    '100M 3N 1D 3G',
    '100M 3N 1D 4G',

    'Total Runtime'],
  'TimeInSeconds': [-0.1]*46
}
BenchmarkResults = pl.DataFrame(BenchmarkResults)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Lags.csv')
del BenchmarkResults
gc.collect()

# Aggregation 1M

# Sum 1 Numeric Variable:

## 1M 1N 1D 0G
data = pl.read_csv(f'{Path}FakeBevData1M.csv', engine = "pyarrow", keep_default_na=False)
data = data.to_pandas(use_pyarrow_extension_array = True)
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Lags.csv')
start = timeit.default_timer()
lags = list(range(1,6))
columns = ['Daily Liters']
names = [f"Lag {col} {lag}" for col in columns for lag in lags]
data[names] = data[columns].shift(lags)
end = timeit.default_timer()
BenchmarkResults[0, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Lags.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 1N 1D 1G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Lags.csv')
start = timeit.default_timer()
lags = list(range(1,6))
columns = ['Daily Liters']
names = [f"Lag {col} {lag}" for col in columns for lag in lags]
data[names] = data.groupby(['Customer'])[columns].shift(lags)
end = timeit.default_timer()
BenchmarkResults[1, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Lags.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 1N 1D 2G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Lags.csv')
start = timeit.default_timer()
lags = list(range(1,6))
columns = ['Daily Liters']
names = [f"Lag {col} {lag}" for col in columns for lag in lags]
data[names] = data.groupby(['Customer','Brand'])[columns].shift(lags)
end = timeit.default_timer()
BenchmarkResults[2, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Lags.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 1N 1D 3G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Lags.csv')
start = timeit.default_timer()
lags = list(range(1,6))
columns = ['Daily Liters']
names = [f"Lag {col} {lag}" for col in columns for lag in lags]
data[names] = data.groupby(['Customer','Brand','Category'])[columns].shift(lags)
end = timeit.default_timer()
BenchmarkResults[3, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Lags.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 1N 1D 4G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Lags.csv')
start = timeit.default_timer()
lags = list(range(1,6))
columns = ['Daily Liters']
names = [f"Lag {col} {lag}" for col in columns for lag in lags]
data[names] = data.groupby(['Customer','Brand','Category','Beverage Flavor'])[columns].shift(lags)
end = timeit.default_timer()
BenchmarkResults[4, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Lags.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 2N 1D 0G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Lags.csv')
start = timeit.default_timer()
lags = list(range(1,6))
columns = ['Daily Liters', 'Daily Units']
names = [f"Lag {col} {lag}" for col in columns for lag in lags]
data[names] = data[columns].shift(lags)
end = timeit.default_timer()
BenchmarkResults[5, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Lags.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 2N 1D 1G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Lags.csv')
start = timeit.default_timer()
lags = list(range(1,6))
columns = ['Daily Liters','Daily Units']
names = [f"Lag {col} {lag}" for col in columns for lag in lags]
data[names] = data.groupby(['Customer'])[columns].shift(lags)
end = timeit.default_timer()
BenchmarkResults[6, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Lags.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 2N 1D 2G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Lags.csv')
start = timeit.default_timer()
lags = list(range(1,6))
columns = ['Daily Liters','Daily Units']
names = [f"Lag {col} {lag}" for col in columns for lag in lags]
data[names] = data.groupby(['Customer','Brand'])[columns].shift(lags)
end = timeit.default_timer()
BenchmarkResults[7, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Lags.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 2N 1D 3G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Lags.csv')
start = timeit.default_timer()
lags = list(range(1,6))
columns = ['Daily Liters','Daily Units']
names = [f"Lag {col} {lag}" for col in columns for lag in lags]
data[names] = data.groupby(['Customer','Brand','Category'])[columns].shift(lags)
end = timeit.default_timer()
BenchmarkResults[8, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Lags.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 2N 1D 4G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Lags.csv')
start = timeit.default_timer()
lags = list(range(1,6))
columns = ['Daily Liters','Daily Units']
names = [f"Lag {col} {lag}" for col in columns for lag in lags]
data[names] = data.groupby(['Customer','Brand','Category','Beverage Flavor'])[columns].shift(lags)
end = timeit.default_timer()
BenchmarkResults[9, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Lags.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 3N 1D 0G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Lags.csv')
start = timeit.default_timer()
lags = list(range(1,6))
columns = ['Daily Liters', 'Daily Units', 'Daily Margin']
names = [f"Lag {col} {lag}" for col in columns for lag in lags]
data[names] = data[columns].shift(lags)
end = timeit.default_timer()
BenchmarkResults[10, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Lags.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 3N 1D 1G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Lags.csv')
start = timeit.default_timer()
lags = list(range(1,6))
columns = ['Daily Liters','Daily Units','Daily Margin']
names = [f"Lag {col} {lag}" for col in columns for lag in lags]
data[names] = data.groupby(['Customer'])[columns].shift(lags)
end = timeit.default_timer()
BenchmarkResults[11, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Lags.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 3N 1D 2G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Lags.csv')
start = timeit.default_timer()
lags = list(range(1,6))
columns = ['Daily Liters','Daily Units','Daily Margin']
names = [f"Lag {col} {lag}" for col in columns for lag in lags]
data[names] = data.groupby(['Customer','Brand'])[columns].shift(lags)
end = timeit.default_timer()
BenchmarkResults[12, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Lags.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 3N 1D 3G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Lags.csv')
start = timeit.default_timer()
lags = list(range(1,6))
columns = ['Daily Liters','Daily Units','Daily Margin']
names = [f"Lag {col} {lag}" for col in columns for lag in lags]
data[names] = data.groupby(['Customer','Brand','Category'])[columns].shift(lags)
end = timeit.default_timer()
BenchmarkResults[13, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Lags.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 3N 1D 4G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Lags.csv')
start = timeit.default_timer()
lags = list(range(1,6))
columns = ['Daily Liters','Daily Units','Daily Margin']
names = [f"Lag {col} {lag}" for col in columns for lag in lags]
data[names] = data.groupby(['Customer','Brand','Category','Beverage Flavor'])[columns].shift(lags)
end = timeit.default_timer()
BenchmarkResults[14, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Lags.csv')
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
data = pl.read_csv(f'{Path}FakeBevData10M.csv', engine = "pyarrow", keep_default_na=False)
data = data.to_pandas(use_pyarrow_extension_array = True)
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Lags.csv')
start = timeit.default_timer()
lags = list(range(1,6))
columns = ['Daily Liters']
names = [f"Lag {col} {lag}" for col in columns for lag in lags]
data[names] = data[columns].shift(lags)
end = timeit.default_timer()
BenchmarkResults[15, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Lags.csv')
del BenchmarkResults, end, start
gc.collect()

## 10M 1N 1D 1G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Lags.csv')
start = timeit.default_timer()
lags = list(range(1,6))
columns = ['Daily Liters']
names = [f"Lag {col} {lag}" for col in columns for lag in lags]
data[names] = data.groupby(['Customer'])[columns].shift(lags)
end = timeit.default_timer()
BenchmarkResults[16, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Lags.csv')
del BenchmarkResults, end, start
gc.collect()

## 10M 1N 1D 2G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Lags.csv')
start = timeit.default_timer()
lags = list(range(1,6))
columns = ['Daily Liters']
names = [f"Lag {col} {lag}" for col in columns for lag in lags]
data[names] = data.groupby(['Customer','Brand'])[columns].shift(lags)
end = timeit.default_timer()
BenchmarkResults[17, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Lags.csv')
del BenchmarkResults, end, start
gc.collect()

## 10M 1N 1D 3G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Lags.csv')
start = timeit.default_timer()
lags = list(range(1,6))
columns = ['Daily Liters']
names = [f"Lag {col} {lag}" for col in columns for lag in lags]
data[names] = data.groupby(['Customer','Brand','Category'])[columns].shift(lags)
end = timeit.default_timer()
BenchmarkResults[18, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Lags.csv')
del BenchmarkResults, end, start
gc.collect()

## 10M 1N 1D 4G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Lags.csv')
start = timeit.default_timer()
lags = list(range(1,6))
columns = ['Daily Liters']
names = [f"Lag {col} {lag}" for col in columns for lag in lags]
data[names] = data.groupby(['Customer','Brand','Category','Beverage Flavor'])[columns].shift(lags)
end = timeit.default_timer()
BenchmarkResults[19, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Lags.csv')
del BenchmarkResults, end, start
gc.collect()

## 10M 2N 1D 0G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Lags.csv')
start = timeit.default_timer()
lags = list(range(1,6))
columns = ['Daily Liters', 'Daily Units']
names = [f"Lag {col} {lag}" for col in columns for lag in lags]
data[names] = data[columns].shift(lags)
end = timeit.default_timer()
BenchmarkResults[20, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Lags.csv')
del BenchmarkResults, end, start
gc.collect()

## 10M 2N 1D 1G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Lags.csv')
start = timeit.default_timer()
lags = list(range(1,6))
columns = ['Daily Liters','Daily Units']
names = [f"Lag {col} {lag}" for col in columns for lag in lags]
data[names] = data.groupby(['Customer'])[columns].shift(lags)
end = timeit.default_timer()
BenchmarkResults[21, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Lags.csv')
del BenchmarkResults, end, start
gc.collect()

## 10M 2N 1D 2G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Lags.csv')
start = timeit.default_timer()
lags = list(range(1,6))
columns = ['Daily Liters','Daily Units']
names = [f"Lag {col} {lag}" for col in columns for lag in lags]
data[names] = data.groupby(['Customer','Brand'])[columns].shift(lags)
end = timeit.default_timer()
BenchmarkResults[22, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Lags.csv')
del BenchmarkResults, end, start
gc.collect()

## 10M 2N 1D 3G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Lags.csv')
start = timeit.default_timer()
lags = list(range(1,6))
columns = ['Daily Liters','Daily Units']
names = [f"Lag {col} {lag}" for col in columns for lag in lags]
data[names] = data.groupby(['Customer','Brand','Category'])[columns].shift(lags)
end = timeit.default_timer()
BenchmarkResults[23, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Lags.csv')
del BenchmarkResults, end, start
gc.collect()

## 10M 2N 1D 4G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Lags.csv')
start = timeit.default_timer()
lags = list(range(1,6))
columns = ['Daily Liters','Daily Units']
names = [f"Lag {col} {lag}" for col in columns for lag in lags]
data[names] = data.groupby(['Customer','Brand','Category','Beverage Flavor'])[columns].shift(lags)
end = timeit.default_timer()
BenchmarkResults[24, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Lags.csv')
del BenchmarkResults, end, start
gc.collect()

## 10M 3N 1D 0G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Lags.csv')
start = timeit.default_timer()
lags = list(range(1,6))
columns = ['Daily Liters', 'Daily Units', 'Daily Margin']
names = [f"Lag {col} {lag}" for col in columns for lag in lags]
data[names] = data[columns].shift(lags)
end = timeit.default_timer()
BenchmarkResults[25, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Lags.csv')
del BenchmarkResults, end, start
gc.collect()

## 10M 3N 1D 1G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Lags.csv')
start = timeit.default_timer()
lags = list(range(1,6))
columns = ['Daily Liters','Daily Units','Daily Margin']
names = [f"Lag {col} {lag}" for col in columns for lag in lags]
data[names] = data.groupby(['Customer'])[columns].shift(lags)
end = timeit.default_timer()
BenchmarkResults[26, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Lags.csv')
del BenchmarkResults, end, start
gc.collect()

## 10M 3N 1D 2G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Lags.csv')
start = timeit.default_timer()
lags = list(range(1,6))
columns = ['Daily Liters','Daily Units','Daily Margin']
names = [f"Lag {col} {lag}" for col in columns for lag in lags]
data[names] = data.groupby(['Customer','Brand'])[columns].shift(lags)
end = timeit.default_timer()
BenchmarkResults[27, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Lags.csv')
del BenchmarkResults, end, start
gc.collect()

## 10M 3N 1D 3G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Lags.csv')
start = timeit.default_timer()
lags = list(range(1,6))
columns = ['Daily Liters','Daily Units','Daily Margin']
names = [f"Lag {col} {lag}" for col in columns for lag in lags]
data[names] = data.groupby(['Customer','Brand','Category'])[columns].shift(lags)
end = timeit.default_timer()
BenchmarkResults[28, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Lags.csv')
del BenchmarkResults, end, start
gc.collect()

## 10M 3N 1D 4G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Lags.csv')
start = timeit.default_timer()
lags = list(range(1,6))
columns = ['Daily Liters','Daily Units','Daily Margin']
names = [f"Lag {col} {lag}" for col in columns for lag in lags]
data[names] = data.groupby(['Customer','Brand','Category','Beverage Flavor'])[columns].shift(lags)
end = timeit.default_timer()
BenchmarkResults[29, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Lags.csv')
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
data = pl.read_csv(f'{Path}FakeBevData100M.csv', engine = "pyarrow", keep_default_na=False)
data = data.to_pandas(use_pyarrow_extension_array = True)
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Lags.csv')
start = timeit.default_timer()
lags = list(range(1,6))
columns = ['Daily Liters']
names = [f"Lag {col} {lag}" for col in columns for lag in lags]
data[names] = data[columns].shift(lags)
end = timeit.default_timer()
BenchmarkResults[30, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Lags.csv')
del BenchmarkResults, end, start
gc.collect()

## 100M 1N 1D 1G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Lags.csv')
start = timeit.default_timer()
lags = list(range(1,6))
columns = ['Daily Liters']
names = [f"Lag {col} {lag}" for col in columns for lag in lags]
data[names] = data.groupby(['Customer'])[columns].shift(lags)
end = timeit.default_timer()
BenchmarkResults[31, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Lags.csv')
del BenchmarkResults, end, start
gc.collect()

## 100M 1N 1D 2G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Lags.csv')
start = timeit.default_timer()
lags = list(range(1,6))
columns = ['Daily Liters']
names = [f"Lag {col} {lag}" for col in columns for lag in lags]
data[names] = data.groupby(['Customer','Brand'])[columns].shift(lags)
end = timeit.default_timer()
BenchmarkResults[32, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Lags.csv')
del BenchmarkResults, end, start
gc.collect()

## 100M 1N 1D 3G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Lags.csv')
start = timeit.default_timer()
lags = list(range(1,6))
columns = ['Daily Liters']
names = [f"Lag {col} {lag}" for col in columns for lag in lags]
data[names] = data.groupby(['Customer','Brand','Category'])[columns].shift(lags)
end = timeit.default_timer()
BenchmarkResults[33, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Lags.csv')
del BenchmarkResults, end, start
gc.collect()

## 100M 1N 1D 4G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Lags.csv')
start = timeit.default_timer()
lags = list(range(1,6))
columns = ['Daily Liters']
names = [f"Lag {col} {lag}" for col in columns for lag in lags]
data[names] = data.groupby(['Customer','Brand','Category','Beverage Flavor'])[columns].shift(lags)
end = timeit.default_timer()
BenchmarkResults[34, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Lags.csv')
del BenchmarkResults, end, start
gc.collect()

## 100M 2N 1D 0G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Lags.csv')
start = timeit.default_timer()
lags = list(range(1,6))
columns = ['Daily Liters', 'Daily Units']
names = [f"Lag {col} {lag}" for col in columns for lag in lags]
data[names] = data[columns].shift(lags)
end = timeit.default_timer()
BenchmarkResults[35, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Lags.csv')
del BenchmarkResults, end, start
gc.collect()

## 100M 2N 1D 1G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Lags.csv')
start = timeit.default_timer()
lags = list(range(1,6))
columns = ['Daily Liters','Daily Units']
names = [f"Lag {col} {lag}" for col in columns for lag in lags]
data[names] = data.groupby(['Customer'])[columns].shift(lags)
end = timeit.default_timer()
BenchmarkResults[36, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Lags.csv')
del BenchmarkResults, end, start
gc.collect()

## 100M 2N 1D 2G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Lags.csv')
start = timeit.default_timer()
lags = list(range(1,6))
columns = ['Daily Liters','Daily Units']
names = [f"Lag {col} {lag}" for col in columns for lag in lags]
data[names] = data.groupby(['Customer','Brand'])[columns].shift(lags)
end = timeit.default_timer()
BenchmarkResults[37, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Lags.csv')
del BenchmarkResults, end, start
gc.collect()

## 100M 2N 1D 3G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Lags.csv')
start = timeit.default_timer()
lags = list(range(1,6))
columns = ['Daily Liters','Daily Units']
names = [f"Lag {col} {lag}" for col in columns for lag in lags]
data[names] = data.groupby(['Customer','Brand','Category'])[columns].shift(lags)
end = timeit.default_timer()
BenchmarkResults[38, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Lags.csv')
del BenchmarkResults, end, start
gc.collect()

## 100M 2N 1D 4G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Lags.csv')
start = timeit.default_timer()
lags = list(range(1,6))
columns = ['Daily Liters','Daily Units']
names = [f"Lag {col} {lag}" for col in columns for lag in lags]
data[names] = data.groupby(['Customer','Brand','Category','Beverage Flavor'])[columns].shift(lags)
end = timeit.default_timer()
BenchmarkResults[39, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Lags.csv')
del BenchmarkResults, end, start
gc.collect()

## 100M 3N 1D 0G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Lags.csv')
start = timeit.default_timer()
lags = list(range(1,6))
columns = ['Daily Liters', 'Daily Units', 'Daily Margin']
names = [f"Lag {col} {lag}" for col in columns for lag in lags]
data[names] = data[columns].shift(lags)
end = timeit.default_timer()
BenchmarkResults[40, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Lags.csv')
del BenchmarkResults, end, start
gc.collect()

## 100M 3N 1D 1G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Lags.csv')
start = timeit.default_timer()
lags = list(range(1,6))
columns = ['Daily Liters','Daily Units','Daily Margin']
names = [f"Lag {col} {lag}" for col in columns for lag in lags]
data[names] = data.groupby(['Customer'])[columns].shift(lags)
end = timeit.default_timer()
BenchmarkResults[41, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Lags.csv')
del BenchmarkResults, end, start
gc.collect()

## 100M 3N 1D 2G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Lags.csv')
start = timeit.default_timer()
lags = list(range(1,6))
columns = ['Daily Liters','Daily Units','Daily Margin']
names = [f"Lag {col} {lag}" for col in columns for lag in lags]
data[names] = data.groupby(['Customer','Brand'])[columns].shift(lags)
end = timeit.default_timer()
BenchmarkResults[42, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Lags.csv')
del BenchmarkResults, end, start
gc.collect()

## 100M 3N 1D 3G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Lags.csv')
start = timeit.default_timer()
lags = list(range(1,6))
columns = ['Daily Liters','Daily Units','Daily Margin']
names = [f"Lag {col} {lag}" for col in columns for lag in lags]
data[names] = data.groupby(['Customer','Brand','Category'])[columns].shift(lags)
end = timeit.default_timer()
BenchmarkResults[43, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Lags.csv')
del BenchmarkResults, end, start
gc.collect()

## 100M 3N 1D 4G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Lags.csv')
start = timeit.default_timer()
lags = list(range(1,6))
columns = ['Daily Liters','Daily Units','Daily Margin']
names = [f"Lag {col} {lag}" for col in columns for lag in lags]
data[names] = data.groupby(['Customer','Brand','Category','Beverage Flavor'])[columns].shift(lags)
end = timeit.default_timer()
BenchmarkResults[44, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Lags.csv')
del data, BenchmarkResults, end, start
gc.collect()

BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Lags.csv')
x = BenchmarkResults[0:44]
y = x['TimeInSeconds'].sum()
BenchmarkResults[45, 'TimeInSeconds'] = y
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Lags.csv')

