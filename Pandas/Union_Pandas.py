import pandas as pd
import statistics as stats
import polars as pl
import timeit
import gc

# Path to source data
Path = "C:/Users/Bizon/Documents/GitHub/rappwd/"

# Create results table
BenchmarkResults = {
  'Framework': ['pandas']*46, 
  'Method': ['union'] * 46,
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
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Union.csv')
del BenchmarkResults
gc.collect()

# Aggregation 1M

# Sum 1 Numeric Variable:

## 1M 1N 1D 0G
data = pl.read_csv(f'{Path}FakeBevData1M.csv')
data = data.to_pandas(use_pyarrow_extension_array = True)
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Union.csv')
cols = ['Date', 'Daily Liters']
temp = data[cols]
rts = [1.1]*3
for i in range(0,3):
  print(i)
  start = timeit.default_timer()
  pd.concat([temp, temp])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[0, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Union.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 1N 1D 1G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Union.csv')
cols = ['Date', 'Customer', 'Daily Liters']
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
del BenchmarkResults, end, start
gc.collect()

## 1M 1N 1D 2G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Union.csv')
cols = ['Date', 'Customer', 'Brand', 'Daily Liters']
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
del BenchmarkResults, end, start
gc.collect()

## 1M 1N 1D 3G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Union.csv')
cols = ['Date', 'Customer', 'Brand', 'Category', 'Daily Liters']
temp = data[cols]
rts = [1.1]*3
for i in range(0,3):
  print(i)
  start = timeit.default_timer()
  pd.concat([temp, temp])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[3, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Union.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 1N 1D 4G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Union.csv')
cols = ['Date', 'Customer', 'Brand', 'Category', 'Beverage Flavor', 'Daily Liters']
temp = data[cols]
rts = [1.1]*3
for i in range(0,3):
  print(i)
  start = timeit.default_timer()
  pd.concat([temp, temp])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[4, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Union.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 2N 1D 0G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Union.csv')
cols = ['Date', 'Daily Liters', 'Daily Units']
temp = data[cols]
rts = [1.1]*3
for i in range(0,3):
  print(i)
  start = timeit.default_timer()
  pd.concat([temp, temp])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[5, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Union.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 2N 1D 1G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Union.csv')
cols = ['Date', 'Customer', 'Daily Liters', 'Daily Units']
temp = data[cols]
rts = [1.1]*3
for i in range(0,3):
  print(i)
  start = timeit.default_timer()
  pd.concat([temp, temp])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[6, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Union.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 2N 1D 2G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Union.csv')
cols = ['Date', 'Customer', 'Brand', 'Daily Liters', 'Daily Units']
temp = data[cols]
rts = [1.1]*3
for i in range(0,3):
  print(i)
  start = timeit.default_timer()
  pd.concat([temp, temp])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[7, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Union.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 2N 1D 3G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Union.csv')
cols = ['Date', 'Customer', 'Brand', 'Category', 'Daily Liters', 'Daily Units']
temp = data[cols]
rts = [1.1]*3
for i in range(0,3):
  print(i)
  start = timeit.default_timer()
  pd.concat([temp, temp])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[8, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Union.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 2N 1D 4G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Union.csv')
cols = ['Date', 'Customer', 'Brand', 'Category', 'Beverage Flavor', 'Daily Liters', 'Daily Units']
temp = data[cols]
rts = [1.1]*3
for i in range(0,3):
  print(i)
  start = timeit.default_timer()
  pd.concat([temp, temp])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[9, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Union.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 3N 1D 0G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Union.csv')
cols = ['Date', 'Daily Liters', 'Daily Units', 'Daily Margin']
temp = data[cols]
rts = [1.1]*3
for i in range(0,3):
  print(i)
  start = timeit.default_timer()
  pd.concat([temp, temp])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[10, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Union.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 3N 1D 1G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Union.csv')
cols = ['Date', 'Customer', 'Daily Liters', 'Daily Units', 'Daily Margin']
temp = data[cols]
rts = [1.1]*3
for i in range(0,3):
  print(i)
  start = timeit.default_timer()
  pd.concat([temp, temp])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[11, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Union.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 3N 1D 2G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Union.csv')
cols = ['Date', 'Customer', 'Brand', 'Daily Liters', 'Daily Units', 'Daily Margin']
temp = data[cols]
rts = [1.1]*3
for i in range(0,3):
  print(i)
  start = timeit.default_timer()
  pd.concat([temp, temp])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[12, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Union.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 3N 1D 3G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Union.csv')
cols = ['Date', 'Customer', 'Brand', 'Category', 'Daily Liters', 'Daily Units', 'Daily Margin']
temp = data[cols]
rts = [1.1]*3
for i in range(0,3):
  print(i)
  start = timeit.default_timer()
  pd.concat([temp, temp])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[13, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Union.csv')
del BenchmarkResults, end, start
gc.collect()

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
BenchmarkResults[14, 'TimeInSeconds'] = stats.median(rts)
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
data = pl.read_csv(f'{Path}FakeBevData10M.csv')
data = data.to_pandas(use_pyarrow_extension_array = True)
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Union.csv')
cols = ['Date', 'Daily Liters']
temp = data[cols]
rts = [1.1]*3
for i in range(0,3):
  print(i)
  start = timeit.default_timer()
  pd.concat([temp, temp])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[15, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Union.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 1N 1D 1G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Union.csv')
cols = ['Date', 'Customer', 'Daily Liters']
temp = data[cols]
rts = [1.1]*3
for i in range(0,3):
  print(i)
  start = timeit.default_timer()
  pd.concat([temp, temp])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[16, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Union.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 1N 1D 2G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Union.csv')
cols = ['Date', 'Customer', 'Brand', 'Daily Liters']
temp = data[cols]
rts = [1.1]*3
for i in range(0,3):
  print(i)
  start = timeit.default_timer()
  pd.concat([temp, temp])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[17, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Union.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 1N 1D 3G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Union.csv')
cols = ['Date', 'Customer', 'Brand', 'Category', 'Daily Liters']
temp = data[cols]
rts = [1.1]*3
for i in range(0,3):
  print(i)
  start = timeit.default_timer()
  pd.concat([temp, temp])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[18, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Union.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 1N 1D 4G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Union.csv')
cols = ['Date', 'Customer', 'Brand', 'Category', 'Beverage Flavor', 'Daily Liters']
temp = data[cols]
rts = [1.1]*3
for i in range(0,3):
  print(i)
  start = timeit.default_timer()
  pd.concat([temp, temp])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[19, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Union.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 2N 1D 0G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Union.csv')
cols = ['Date', 'Daily Liters', 'Daily Units']
temp = data[cols]
rts = [1.1]*3
for i in range(0,3):
  print(i)
  start = timeit.default_timer()
  pd.concat([temp, temp])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[20, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Union.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 2N 1D 1G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Union.csv')
cols = ['Date', 'Customer', 'Daily Liters', 'Daily Units']
temp = data[cols]
rts = [1.1]*3
for i in range(0,3):
  print(i)
  start = timeit.default_timer()
  pd.concat([temp, temp])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[21, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Union.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 2N 1D 2G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Union.csv')
cols = ['Date', 'Customer', 'Brand', 'Daily Liters', 'Daily Units']
temp = data[cols]
rts = [1.1]*3
for i in range(0,3):
  print(i)
  start = timeit.default_timer()
  pd.concat([temp, temp])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[22, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Union.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 2N 1D 3G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Union.csv')
cols = ['Date', 'Customer', 'Brand', 'Category', 'Daily Liters', 'Daily Units']
temp = data[cols]
rts = [1.1]*3
for i in range(0,3):
  print(i)
  start = timeit.default_timer()
  pd.concat([temp, temp])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[23, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Union.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 2N 1D 4G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Union.csv')
cols = ['Date', 'Customer', 'Brand', 'Category', 'Beverage Flavor', 'Daily Liters', 'Daily Units']
temp = data[cols]
rts = [1.1]*3
for i in range(0,3):
  print(i)
  start = timeit.default_timer()
  pd.concat([temp, temp])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[24, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Union.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 3N 1D 0G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Union.csv')
cols = ['Date', 'Daily Liters', 'Daily Units', 'Daily Margin']
temp = data[cols]
rts = [1.1]*3
for i in range(0,3):
  print(i)
  start = timeit.default_timer()
  pd.concat([temp, temp])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[25, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Union.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 3N 1D 1G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Union.csv')
cols = ['Date', 'Customer', 'Daily Liters', 'Daily Units', 'Daily Margin']
temp = data[cols]
rts = [1.1]*3
for i in range(0,3):
  print(i)
  start = timeit.default_timer()
  pd.concat([temp, temp])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[26, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Union.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 3N 1D 2G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Union.csv')
cols = ['Date', 'Customer', 'Brand', 'Daily Liters', 'Daily Units', 'Daily Margin']
temp = data[cols]
rts = [1.1]*3
for i in range(0,3):
  print(i)
  start = timeit.default_timer()
  pd.concat([temp, temp])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[27, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Union.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 3N 1D 3G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Union.csv')
cols = ['Date', 'Customer', 'Brand', 'Category', 'Daily Liters', 'Daily Units', 'Daily Margin']
temp = data[cols]
rts = [1.1]*3
for i in range(0,3):
  print(i)
  start = timeit.default_timer()
  pd.concat([temp, temp])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[28, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Union.csv')
del BenchmarkResults, end, start
gc.collect()

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
BenchmarkResults[29, 'TimeInSeconds'] = stats.median(rts)
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
data = pl.read_csv(f'{Path}FakeBevData100M.csv')
data = data.to_pandas(use_pyarrow_extension_array = True)
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Union.csv')
cols = ['Date', 'Daily Liters']
temp = data[cols]
rts = [1.1]*3
for i in range(0,3):
  print(i)
  start = timeit.default_timer()
  pd.concat([temp, temp])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[30, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Union.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 1N 1D 1G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Union.csv')
cols = ['Date', 'Customer', 'Daily Liters']
temp = data[cols]
rts = [1.1]*3
for i in range(0,3):
  print(i)
  start = timeit.default_timer()
  pd.concat([temp, temp])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[31, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Union.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 1N 1D 2G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Union.csv')
cols = ['Date', 'Customer', 'Brand', 'Daily Liters']
temp = data[cols]
rts = [1.1]*3
for i in range(0,3):
  print(i)
  start = timeit.default_timer()
  pd.concat([temp, temp])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[32, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Union.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 1N 1D 3G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Union.csv')
cols = ['Date', 'Customer', 'Brand', 'Category', 'Daily Liters']
temp = data[cols]
rts = [1.1]*3
for i in range(0,3):
  print(i)
  start = timeit.default_timer()
  pd.concat([temp, temp])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[33, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Union.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 1N 1D 4G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Union.csv')
cols = ['Date', 'Customer', 'Brand', 'Category', 'Beverage Flavor', 'Daily Liters']
temp = data[cols]
rts = [1.1]*3
for i in range(0,3):
  print(i)
  start = timeit.default_timer()
  pd.concat([temp, temp])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[34, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Union.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 2N 1D 0G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Union.csv')
cols = ['Date', 'Daily Liters', 'Daily Units']
temp = data[cols]
rts = [1.1]*3
for i in range(0,3):
  print(i)
  start = timeit.default_timer()
  pd.concat([temp, temp])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[35, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Union.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 2N 1D 1G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Union.csv')
cols = ['Date', 'Customer', 'Daily Liters', 'Daily Units']
temp = data[cols]
rts = [1.1]*3
for i in range(0,3):
  print(i)
  start = timeit.default_timer()
  pd.concat([temp, temp])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[36, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Union.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 2N 1D 2G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Union.csv')
cols = ['Date', 'Customer', 'Brand', 'Daily Liters', 'Daily Units']
temp = data[cols]
rts = [1.1]*3
for i in range(0,3):
  print(i)
  start = timeit.default_timer()
  pd.concat([temp, temp])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[37, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Union.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 2N 1D 3G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Union.csv')
cols = ['Date', 'Customer', 'Brand', 'Category', 'Daily Liters', 'Daily Units']
temp = data[cols]
rts = [1.1]*3
for i in range(0,3):
  print(i)
  start = timeit.default_timer()
  pd.concat([temp, temp])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[38, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Union.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 2N 1D 4G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Union.csv')
cols = ['Date', 'Customer', 'Brand', 'Category', 'Beverage Flavor', 'Daily Liters', 'Daily Units']
temp = data[cols]
rts = [1.1]*3
for i in range(0,3):
  print(i)
  start = timeit.default_timer()
  pd.concat([temp, temp])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[39, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Union.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 3N 1D 0G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Union.csv')
cols = ['Date', 'Daily Liters', 'Daily Units', 'Daily Margin']
temp = data[cols]
rts = [1.1]*3
for i in range(0,3):
  print(i)
  start = timeit.default_timer()
  pd.concat([temp, temp])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[40, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Union.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 3N 1D 1G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Union.csv')
cols = ['Date', 'Customer', 'Daily Liters', 'Daily Units', 'Daily Margin']
temp = data[cols]
rts = [1.1]*3
for i in range(0,3):
  print(i)
  start = timeit.default_timer()
  pd.concat([temp, temp])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[41, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Union.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 3N 1D 2G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Union.csv')
cols = ['Date', 'Customer', 'Brand', 'Daily Liters', 'Daily Units', 'Daily Margin']
temp = data[cols]
rts = [1.1]*3
for i in range(0,3):
  print(i)
  start = timeit.default_timer()
  pd.concat([temp, temp])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[42, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Union.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 3N 1D 3G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Union.csv')
cols = ['Date', 'Customer', 'Brand', 'Category', 'Daily Liters', 'Daily Units', 'Daily Margin']
temp = data[cols]
rts = [1.1]*3
for i in range(0,3):
  print(i)
  start = timeit.default_timer()
  pd.concat([temp, temp])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[43, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Union.csv')
del BenchmarkResults, end, start
gc.collect()

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
BenchmarkResults[44, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Union.csv')
del data, BenchmarkResults, end, start
gc.collect()


BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Union.csv')
x = BenchmarkResults[0:44]
y = x['TimeInSeconds'].sum()
BenchmarkResults[45, 'TimeInSeconds'] = y
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Union.csv')

