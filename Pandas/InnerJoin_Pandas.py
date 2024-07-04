import pandas as pd
import polars as pl
import statistics as stats
import timeit
import gc

# Path to source data
Path = "C:/Users/Bizon/Documents/GitHub/rappwd/"

# Create results table
BenchmarkResults = {
  'Framework': ['pandas']*10, 
  'Method': ['inner join'] * 10,
  'Experiment': [
    "1M 1N 1D 4G",
    "1M 2N 1D 4G",
    "1M 3N 1D 4G",

    "10M 1N 1D 4G",
    "10M 2N 1D 4G",
    "10M 3N 1D 4G",

    "100M 1N 1D 4G",
    "100M 2N 1D 4G",
    "100M 3N 1D 4G",

    'Total Runtime'],
  'TimeInSeconds': [-0.1]*10
}
BenchmarkResults = pl.DataFrame(BenchmarkResults)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_InnerJoin.csv')
del BenchmarkResults
gc.collect()

# Aggregation 1M

# Sum 1 Numeric Variable:

## 1M 1N 1D 0G
data = pd.read_csv(f'{Path}FakeBevData1M.csv', engine = "pyarrow", keep_default_na=False)
data['Date'] = pd.to_datetime(data['Date'])
data['Customer'] = data['Customer'].astype('category')
data['Brand'] = data['Brand'].astype('category')
data['Category'] = data['Category'].astype('category')
data['Beverage Flavor'] = data['Beverage Flavor'].astype('category')
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_InnerJoin.csv')
cols1 = ['Date','Customer','Brand','Category','Beverage Flavor','Daily Liters']
cols2 = ['Date','Customer','Brand','Category','Beverage Flavor','Daily Units']
temp1 = data[cols1]
temp2 = data[cols2]
rts = [1.1]*3
for i in range(0,3):
  print(i)
  start = timeit.default_timer()
  x = temp1.merge(temp2, on=['Date','Customer','Brand','Category','Beverage Flavor'], how="inner")
  end = timeit.default_timer()
  del x
  rts[i] = end - start
BenchmarkResults[0, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_InnerJoin.csv')
del BenchmarkResults, end, start, rts
gc.collect()

## 1M 1N 1D 1G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_InnerJoin.csv')
cols1 = ['Date','Customer','Brand','Category','Beverage Flavor','Daily Liters']
cols2 = ['Date','Customer','Brand','Category','Beverage Flavor','Daily Units','Daily Margin']
temp1 = data[cols1]
temp2 = data[cols2]
rts = [1.1]*3
for i in range(0,3):
  print(i)
  start = timeit.default_timer()
  x = temp1.merge(temp2, on=['Date','Customer','Brand','Category','Beverage Flavor'], how="inner")
  end = timeit.default_timer()
  del x
  rts[i] = end - start
BenchmarkResults[1, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_InnerJoin.csv')
del BenchmarkResults, end, start, rts
gc.collect()

## 1M 1N 1D 2G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_InnerJoin.csv')
cols1 = ['Date','Customer','Brand','Category','Beverage Flavor','Daily Liters']
cols2 = ['Date','Customer','Brand','Category','Beverage Flavor','Daily Units','Daily Margin','Daily Revenue']
temp1 = data[cols1]
temp2 = data[cols2]
rts = [1.1]*3
for i in range(0,3):
  print(i)
  start = timeit.default_timer()
  x = temp1.merge(temp2, on=['Date','Customer','Brand','Category','Beverage Flavor'], how="inner")
  end = timeit.default_timer()
  del x
  rts[i] = end - start
BenchmarkResults[2, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_InnerJoin.csv')
del BenchmarkResults, end, start, rts
gc.collect()


###################################################################################################
###################################################################################################
###################################################################################################
###################################################################################################
###################################################################################################

# Aggregation 10M

# Sum 1 Numeric Variable:

## 10M 1N 1D 0G
data = pd.read_csv(f'{Path}FakeBevData10M.csv', engine = "pyarrow", keep_default_na=False)
data['Date'] = pd.to_datetime(data['Date'])
data['Customer'] = data['Customer'].astype('category')
data['Brand'] = data['Brand'].astype('category')
data['Category'] = data['Category'].astype('category')
data['Beverage Flavor'] = data['Beverage Flavor'].astype('category')
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_InnerJoin.csv')
cols1 = ['Date','Customer','Brand','Category','Beverage Flavor','Daily Liters']
cols2 = ['Date','Customer','Brand','Category','Beverage Flavor','Daily Units']
temp1 = data[cols1]
temp2 = data[cols2]
rts = [1.1]*3
for i in range(0,3):
  print(i)
  start = timeit.default_timer()
  x = temp1.merge(temp2, on=['Date','Customer','Brand','Category','Beverage Flavor'], how="inner")
  end = timeit.default_timer()
  del x
  rts[i] = end - start
BenchmarkResults[3, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_InnerJoin.csv')
del BenchmarkResults, end, start, rts
gc.collect()

## 1M 1N 1D 1G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_InnerJoin.csv')
cols1 = ['Date','Customer','Brand','Category','Beverage Flavor','Daily Liters']
cols2 = ['Date','Customer','Brand','Category','Beverage Flavor','Daily Units','Daily Margin']
temp1 = data[cols1]
temp2 = data[cols2]
rts = [1.1]*3
for i in range(0,3):
  print(i)
  start = timeit.default_timer()
  x = temp1.merge(temp2, on=['Date','Customer','Brand','Category','Beverage Flavor'], how="inner")
  end = timeit.default_timer()
  del x
  rts[i] = end - start
BenchmarkResults[4, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_InnerJoin.csv')
del BenchmarkResults, end, start, rts
gc.collect()

## 1M 1N 1D 2G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_InnerJoin.csv')
cols1 = ['Date','Customer','Brand','Category','Beverage Flavor','Daily Liters']
cols2 = ['Date','Customer','Brand','Category','Beverage Flavor','Daily Units','Daily Margin','Daily Revenue']
temp1 = data[cols1]
temp2 = data[cols2]
rts = [1.1]*3
for i in range(0,3):
  print(i)
  start = timeit.default_timer()
  x = temp1.merge(temp2, on=['Date','Customer','Brand','Category','Beverage Flavor'], how="inner")
  end = timeit.default_timer()
  del x
  rts[i] = end - start
BenchmarkResults[5, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_InnerJoin.csv')
del BenchmarkResults, end, start, rts
gc.collect()


###################################################################################################
###################################################################################################
###################################################################################################
###################################################################################################
###################################################################################################

# Aggregation 100M

# Sum 1 Numeric Variable:

## 100M 1N 1D 0G
data = pd.read_csv(f'{Path}FakeBevData100M.csv', engine = "pyarrow", keep_default_na=False)
data['Date'] = pd.to_datetime(data['Date'])
data['Customer'] = data['Customer'].astype('category')
data['Brand'] = data['Brand'].astype('category')
data['Category'] = data['Category'].astype('category')
data['Beverage Flavor'] = data['Beverage Flavor'].astype('category')
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_InnerJoin.csv')
cols1 = ['Date','Customer','Brand','Category','Beverage Flavor','Daily Liters']
cols2 = ['Date','Customer','Brand','Category','Beverage Flavor','Daily Units']
temp1 = data[cols1]
temp2 = data[cols2]
rts = [1.1]*3
for i in range(0,3):
  print(i)
  start = timeit.default_timer()
  x = temp1.merge(temp2, on=['Date','Customer','Brand','Category','Beverage Flavor'], how="inner")
  end = timeit.default_timer()
  del x
  rts[i] = end - start
BenchmarkResults[6, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_InnerJoin.csv')
del BenchmarkResults, end, start, rts
gc.collect()

## 1M 1N 1D 1G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_InnerJoin.csv')
cols1 = ['Date','Customer','Brand','Category','Beverage Flavor','Daily Liters']
cols2 = ['Date','Customer','Brand','Category','Beverage Flavor','Daily Units','Daily Margin']
temp1 = data[cols1]
temp2 = data[cols2]
rts = [1.1]*3
for i in range(0,3):
  print(i)
  start = timeit.default_timer()
  x = temp1.merge(temp2, on=['Date','Customer','Brand','Category','Beverage Flavor'], how="inner")
  end = timeit.default_timer()
  del x
  rts[i] = end - start
BenchmarkResults[7, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_InnerJoin.csv')
del BenchmarkResults, end, start, rts
gc.collect()

## 1M 1N 1D 2G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_InnerJoin.csv')
cols1 = ['Date','Customer','Brand','Category','Beverage Flavor','Daily Liters']
cols2 = ['Date','Customer','Brand','Category','Beverage Flavor','Daily Units','Daily Margin','Daily Revenue']
temp1 = data[cols1]
temp2 = data[cols2]
rts = [1.1]*3
for i in range(0,3):
  print(i)
  start = timeit.default_timer()
  x = temp1.merge(temp2, on=['Date','Customer','Brand','Category','Beverage Flavor'], how="inner")
  end = timeit.default_timer()
  del x
  rts[i] = end - start
BenchmarkResults[8, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_InnerJoin.csv')
del BenchmarkResults, end, start, rts
gc.collect()


BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_InnerJoin.csv')
x = BenchmarkResults[0:8]
y = x['TimeInSeconds'].sum()
BenchmarkResults[9, 'TimeInSeconds'] = y
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_InnerJoin.csv')



