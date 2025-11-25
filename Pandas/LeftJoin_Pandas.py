import pandas as pd
import polars as pl
import statistics as stats
import timeit
import gc

# Path to source data
Path = "C:/Users/Bizon/Documents/GitHub/rappwd/"

# Create results table
BenchmarkResults = {
  'Framework': ['pandas']*3, 
  'Method': ['left join'] * 3,
  'Experiment': [
    "1M 3N 1D 4G",
    "10M 3N 1D 4G",
    "100M 3N 1D 4G"
  ],
  'TimeInSeconds': [-0.1]*3
}
BenchmarkResults = pl.DataFrame(BenchmarkResults)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_LeftJoin.csv')
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


## 1M 1N 1D 2G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_LeftJoin.csv')
cols1 = ['Date','Customer','Brand','Category','Beverage Flavor','Daily Liters']
cols2 = ['Date','Customer','Brand','Category','Beverage Flavor','Daily Units','Daily Margin','Daily Revenue']
temp1 = data[cols1]
temp2 = data[cols2]
temp2 = temp2[temp2["Brand"] != "Zingers"]
rts = [1.1]*3
for i in range(0,3):
  print(i)
  start = timeit.default_timer()
  x = temp1.merge(temp2, on=['Date','Customer','Brand','Category','Beverage Flavor'], how="left")
  end = timeit.default_timer()
  del x
  rts[i] = end - start
BenchmarkResults[0, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_LeftJoin.csv')
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


## 1M 1N 1D 2G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_LeftJoin.csv')
cols1 = ['Date','Customer','Brand','Category','Beverage Flavor','Daily Liters']
cols2 = ['Date','Customer','Brand','Category','Beverage Flavor','Daily Units','Daily Margin','Daily Revenue']
temp1 = data[cols1]
temp2 = data[cols2]
temp2 = temp2[temp2["Brand"] != "Zingers"]
rts = [1.1]*3
for i in range(0,3):
  print(i)
  start = timeit.default_timer()
  x = temp1.merge(temp2, on=['Date','Customer','Brand','Category','Beverage Flavor'], how="left")
  end = timeit.default_timer()
  del x
  rts[i] = end - start
BenchmarkResults[1, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_LeftJoin.csv')
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


## 1M 1N 1D 2G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_LeftJoin.csv')
cols1 = ['Date','Customer','Brand','Category','Beverage Flavor','Daily Liters']
cols2 = ['Date','Customer','Brand','Category','Beverage Flavor','Daily Units','Daily Margin','Daily Revenue']
temp1 = data[cols1]
temp2 = data[cols2]
temp2 = temp2[temp2["Brand"] != "Zingers"]
rts = [1.1]*3
for i in range(0,3):
  print(i)
  start = timeit.default_timer()
  x = temp1.merge(temp2, on=['Date','Customer','Brand','Category','Beverage Flavor'], how="left")
  end = timeit.default_timer()
  del x
  rts[i] = end - start
BenchmarkResults[2, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_LeftJoin.csv')
del BenchmarkResults, end, start, rts
gc.collect()
