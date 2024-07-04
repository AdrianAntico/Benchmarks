import pandas as pd
import pyarrow as pa
import polars as pl
import timeit
import statistics as stats
import datetime as d
import gc

# Path to source data
Path = "C:/Users/Bizon/Documents/GitHub/rappwd/"

# Create results table
BenchmarkResults = {
  'Framework': ['pandas']*41, 
  'Method': ['filter'] * 41,
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
    
    '1B 1N 1D 0G',
    '1B 1N 1D 1G',
    '1B 1N 1D 2G',
    '1B 1N 1D 3G',
    '1B 1N 1D 4G',
    '1B 2N 1D 0G',
    '1B 2N 1D 1G',
    '1B 2N 1D 2G',
    '1B 2N 1D 3G',
    '1B 2N 1D 4G', 
    'Total Runtime'],
  'TimeInSeconds': [-0.1]*41
}
BenchmarkResults = pl.DataFrame(BenchmarkResults)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Filter.csv')
del BenchmarkResults
gc.collect()

# Filter 1M
def subset_list(original_list, indices):
    return [original_list[i] for i in indices]

# Sum 1 Numeric Variable:
data = pd.read_csv(f'{Path}FakeBevData1M.csv', engine = "pyarrow", keep_default_na=False)
data['Date'] = pd.to_datetime(data['Date'])
data['Customer'] = data['Customer'].astype('category')
data['Brand'] = data['Brand'].astype('category')
data['Category'] = data['Category'].astype('category')
data['Beverage Flavor'] = data['Beverage Flavor'].astype('category')

# Filters
x = data["Customer"].unique()
CustList = ["Location " + str(i) for i in range(1,1+len(x), 2)]
x = list(data["Brand"].unique())
x.sort()
BrandList = subset_list(x, [0,2,4,8,10,12])
x = list(data["Category"].unique())
x.sort()
CatList = subset_list(x, [0,2,4])
x = list(data["Beverage Flavor"].unique())
x.sort()
BevFlavList = subset_list(x, [x for x in range(19) if x % 2 == 0])

## 1M 1N 1D 0G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Filter.csv')
rts = [1.1]*3
for i in range(0,3):
  print(i)
  start = timeit.default_timer()
  x = data[(data['Date'] > '2021-06-01') & (data['Daily Liters'] > 20)]
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[0, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Filter.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 1N 1D 1G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Filter.csv')
rts = [1.1]*3
for i in range(0,3):
  print(i)
  start = timeit.default_timer()
  x = data[(data['Date'] > '2021-06-01') & (data['Customer'].isin(CustList)) & (data['Daily Liters'] > 20)]
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[1, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Filter.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 1N 1D 2G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Filter.csv')
rts = [1.1]*3
for i in range(0,3):
  print(i)
  start = timeit.default_timer()
  x = data[(data['Date'] > '2021-06-01') & (data['Customer'].isin(CustList)) & (data['Brand'].isin(BrandList)) & (data['Daily Liters'] > 20)]
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[2, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Filter.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 1N 1D 3G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Filter.csv')
rts = [1.1]*3
for i in range(0,3):
  print(i)
  start = timeit.default_timer()
  x = data[(data['Date'] > '2021-06-01') & (data['Customer'].isin(CustList)) & (data['Brand'].isin(BrandList)) & (data['Category'].isin(CatList)) & (data['Daily Liters'] > 20)]
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[3, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Filter.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 1N 1D 4G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Filter.csv')
rts = [1.1]*3
for i in range(0,3):
  print(i)
  start = timeit.default_timer()
  x = data[(data['Date'] > '2021-06-01') & (data['Customer'].isin(CustList)) & (data['Brand'].isin(BrandList)) & (data['Category'].isin(CatList)) & (data['Beverage Flavor'].isin(BevFlavList)) & (data['Daily Liters'] > 20)]
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[4, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Filter.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 1N 1D 0G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Filter.csv')
rts = [1.1]*3
for i in range(0,3):
  print(i)
  start = timeit.default_timer()
  x = data[(data['Date'] > '2021-06-01') & (data['Daily Liters'] > 20) & (data['Daily Margin'] < 100)]
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[5, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Filter.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 1N 1D 1G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Filter.csv')
rts = [1.1]*3
for i in range(0,3):
  print(i)
  start = timeit.default_timer()
  x = data[(data['Date'] > '2021-06-01') & (data['Customer'].isin(CustList)) & (data['Daily Liters'] > 20) & (data['Daily Margin'] < 100)]
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[6, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Filter.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 1N 1D 2G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Filter.csv')
rts = [1.1]*3
for i in range(0,3):
  print(i)
  start = timeit.default_timer()
  x = data[(data['Date'] > '2021-06-01') & (data['Customer'].isin(CustList)) & (data['Brand'].isin(BrandList)) & (data['Daily Liters'] > 20) & (data['Daily Margin'] < 100)]
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[7, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Filter.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 1N 1D 3G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Filter.csv')
rts = [1.1]*3
for i in range(0,3):
  print(i)
  start = timeit.default_timer()
  x = data[(data['Date'] > '2021-06-01') & (data['Customer'].isin(CustList)) & (data['Brand'].isin(BrandList)) & (data['Category'].isin(CatList)) & (data['Daily Liters'] > 20) & (data['Daily Margin'] < 100)]
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[8, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Filter.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 1N 1D 4G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Filter.csv')
rts = [1.1]*3
for i in range(0,3):
  print(i)
  start = timeit.default_timer()
  x = data[(data['Date'] > '2021-06-01') & (data['Customer'].isin(CustList)) & (data['Brand'].isin(BrandList)) & (data['Category'].isin(CatList)) & (data['Beverage Flavor'].isin(BevFlavList)) & (data['Daily Liters'] > 20) & (data['Daily Margin'] < 100)]
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[9, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Filter.csv')
del BenchmarkResults, end, start
gc.collect()

###################################################################################################
###################################################################################################
###################################################################################################
###################################################################################################
###################################################################################################

# Filter 10M

# Sum 1 Numeric Variable:
data = pd.read_csv(f'{Path}FakeBevData10M.csv', engine = "pyarrow")
data['Date'] = pd.to_datetime(data['Date'])
data['Customer'] = data['Customer'].astype('category')
data['Brand'] = data['Brand'].astype('category')
data['Category'] = data['Category'].astype('category')
data['Beverage Flavor'] = data['Beverage Flavor'].astype('category')

# Filters
x = data["Customer"].unique()
CustList = ["Location " + str(i) for i in range(1,1+len(x), 2)]

## 1M 1N 1D 0G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Filter.csv')
rts = [1.1]*3
for i in range(0,3):
  print(i)
  start = timeit.default_timer()
  x = data[(data['Date'] > '2021-06-01') & (data['Daily Liters'] > 20)]
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[10, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Filter.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 1N 1D 1G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Filter.csv')
rts = [1.1]*3
for i in range(0,3):
  print(i)
  start = timeit.default_timer()
  x = data[(data['Date'] > '2021-06-01') & (data['Customer'].isin(CustList)) & (data['Daily Liters'] > 20)]
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[11, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Filter.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 1N 1D 2G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Filter.csv')
rts = [1.1]*3
for i in range(0,3):
  print(i)
  start = timeit.default_timer()
  x = data[(data['Date'] > '2021-06-01') & (data['Customer'].isin(CustList)) & (data['Brand'].isin(BrandList)) & (data['Daily Liters'] > 20)]
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[12, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Filter.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 1N 1D 3G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Filter.csv')
rts = [1.1]*3
for i in range(0,3):
  print(i)
  start = timeit.default_timer()
  x = data[(data['Date'] > '2021-06-01') & (data['Customer'].isin(CustList)) & (data['Brand'].isin(BrandList)) & (data['Category'].isin(CatList)) & (data['Daily Liters'] > 20)]
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[13, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Filter.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 1N 1D 4G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Filter.csv')
rts = [1.1]*3
for i in range(0,3):
  print(i)
  start = timeit.default_timer()
  x = data[(data['Date'] > '2021-06-01') & (data['Customer'].isin(CustList)) & (data['Brand'].isin(BrandList)) & (data['Category'].isin(CatList)) & (data['Beverage Flavor'].isin(BevFlavList)) & (data['Daily Liters'] > 20)]
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[14, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Filter.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 1N 1D 0G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Filter.csv')
rts = [1.1]*3
for i in range(0,3):
  print(i)
  start = timeit.default_timer()
  x = data[(data['Date'] > '2021-06-01') & (data['Daily Liters'] > 20) & (data['Daily Margin'] < 100)]
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[15, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Filter.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 1N 1D 1G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Filter.csv')
rts = [1.1]*3
for i in range(0,3):
  print(i)
  start = timeit.default_timer()
  x = data[(data['Date'] > '2021-06-01') & (data['Customer'].isin(CustList)) & (data['Daily Liters'] > 20) & (data['Daily Margin'] < 100)]
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[16, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Filter.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 1N 1D 2G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Filter.csv')
rts = [1.1]*3
for i in range(0,3):
  print(i)
  start = timeit.default_timer()
  x = data[(data['Date'] > '2021-06-01') & (data['Customer'].isin(CustList)) & (data['Brand'].isin(BrandList)) & (data['Daily Liters'] > 20) & (data['Daily Margin'] < 100)]
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[17, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Filter.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 1N 1D 3G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Filter.csv')
rts = [1.1]*3
for i in range(0,3):
  print(i)
  start = timeit.default_timer()
  x = data[(data['Date'] > '2021-06-01') & (data['Customer'].isin(CustList)) & (data['Brand'].isin(BrandList)) & (data['Category'].isin(CatList)) & (data['Daily Liters'] > 20) & (data['Daily Margin'] < 100)]
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[18, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Filter.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 1N 1D 4G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Filter.csv')
rts = [1.1]*3
for i in range(0,3):
  print(i)
  start = timeit.default_timer()
  x = data[(data['Date'] > '2021-06-01') & (data['Customer'].isin(CustList)) & (data['Brand'].isin(BrandList)) & (data['Category'].isin(CatList)) & (data['Beverage Flavor'].isin(BevFlavList)) & (data['Daily Liters'] > 20) & (data['Daily Margin'] < 100)]
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[19, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Filter.csv')
del BenchmarkResults, end, start
gc.collect()


###################################################################################################
###################################################################################################
###################################################################################################
###################################################################################################
###################################################################################################

# Filter 100M

# Sum 1 Numeric Variable:
data = pd.read_csv(f'{Path}FakeBevData100M.csv', engine = "pyarrow")
data['Date'] = pd.to_datetime(data['Date'])
data['Customer'] = data['Customer'].astype('category')
data['Brand'] = data['Brand'].astype('category')
data['Category'] = data['Category'].astype('category')
data['Beverage Flavor'] = data['Beverage Flavor'].astype('category')

# Filters
x = data["Customer"].unique()
CustList = ["Location " + str(i) for i in range(1,1+len(x), 2)]

## 1M 1N 1D 0G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Filter.csv')
rts = [1.1]*3
for i in range(0,3):
  print(i)
  start = timeit.default_timer()
  x = data[(data['Date'] > '2021-06-01') & (data['Daily Liters'] > 20)]
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[20, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Filter.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 1N 1D 1G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Filter.csv')
rts = [1.1]*3
for i in range(0,3):
  print(i)
  start = timeit.default_timer()
  x = data[(data['Date'] > '2021-06-01') & (data['Customer'].isin(CustList)) & (data['Daily Liters'] > 20)]
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[21, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Filter.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 1N 1D 2G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Filter.csv')
rts = [1.1]*3
for i in range(0,3):
  print(i)
  start = timeit.default_timer()
  x = data[(data['Date'] > '2021-06-01') & (data['Customer'].isin(CustList)) & (data['Brand'].isin(BrandList)) & (data['Daily Liters'] > 20)]
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[22, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Filter.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 1N 1D 3G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Filter.csv')
rts = [1.1]*3
for i in range(0,3):
  print(i)
  start = timeit.default_timer()
  x = data[(data['Date'] > '2021-06-01') & (data['Customer'].isin(CustList)) & (data['Brand'].isin(BrandList)) & (data['Category'].isin(CatList)) & (data['Daily Liters'] > 20)]
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[23, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Filter.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 1N 1D 4G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Filter.csv')
rts = [1.1]*3
for i in range(0,3):
  print(i)
  start = timeit.default_timer()
  x = data[(data['Date'] > '2021-06-01') & (data['Customer'].isin(CustList)) & (data['Brand'].isin(BrandList)) & (data['Category'].isin(CatList)) & (data['Beverage Flavor'].isin(BevFlavList)) & (data['Daily Liters'] > 20)]
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[24, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Filter.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 1N 1D 0G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Filter.csv')
rts = [1.1]*3
for i in range(0,3):
  print(i)
  start = timeit.default_timer()
  x = data[(data['Date'] > '2021-06-01') & (data['Daily Liters'] > 20) & (data['Daily Margin'] < 100)]
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[25, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Filter.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 1N 1D 1G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Filter.csv')
rts = [1.1]*3
for i in range(0,3):
  print(i)
  start = timeit.default_timer()
  x = data[(data['Date'] > '2021-06-01') & (data['Customer'].isin(CustList)) & (data['Daily Liters'] > 20) & (data['Daily Margin'] < 100)]
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[26, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Filter.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 1N 1D 2G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Filter.csv')
rts = [1.1]*3
for i in range(0,3):
  print(i)
  start = timeit.default_timer()
  x = data[(data['Date'] > '2021-06-01') & (data['Customer'].isin(CustList)) & (data['Brand'].isin(BrandList)) & (data['Daily Liters'] > 20) & (data['Daily Margin'] < 100)]
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[27, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Filter.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 1N 1D 3G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Filter.csv')
rts = [1.1]*3
for i in range(0,3):
  print(i)
  start = timeit.default_timer()
  x = data[(data['Date'] > '2021-06-01') & (data['Customer'].isin(CustList)) & (data['Brand'].isin(BrandList)) & (data['Category'].isin(CatList)) & (data['Daily Liters'] > 20) & (data['Daily Margin'] < 100)]
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[28, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Filter.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 1N 1D 4G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Filter.csv')
rts = [1.1]*3
for i in range(0,3):
  print(i)
  start = timeit.default_timer()
  x = data[(data['Date'] > '2021-06-01') & (data['Customer'].isin(CustList)) & (data['Brand'].isin(BrandList)) & (data['Category'].isin(CatList)) & (data['Beverage Flavor'].isin(BevFlavList)) & (data['Daily Liters'] > 20) & (data['Daily Margin'] < 100)]
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[29, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Filter.csv')
del BenchmarkResults, end, start
gc.collect()

###################################################################################################
###################################################################################################
###################################################################################################
###################################################################################################
###################################################################################################

# Filter 1B

# Sum 1 Numeric Variable:

# Sum 1 Numeric Variable:
data = pd.read_csv(f'{Path}FakeBevData1B.csv', engine = "pyarrow")
data['Date'] = pd.to_datetime(data['Date'])
data['Customer'] = data['Customer'].astype('category')
data['Brand'] = data['Brand'].astype('category')
data['Category'] = data['Category'].astype('category')
data['Beverage Flavor'] = data['Beverage Flavor'].astype('category')

# Filters
x = data["Customer"].unique()
CustList = ["Location " + str(i) for i in range(1,1+len(x), 2)]

## 1M 1N 1D 0G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Filter.csv')
rts = [1.1]*3
for i in range(0,3):
  print(i)
  start = timeit.default_timer()
  x = data[(data['Date'] > '2021-06-01') & (data['Daily Liters'] > 20)]
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[30, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Filter.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 1N 1D 1G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Filter.csv')
rts = [1.1]*3
for i in range(0,3):
  print(i)
  start = timeit.default_timer()
  x = data[(data['Date'] > '2021-06-01') & (data['Customer'].isin(CustList)) & (data['Daily Liters'] > 20)]
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[31, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Filter.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 1N 1D 2G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Filter.csv')
rts = [1.1]*3
for i in range(0,3):
  print(i)
  start = timeit.default_timer()
  x = data[(data['Date'] > '2021-06-01') & (data['Customer'].isin(CustList)) & (data['Brand'].isin(BrandList)) & (data['Daily Liters'] > 20)]
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[32, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Filter.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 1N 1D 3G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Filter.csv')
rts = [1.1]*3
for i in range(0,3):
  print(i)
  start = timeit.default_timer()
  x = data[(data['Date'] > '2021-06-01') & (data['Customer'].isin(CustList)) & (data['Brand'].isin(BrandList)) & (data['Category'].isin(CatList)) & (data['Daily Liters'] > 20)]
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[33, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Filter.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 1N 1D 4G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Filter.csv')
rts = [1.1]*3
for i in range(0,3):
  print(i)
  start = timeit.default_timer()
  x = data[(data['Date'] > '2021-06-01') & (data['Customer'].isin(CustList)) & (data['Brand'].isin(BrandList)) & (data['Category'].isin(CatList)) & (data['Beverage Flavor'].isin(BevFlavList)) & (data['Daily Liters'] > 20)]
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[34, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Filter.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 1N 1D 0G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Filter.csv')
rts = [1.1]*3
for i in range(0,3):
  print(i)
  start = timeit.default_timer()
  x = data[(data['Date'] > '2021-06-01') & (data['Daily Liters'] > 20) & (data['Daily Margin'] < 100)]
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[35, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Filter.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 1N 1D 1G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Filter.csv')
rts = [1.1]*3
for i in range(0,3):
  print(i)
  start = timeit.default_timer()
  x = data[(data['Date'] > '2021-06-01') & (data['Customer'].isin(CustList)) & (data['Daily Liters'] > 20) & (data['Daily Margin'] < 100)]
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[36, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Filter.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 1N 1D 2G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Filter.csv')
rts = [1.1]*3
for i in range(0,3):
  print(i)
  start = timeit.default_timer()
  x = data[(data['Date'] > '2021-06-01') & (data['Customer'].isin(CustList)) & (data['Brand'].isin(BrandList)) & (data['Daily Liters'] > 20) & (data['Daily Margin'] < 100)]
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[37, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Filter.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 1N 1D 3G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Filter.csv')
rts = [1.1]*3
for i in range(0,3):
  print(i)
  start = timeit.default_timer()
  x = data[(data['Date'] > '2021-06-01') & (data['Customer'].isin(CustList)) & (data['Brand'].isin(BrandList)) & (data['Category'].isin(CatList)) & (data['Daily Liters'] > 20) & (data['Daily Margin'] < 100)]
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[38, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Filter.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 1N 1D 4G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Filter.csv')
rts = [1.1]*3
for i in range(0,3):
  print(i)
  start = timeit.default_timer()
  x = data[(data['Date'] > '2021-06-01') & (data['Customer'].isin(CustList)) & (data['Brand'].isin(BrandList)) & (data['Category'].isin(CatList)) & (data['Beverage Flavor'].isin(BevFlavList)) & (data['Daily Liters'] > 20) & (data['Daily Margin'] < 100)]
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[39, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Filter.csv')
del BenchmarkResults, end, start
gc.collect()

BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Filter.csv')
x = BenchmarkResults[0:39]
y = x['TimeInSeconds'].sum()
BenchmarkResults[40, 'TimeInSeconds'] = y
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Filter.csv')

