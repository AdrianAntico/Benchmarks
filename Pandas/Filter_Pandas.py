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
  'Framework': ['pandas']*3, 
  'Method': ['filter'] * 3,
  'Experiment': [
    '1M 2N 1D 4G',
    '10M 2N 1D 4G',
    '100M 2N 1D 4G'
  ],
  'TimeInSeconds': [-0.1]*3
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


## 1M 1N 1D 4G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Filter.csv')
rts = [1.1]*3
for i in range(0,3):
  print(i)
  start = timeit.default_timer()
  x = data[(data['Date'] > '2021-06-01') & (data['Customer'].isin(CustList)) & (data['Brand'].isin(BrandList)) & (data['Category'].isin(CatList)) & (data['Beverage Flavor'].isin(BevFlavList)) & (data['Daily Liters'] > 20) & (data['Daily Margin'] < 100)]
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[0, 'TimeInSeconds'] = stats.median(rts)
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



## 1M 1N 1D 4G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Filter.csv')
rts = [1.1]*3
for i in range(0,3):
  print(i)
  start = timeit.default_timer()
  x = data[(data['Date'] > '2021-06-01') & (data['Customer'].isin(CustList)) & (data['Brand'].isin(BrandList)) & (data['Category'].isin(CatList)) & (data['Beverage Flavor'].isin(BevFlavList)) & (data['Daily Liters'] > 20) & (data['Daily Margin'] < 100)]
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[1, 'TimeInSeconds'] = stats.median(rts)
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


## 1M 1N 1D 4G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPandas_Filter.csv')
rts = [1.1]*3
for i in range(0,3):
  print(i)
  start = timeit.default_timer()
  x = data[(data['Date'] > '2021-06-01') & (data['Customer'].isin(CustList)) & (data['Brand'].isin(BrandList)) & (data['Category'].isin(CatList)) & (data['Beverage Flavor'].isin(BevFlavList)) & (data['Daily Liters'] > 20) & (data['Daily Margin'] < 100)]
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[2, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPandas_Filter.csv')
del BenchmarkResults, end, start
gc.collect()
