import pandas as pd
import pyarrow as pa
import timeit
import gc

# Path to source data
Path = "C:/Users/Bizon/Documents/GitHub/rappwd/"

# Create results table
BenchmarkResults = {
  'Framework': ['pandas']*61, 
  'Method': ['sum aggregation'] * 61,
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
    '1B 3N 1D 0G',
    '1B 3N 1D 1G',
    '1B 3N 1D 2G',
    '1B 3N 1D 3G',
    '1B 3N 1D 4G',
    'Total Runtime'],
  'TimeInSeconds': [-0.1]*61
}
BenchmarkResults = pd.DataFrame(BenchmarkResults)
BenchmarkResults.to_csv(f'{Path}BenchmarkResultsPandas.csv')
del BenchmarkResults
gc.collect()

# Aggregation 1M

# Sum 1 Numeric Variable:

## 1M 1N 1D 0G
data = pd.read_csv(f'{Path}FakeBevData1M.csv', engine = "pyarrow")
data['Date'] = pd.to_datetime(data['Date'])
data['Customer'] = data['Customer'].astype('category')
data['Brand'] = data['Brand'].astype('category')
data['Category'] = data['Category'].astype('category')
data['Beverage Flavor'] = data['Beverage Flavor'].astype('category')
BenchmarkResults = pd.read_csv(f'{Path}BenchmarkResultsPandas.csv')
start = timeit.default_timer()
data.groupby('Date', as_index=False, sort=False, observed=True, dropna=True).agg({'Daily Liters':'sum'})
end = timeit.default_timer()
BenchmarkResults.at[0, 'TimeInSeconds'] = end - start
BenchmarkResults.to_csv(f'{Path}BenchmarkResultsPandas.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 1N 1D 1G
BenchmarkResults = pd.read_csv(f'{Path}BenchmarkResultsPandas.csv')
start = timeit.default_timer()
data.groupby(['Date','Customer'], as_index=False, sort=False, observed=True, dropna=True).agg({'Daily Liters':'sum'})
end = timeit.default_timer()
BenchmarkResults.at[1, 'TimeInSeconds'] = end - start
BenchmarkResults.to_csv(f'{Path}BenchmarkResultsPandas.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 1N 1D 2G
BenchmarkResults = pd.read_csv(f'{Path}BenchmarkResultsPandas.csv')
start = timeit.default_timer()
data.groupby(['Date','Customer','Brand'], as_index=False, sort=False, observed=True, dropna=True).agg({'Daily Liters':'sum'})
end = timeit.default_timer()
BenchmarkResults.at[2, 'TimeInSeconds'] = end - start
BenchmarkResults.to_csv(f'{Path}BenchmarkResultsPandas.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 1N 1D 3G
BenchmarkResults = pd.read_csv(f'{Path}BenchmarkResultsPandas.csv')
start = timeit.default_timer()
data.groupby(['Date','Customer','Brand','Category'], as_index=False, sort=False, observed=True, dropna=True).agg({'Daily Liters':'sum'})
end = timeit.default_timer()
BenchmarkResults.at[3, 'TimeInSeconds'] = end - start
BenchmarkResults.to_csv(f'{Path}BenchmarkResultsPandas.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 1N 1D 4G
BenchmarkResults = pd.read_csv(f'{Path}BenchmarkResultsPandas.csv')
start = timeit.default_timer()
data.groupby(['Date','Customer','Brand','Category','Beverage Flavor'], as_index=False, sort=False, observed=True, dropna=True).agg({'Daily Liters':'sum'})
end = timeit.default_timer()
BenchmarkResults.at[4, 'TimeInSeconds'] = end - start
BenchmarkResults.to_csv(f'{Path}BenchmarkResultsPandas.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 2N 1D 0G
BenchmarkResults = pd.read_csv(f'{Path}BenchmarkResultsPandas.csv')
start = timeit.default_timer()
data.groupby('Date', as_index=False, sort=False, observed=True, dropna=True).agg({'Daily Liters':'sum', 'Daily Units':'sum'})
end = timeit.default_timer()
BenchmarkResults.at[5, 'TimeInSeconds'] = end - start
BenchmarkResults.to_csv(f'{Path}BenchmarkResultsPandas.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 2N 1D 1G
BenchmarkResults = pd.read_csv(f'{Path}BenchmarkResultsPandas.csv')
start = timeit.default_timer()
data.groupby(['Date','Customer'], as_index=False, sort=False, observed=True, dropna=True).agg({'Daily Liters':'sum', 'Daily Units':'sum'})
end = timeit.default_timer()
BenchmarkResults.at[6, 'TimeInSeconds'] = end - start
BenchmarkResults.to_csv(f'{Path}BenchmarkResultsPandas.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 2N 1D 2G
BenchmarkResults = pd.read_csv(f'{Path}BenchmarkResultsPandas.csv')
start = timeit.default_timer()
data.groupby(['Date','Customer','Brand'], as_index=False, sort=False, observed=True, dropna=True).agg({'Daily Liters':'sum', 'Daily Units':'sum'})
end = timeit.default_timer()
BenchmarkResults.at[7, 'TimeInSeconds'] = end - start
BenchmarkResults.to_csv(f'{Path}BenchmarkResultsPandas.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 2N 1D 3G
BenchmarkResults = pd.read_csv(f'{Path}BenchmarkResultsPandas.csv')
start = timeit.default_timer()
data.groupby(['Date','Customer','Brand','Category'], as_index=False, sort=False, observed=True, dropna=True).agg({'Daily Liters':'sum', 'Daily Units':'sum'})
end = timeit.default_timer()
BenchmarkResults.at[8, 'TimeInSeconds'] = end - start
BenchmarkResults.to_csv(f'{Path}BenchmarkResultsPandas.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 2N 1D 4G
BenchmarkResults = pd.read_csv(f'{Path}BenchmarkResultsPandas.csv')
start = timeit.default_timer()
data.groupby(['Date','Customer','Brand','Category','Beverage Flavor'], as_index=False, sort=False, observed=True, dropna=True).agg({'Daily Liters':'sum', 'Daily Units':'sum'})
end = timeit.default_timer()
BenchmarkResults.at[9, 'TimeInSeconds'] = end - start
BenchmarkResults.to_csv(f'{Path}BenchmarkResultsPandas.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 3N 1D 0G
BenchmarkResults = pd.read_csv(f'{Path}BenchmarkResultsPandas.csv')
start = timeit.default_timer()
data.groupby('Date', as_index=False, sort=False, observed=True, dropna=True).agg({'Daily Liters':'sum', 'Daily Units':'sum', 'Daily Margin':'sum'})
end = timeit.default_timer()
BenchmarkResults.at[10, 'TimeInSeconds'] = end - start
BenchmarkResults.to_csv(f'{Path}BenchmarkResultsPandas.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 3N 1D 1G
BenchmarkResults = pd.read_csv(f'{Path}BenchmarkResultsPandas.csv')
start = timeit.default_timer()
data.groupby(['Date','Customer'], as_index=False, sort=False, observed=True, dropna=True).agg({'Daily Liters':'sum', 'Daily Units':'sum', 'Daily Margin':'sum'})
end = timeit.default_timer()
BenchmarkResults.at[11, 'TimeInSeconds'] = end - start
BenchmarkResults.to_csv(f'{Path}BenchmarkResultsPandas.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 3N 1D 2G
BenchmarkResults = pd.read_csv(f'{Path}BenchmarkResultsPandas.csv')
start = timeit.default_timer()
data.groupby(['Date','Customer','Brand'], as_index=False, sort=False, observed=True, dropna=True).agg({'Daily Liters':'sum', 'Daily Units':'sum', 'Daily Margin':'sum'})
end = timeit.default_timer()
BenchmarkResults.at[12, 'TimeInSeconds'] = end - start
BenchmarkResults.to_csv(f'{Path}BenchmarkResultsPandas.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 3N 1D 3G
BenchmarkResults = pd.read_csv(f'{Path}BenchmarkResultsPandas.csv')
start = timeit.default_timer()
data.groupby(['Date','Customer','Brand','Category'], as_index=False, sort=False, observed=True, dropna=True).agg({'Daily Liters':'sum', 'Daily Units':'sum', 'Daily Margin':'sum'})
end = timeit.default_timer()
BenchmarkResults.at[13, 'TimeInSeconds'] = end - start
BenchmarkResults.to_csv(f'{Path}BenchmarkResultsPandas.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 3N 1D 4G
BenchmarkResults = pd.read_csv(f'{Path}BenchmarkResultsPandas.csv')
start = timeit.default_timer()
data.groupby(['Date','Customer','Brand','Category','Beverage Flavor'], as_index=False, sort=False, observed=True, dropna=True).agg({'Daily Liters':'sum', 'Daily Units':'sum', 'Daily Margin':'sum'})
end = timeit.default_timer()
BenchmarkResults.at[14, 'TimeInSeconds'] = end - start
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

## 10M 1N 1D 0G
data = pd.read_csv(f'{Path}FakeBevData10M.csv', engine = "pyarrow")
data['Date'] = pd.to_datetime(data['Date'])
data['Customer'] = data['Customer'].astype('category')
data['Brand'] = data['Brand'].astype('category')
data['Category'] = data['Category'].astype('category')
data['Beverage Flavor'] = data['Beverage Flavor'].astype('category')
BenchmarkResults = pd.read_csv(f'{Path}BenchmarkResultsPandas.csv')
start = timeit.default_timer()
data.groupby('Date', as_index=False, sort=False, observed=True, dropna=True).agg({'Daily Liters':'sum'})
end = timeit.default_timer()
BenchmarkResults.at[15, 'TimeInSeconds'] = end - start
BenchmarkResults.to_csv(f'{Path}BenchmarkResultsPandas.csv')
del BenchmarkResults, end, start
gc.collect()

## 10M 1N 1D 1G
BenchmarkResults = pd.read_csv(f'{Path}BenchmarkResultsPandas.csv')
start = timeit.default_timer()
data.groupby(['Date','Customer'], as_index=False, sort=False, observed=True, dropna=True).agg({'Daily Liters':'sum'})
end = timeit.default_timer()
BenchmarkResults.at[16, 'TimeInSeconds'] = end - start
BenchmarkResults.to_csv(f'{Path}BenchmarkResultsPandas.csv')
del BenchmarkResults, end, start
gc.collect()

## 10M 1N 1D 2G
BenchmarkResults = pd.read_csv(f'{Path}BenchmarkResultsPandas.csv')
start = timeit.default_timer()
data.groupby(['Date','Customer','Brand'], as_index=False, sort=False, observed=True, dropna=True).agg({'Daily Liters':'sum'})
end = timeit.default_timer()
BenchmarkResults.at[17, 'TimeInSeconds'] = end - start
BenchmarkResults.to_csv(f'{Path}BenchmarkResultsPandas.csv')
del BenchmarkResults, end, start
gc.collect()

## 10M 1N 1D 3G
BenchmarkResults = pd.read_csv(f'{Path}BenchmarkResultsPandas.csv')
start = timeit.default_timer()
data.groupby(['Date','Customer','Brand','Category'], as_index=False, sort=False, observed=True, dropna=True).agg({'Daily Liters':'sum'})
end = timeit.default_timer()
BenchmarkResults.at[18, 'TimeInSeconds'] = end - start
BenchmarkResults.to_csv(f'{Path}BenchmarkResultsPandas.csv')
del BenchmarkResults, end, start
gc.collect()

## 10M 1N 1D 4G
BenchmarkResults = pd.read_csv(f'{Path}BenchmarkResultsPandas.csv')
start = timeit.default_timer()
data.groupby(['Date','Customer','Brand','Category','Beverage Flavor'], as_index=False, sort=False, observed=True, dropna=True).agg({'Daily Liters':'sum'})
end = timeit.default_timer()
BenchmarkResults.at[19, 'TimeInSeconds'] = end - start
BenchmarkResults.to_csv(f'{Path}BenchmarkResultsPandas.csv')
del BenchmarkResults, end, start
gc.collect()

## 10M 2N 1D 0G
BenchmarkResults = pd.read_csv(f'{Path}BenchmarkResultsPandas.csv')
start = timeit.default_timer()
data.groupby('Date', as_index=False, sort=False, observed=True, dropna=True).agg({'Daily Liters':'sum', 'Daily Units':'sum'})
end = timeit.default_timer()
BenchmarkResults.at[20, 'TimeInSeconds'] = end - start
BenchmarkResults.to_csv(f'{Path}BenchmarkResultsPandas.csv')
del BenchmarkResults, end, start
gc.collect()

## 10M 2N 1D 1G
BenchmarkResults = pd.read_csv(f'{Path}BenchmarkResultsPandas.csv')
start = timeit.default_timer()
data.groupby(['Date','Customer'], as_index=False, sort=False, observed=True, dropna=True).agg({'Daily Liters':'sum', 'Daily Units':'sum'})
end = timeit.default_timer()
BenchmarkResults.at[21, 'TimeInSeconds'] = end - start
BenchmarkResults.to_csv(f'{Path}BenchmarkResultsPandas.csv')
del BenchmarkResults, end, start
gc.collect()

## 10M 2N 1D 2G
BenchmarkResults = pd.read_csv(f'{Path}BenchmarkResultsPandas.csv')
start = timeit.default_timer()
data.groupby(['Date','Customer','Brand'], as_index=False, sort=False, observed=True, dropna=True).agg({'Daily Liters':'sum', 'Daily Units':'sum'})
end = timeit.default_timer()
BenchmarkResults.at[22, 'TimeInSeconds'] = end - start
BenchmarkResults.to_csv(f'{Path}BenchmarkResultsPandas.csv')
del BenchmarkResults, end, start
gc.collect()

## 10M 2N 1D 3G
BenchmarkResults = pd.read_csv(f'{Path}BenchmarkResultsPandas.csv')
start = timeit.default_timer()
data.groupby(['Date','Customer','Brand','Category'], as_index=False, sort=False, observed=True, dropna=True).agg({'Daily Liters':'sum', 'Daily Units':'sum'})
end = timeit.default_timer()
BenchmarkResults.at[23, 'TimeInSeconds'] = end - start
BenchmarkResults.to_csv(f'{Path}BenchmarkResultsPandas.csv')
del BenchmarkResults, end, start
gc.collect()

## 10M 2N 1D 4G
BenchmarkResults = pd.read_csv(f'{Path}BenchmarkResultsPandas.csv')
start = timeit.default_timer()
data.groupby(['Date','Customer','Brand','Category','Beverage Flavor'], as_index=False, sort=False, observed=True, dropna=True).agg({'Daily Liters':'sum', 'Daily Units':'sum'})
end = timeit.default_timer()
BenchmarkResults.at[24, 'TimeInSeconds'] = end - start
BenchmarkResults.to_csv(f'{Path}BenchmarkResultsPandas.csv')
del BenchmarkResults, end, start
gc.collect()

## 10M 3N 1D 0G
BenchmarkResults = pd.read_csv(f'{Path}BenchmarkResultsPandas.csv')
start = timeit.default_timer()
data.groupby('Date', as_index=False, sort=False, observed=True, dropna=True).agg({'Daily Liters':'sum', 'Daily Units':'sum', 'Daily Margin':'sum'})
end = timeit.default_timer()
BenchmarkResults.at[25, 'TimeInSeconds'] = end - start
BenchmarkResults.to_csv(f'{Path}BenchmarkResultsPandas.csv')
del BenchmarkResults, end, start
gc.collect()

## 10M 3N 1D 1G
BenchmarkResults = pd.read_csv(f'{Path}BenchmarkResultsPandas.csv')
start = timeit.default_timer()
data.groupby(['Date','Customer'], as_index=False, sort=False, observed=True, dropna=True).agg({'Daily Liters':'sum', 'Daily Units':'sum', 'Daily Margin':'sum'})
end = timeit.default_timer()
BenchmarkResults.at[26, 'TimeInSeconds'] = end - start
BenchmarkResults.to_csv(f'{Path}BenchmarkResultsPandas.csv')
del BenchmarkResults, end, start
gc.collect()

## 10M 3N 1D 2G
BenchmarkResults = pd.read_csv(f'{Path}BenchmarkResultsPandas.csv')
start = timeit.default_timer()
data.groupby(['Date','Customer','Brand'], as_index=False, sort=False, observed=True, dropna=True).agg({'Daily Liters':'sum', 'Daily Units':'sum', 'Daily Margin':'sum'})
end = timeit.default_timer()
BenchmarkResults.at[27, 'TimeInSeconds'] = end - start
BenchmarkResults.to_csv(f'{Path}BenchmarkResultsPandas.csv')
del BenchmarkResults, end, start
gc.collect()

## 10M 3N 1D 3G
BenchmarkResults = pd.read_csv(f'{Path}BenchmarkResultsPandas.csv')
start = timeit.default_timer()
data.groupby(['Date','Customer','Brand','Category'], as_index=False, sort=False, observed=True, dropna=True).agg({'Daily Liters':'sum', 'Daily Units':'sum', 'Daily Margin':'sum'})
end = timeit.default_timer()
BenchmarkResults.at[28, 'TimeInSeconds'] = end - start
BenchmarkResults.to_csv(f'{Path}BenchmarkResultsPandas.csv')
del BenchmarkResults, end, start
gc.collect()

## 10M 3N 1D 4G
BenchmarkResults = pd.read_csv(f'{Path}BenchmarkResultsPandas.csv')
start = timeit.default_timer()
data.groupby(['Date','Customer','Brand','Category','Beverage Flavor'], as_index=False, sort=False, observed=True, dropna=True).agg({'Daily Liters':'sum', 'Daily Units':'sum', 'Daily Margin':'sum'})
end = timeit.default_timer()
BenchmarkResults.at[29, 'TimeInSeconds'] = end - start
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

## 100M 1N 1D 0G
data = pd.read_csv(f'{Path}FakeBevData100M.csv', engine = "pyarrow")
data['Date'] = pd.to_datetime(data['Date'])
data['Customer'] = data['Customer'].astype('category')
data['Brand'] = data['Brand'].astype('category')
data['Category'] = data['Category'].astype('category')
data['Beverage Flavor'] = data['Beverage Flavor'].astype('category')
BenchmarkResults = pd.read_csv(f'{Path}BenchmarkResultsPandas.csv')
start = timeit.default_timer()
data.groupby('Date', as_index=False, sort=False, observed=True, dropna=True).agg({'Daily Liters':'sum'})
end = timeit.default_timer()
BenchmarkResults.at[30, 'TimeInSeconds'] = end - start
BenchmarkResults.to_csv(f'{Path}BenchmarkResultsPandas.csv')
del BenchmarkResults, end, start
gc.collect()

## 100M 1N 1D 1G
BenchmarkResults = pd.read_csv(f'{Path}BenchmarkResultsPandas.csv')
start = timeit.default_timer()
data.groupby(['Date','Customer'], as_index=False, sort=False, observed=True, dropna=True).agg({'Daily Liters':'sum'})
end = timeit.default_timer()
BenchmarkResults.at[31, 'TimeInSeconds'] = end - start
BenchmarkResults.to_csv(f'{Path}BenchmarkResultsPandas.csv')
del BenchmarkResults, end, start
gc.collect()

## 100M 1N 1D 2G
BenchmarkResults = pd.read_csv(f'{Path}BenchmarkResultsPandas.csv')
start = timeit.default_timer()
data.groupby(['Date','Customer','Brand'], as_index=False, sort=False, observed=True, dropna=True).agg({'Daily Liters':'sum'})
end = timeit.default_timer()
BenchmarkResults.at[32, 'TimeInSeconds'] = end - start
BenchmarkResults.to_csv(f'{Path}BenchmarkResultsPandas.csv')
del BenchmarkResults, end, start
gc.collect()

## 100M 1N 1D 3G
BenchmarkResults = pd.read_csv(f'{Path}BenchmarkResultsPandas.csv')
start = timeit.default_timer()
data.groupby(['Date','Customer','Brand','Category'], as_index=False, sort=False, observed=True, dropna=True).agg({'Daily Liters':'sum'})
end = timeit.default_timer()
BenchmarkResults.at[33, 'TimeInSeconds'] = end - start
BenchmarkResults.to_csv(f'{Path}BenchmarkResultsPandas.csv')
del BenchmarkResults, end, start
gc.collect()

## 100M 1N 1D 4G
BenchmarkResults = pd.read_csv(f'{Path}BenchmarkResultsPandas.csv')
start = timeit.default_timer()
data.groupby(['Date','Customer','Brand','Category','Beverage Flavor'], as_index=False, sort=False, observed=True, dropna=True).agg({'Daily Liters':'sum'})
end = timeit.default_timer()
BenchmarkResults.at[34, 'TimeInSeconds'] = end - start
BenchmarkResults.to_csv(f'{Path}BenchmarkResultsPandas.csv')
del BenchmarkResults, end, start
gc.collect()

## 100M 2N 1D 0G
BenchmarkResults = pd.read_csv(f'{Path}BenchmarkResultsPandas.csv')
start = timeit.default_timer()
data.groupby('Date', as_index=False, sort=False, observed=True, dropna=True).agg({'Daily Liters':'sum', 'Daily Units':'sum'})
end = timeit.default_timer()
BenchmarkResults.at[35, 'TimeInSeconds'] = end - start
BenchmarkResults.to_csv(f'{Path}BenchmarkResultsPandas.csv')
del BenchmarkResults, end, start
gc.collect()

## 100M 2N 1D 1G
BenchmarkResults = pd.read_csv(f'{Path}BenchmarkResultsPandas.csv')
start = timeit.default_timer()
data.groupby(['Date','Customer'], as_index=False, sort=False, observed=True, dropna=True).agg({'Daily Liters':'sum', 'Daily Units':'sum'})
end = timeit.default_timer()
BenchmarkResults.at[36, 'TimeInSeconds'] = end - start
BenchmarkResults.to_csv(f'{Path}BenchmarkResultsPandas.csv')
del BenchmarkResults, end, start
gc.collect()

## 100M 2N 1D 2G
BenchmarkResults = pd.read_csv(f'{Path}BenchmarkResultsPandas.csv')
start = timeit.default_timer()
data.groupby(['Date','Customer','Brand'], as_index=False, sort=False, observed=True, dropna=True).agg({'Daily Liters':'sum', 'Daily Units':'sum'})
end = timeit.default_timer()
BenchmarkResults.at[37, 'TimeInSeconds'] = end - start
BenchmarkResults.to_csv(f'{Path}BenchmarkResultsPandas.csv')
del BenchmarkResults, end, start
gc.collect()

## 100M 2N 1D 3G
BenchmarkResults = pd.read_csv(f'{Path}BenchmarkResultsPandas.csv')
start = timeit.default_timer()
data.groupby(['Date','Customer','Brand','Category'], as_index=False, sort=False, observed=True, dropna=True).agg({'Daily Liters':'sum', 'Daily Units':'sum'})
end = timeit.default_timer()
BenchmarkResults.at[38, 'TimeInSeconds'] = end - start
BenchmarkResults.to_csv(f'{Path}BenchmarkResultsPandas.csv')
del BenchmarkResults, end, start
gc.collect()

## 100M 2N 1D 4G
BenchmarkResults = pd.read_csv(f'{Path}BenchmarkResultsPandas.csv')
start = timeit.default_timer()
data.groupby(['Date','Customer','Brand','Category','Beverage Flavor'], as_index=False, sort=False, observed=True, dropna=True).agg({'Daily Liters':'sum', 'Daily Units':'sum'})
end = timeit.default_timer()
BenchmarkResults.at[39, 'TimeInSeconds'] = end - start
BenchmarkResults.to_csv(f'{Path}BenchmarkResultsPandas.csv')
del BenchmarkResults, end, start
gc.collect()

## 100M 3N 1D 0G
BenchmarkResults = pd.read_csv(f'{Path}BenchmarkResultsPandas.csv')
start = timeit.default_timer()
data.groupby('Date', as_index=False, sort=False, observed=True, dropna=True).agg({'Daily Liters':'sum', 'Daily Units':'sum', 'Daily Margin':'sum'})
end = timeit.default_timer()
BenchmarkResults.at[40, 'TimeInSeconds'] = end - start
BenchmarkResults.to_csv(f'{Path}BenchmarkResultsPandas.csv')
del BenchmarkResults, end, start
gc.collect()

## 100M 3N 1D 1G
BenchmarkResults = pd.read_csv(f'{Path}BenchmarkResultsPandas.csv')
start = timeit.default_timer()
data.groupby(['Date','Customer'], as_index=False, sort=False, observed=True, dropna=True).agg({'Daily Liters':'sum', 'Daily Units':'sum', 'Daily Margin':'sum'})
end = timeit.default_timer()
BenchmarkResults.at[41, 'TimeInSeconds'] = end - start
BenchmarkResults.to_csv(f'{Path}BenchmarkResultsPandas.csv')
del BenchmarkResults, end, start
gc.collect()

## 100M 3N 1D 2G
BenchmarkResults = pd.read_csv(f'{Path}BenchmarkResultsPandas.csv')
start = timeit.default_timer()
data.groupby(['Date','Customer','Brand'], as_index=False, sort=False, observed=True, dropna=True).agg({'Daily Liters':'sum', 'Daily Units':'sum', 'Daily Margin':'sum'})
end = timeit.default_timer()
BenchmarkResults.at[42, 'TimeInSeconds'] = end - start
BenchmarkResults.to_csv(f'{Path}BenchmarkResultsPandas.csv')
del BenchmarkResults, end, start
gc.collect()

## 100M 3N 1D 3G
BenchmarkResults = pd.read_csv(f'{Path}BenchmarkResultsPandas.csv')
start = timeit.default_timer()
data.groupby(['Date','Customer','Brand','Category'], as_index=False, sort=False, observed=True, dropna=True).agg({'Daily Liters':'sum', 'Daily Units':'sum', 'Daily Margin':'sum'})
end = timeit.default_timer()
BenchmarkResults.at[43, 'TimeInSeconds'] = end - start
BenchmarkResults.to_csv(f'{Path}BenchmarkResultsPandas.csv')
del BenchmarkResults, end, start
gc.collect()

## 100M 3N 1D 4G
BenchmarkResults = pd.read_csv(f'{Path}BenchmarkResultsPandas.csv')
start = timeit.default_timer()
data.groupby(['Date','Customer','Brand','Category','Beverage Flavor'], as_index=False, sort=False, observed=True, dropna=True).agg({'Daily Liters':'sum', 'Daily Units':'sum', 'Daily Margin':'sum'})
end = timeit.default_timer()
BenchmarkResults.at[44, 'TimeInSeconds'] = end - start
BenchmarkResults.to_csv(f'{Path}BenchmarkResultsPandas.csv')
del data, BenchmarkResults, end, start
gc.collect()

###################################################################################################
###################################################################################################
###################################################################################################
###################################################################################################
###################################################################################################

# Aggregation 1B

# Sum 1 Numeric Variable:

## 1B 1N 1D 0G
data = pd.read_csv(f'{Path}FakeBevData1B.csv', engine = "pyarrow")
data['Date'] = pd.to_datetime(data['Date'])
data['Customer'] = data['Customer'].astype('category')
data['Brand'] = data['Brand'].astype('category')
data['Category'] = data['Category'].astype('category')
data['Beverage Flavor'] = data['Beverage Flavor'].astype('category')
BenchmarkResults = pd.read_csv(f'{Path}BenchmarkResultsPandas.csv')
start = timeit.default_timer()
data.groupby('Date', as_index=False, sort=False, observed=True, dropna=True).agg({'Daily Liters':'sum'})
end = timeit.default_timer()
BenchmarkResults.at[45, 'TimeInSeconds'] = end - start
BenchmarkResults.to_csv(f'{Path}BenchmarkResultsPandas.csv')
del BenchmarkResults, end, start
gc.collect()

## 1B 1N 1D 1G
BenchmarkResults = pd.read_csv(f'{Path}BenchmarkResultsPandas.csv')
start = timeit.default_timer()
data.groupby(['Date','Customer'], as_index=False, sort=False, observed=True, dropna=True).agg({'Daily Liters':'sum'})
end = timeit.default_timer()
BenchmarkResults.at[46, 'TimeInSeconds'] = end - start
BenchmarkResults.to_csv(f'{Path}BenchmarkResultsPandas.csv')
del BenchmarkResults, end, start
gc.collect()

## 1B 1N 1D 2G
BenchmarkResults = pd.read_csv(f'{Path}BenchmarkResultsPandas.csv')
start = timeit.default_timer()
data.groupby(['Date','Customer','Brand'], as_index=False, sort=False, observed=True, dropna=True).agg({'Daily Liters':'sum'})
end = timeit.default_timer()
BenchmarkResults.at[47, 'TimeInSeconds'] = end - start
BenchmarkResults.to_csv(f'{Path}BenchmarkResultsPandas.csv')
del BenchmarkResults, end, start
gc.collect()

## 1B 1N 1D 3G
BenchmarkResults = pd.read_csv(f'{Path}BenchmarkResultsPandas.csv')
start = timeit.default_timer()
data.groupby(['Date','Customer','Brand','Category'], as_index=False, sort=False, observed=True, dropna=True).agg({'Daily Liters':'sum'})
end = timeit.default_timer()
BenchmarkResults.at[48, 'TimeInSeconds'] = end - start
BenchmarkResults.to_csv(f'{Path}BenchmarkResultsPandas.csv')
del BenchmarkResults, end, start
gc.collect()

## 1B 1N 1D 4G
BenchmarkResults = pd.read_csv(f'{Path}BenchmarkResultsPandas.csv')
start = timeit.default_timer()
data.groupby(['Date','Customer','Brand','Category','Beverage Flavor'], as_index=False, sort=False, observed=True, dropna=True).agg({'Daily Liters':'sum'})
end = timeit.default_timer()
BenchmarkResults.at[49, 'TimeInSeconds'] = end - start
BenchmarkResults.to_csv(f'{Path}BenchmarkResultsPandas.csv')
del BenchmarkResults, end, start
gc.collect()

## 1B 2N 1D 0G
BenchmarkResults = pd.read_csv(f'{Path}BenchmarkResultsPandas.csv')
start = timeit.default_timer()
data.groupby('Date', as_index=False, sort=False, observed=True, dropna=True).agg({'Daily Liters':'sum', 'Daily Units':'sum'})
end = timeit.default_timer()
BenchmarkResults.at[50, 'TimeInSeconds'] = end - start
BenchmarkResults.to_csv(f'{Path}BenchmarkResultsPandas.csv')
del BenchmarkResults, end, start
gc.collect()

## 1B 2N 1D 1G
BenchmarkResults = pd.read_csv(f'{Path}BenchmarkResultsPandas.csv')
start = timeit.default_timer()
data.groupby(['Date','Customer'], as_index=False, sort=False, observed=True, dropna=True).agg({'Daily Liters':'sum', 'Daily Units':'sum'})
end = timeit.default_timer()
BenchmarkResults.at[51, 'TimeInSeconds'] = end - start
BenchmarkResults.to_csv(f'{Path}BenchmarkResultsPandas.csv')
del BenchmarkResults, end, start
gc.collect()

## 1B 2N 1D 2G
BenchmarkResults = pd.read_csv(f'{Path}BenchmarkResultsPandas.csv')
start = timeit.default_timer()
data.groupby(['Date','Customer','Brand'], as_index=False, sort=False, observed=True, dropna=True).agg({'Daily Liters':'sum', 'Daily Units':'sum'})
end = timeit.default_timer()
BenchmarkResults.at[52, 'TimeInSeconds'] = end - start
BenchmarkResults.to_csv(f'{Path}BenchmarkResultsPandas.csv')
del BenchmarkResults, end, start
gc.collect()

## 1B 2N 1D 3G
BenchmarkResults = pd.read_csv(f'{Path}BenchmarkResultsPandas.csv')
start = timeit.default_timer()
data.groupby(['Date','Customer','Brand','Category'], as_index=False, sort=False, observed=True, dropna=True).agg({'Daily Liters':'sum', 'Daily Units':'sum'})
end = timeit.default_timer()
BenchmarkResults.at[53, 'TimeInSeconds'] = end - start
BenchmarkResults.to_csv(f'{Path}BenchmarkResultsPandas.csv')
del BenchmarkResults, end, start
gc.collect()

## 1B 2N 1D 4G
BenchmarkResults = pd.read_csv(f'{Path}BenchmarkResultsPandas.csv')
start = timeit.default_timer()
data.groupby(['Date','Customer','Brand','Category','Beverage Flavor'], as_index=False, sort=False, observed=True, dropna=True).agg({'Daily Liters':'sum', 'Daily Units':'sum'})
end = timeit.default_timer()
BenchmarkResults.at[54, 'TimeInSeconds'] = end - start
BenchmarkResults.to_csv(f'{Path}BenchmarkResultsPandas.csv')
del BenchmarkResults, end, start
gc.collect()

## 1B 3N 1D 0G
BenchmarkResults = pd.read_csv(f'{Path}BenchmarkResultsPandas.csv')
start = timeit.default_timer()
data.groupby('Date', as_index=False, sort=False, observed=True, dropna=True).agg({'Daily Liters':'sum', 'Daily Units':'sum', 'Daily Margin':'sum'})
end = timeit.default_timer()
BenchmarkResults.at[55, 'TimeInSeconds'] = end - start
BenchmarkResults.to_csv(f'{Path}BenchmarkResultsPandas.csv')
del BenchmarkResults, end, start
gc.collect()

## 1B 3N 1D 1G
BenchmarkResults = pd.read_csv(f'{Path}BenchmarkResultsPandas.csv')
start = timeit.default_timer()
data.groupby(['Date','Customer'], as_index=False, sort=False, observed=True, dropna=True).agg({'Daily Liters':'sum', 'Daily Units':'sum', 'Daily Margin':'sum'})
end = timeit.default_timer()
BenchmarkResults.at[56, 'TimeInSeconds'] = end - start
BenchmarkResults.to_csv(f'{Path}BenchmarkResultsPandas.csv')
del BenchmarkResults, end, start
gc.collect()

## 1B 3N 1D 2G
BenchmarkResults = pd.read_csv(f'{Path}BenchmarkResultsPandas.csv')
start = timeit.default_timer()
data.groupby(['Date','Customer','Brand'], as_index=False, sort=False, observed=True, dropna=True).agg({'Daily Liters':'sum', 'Daily Units':'sum', 'Daily Margin':'sum'})
end = timeit.default_timer()
BenchmarkResults.at[57, 'TimeInSeconds'] = end - start
BenchmarkResults.to_csv(f'{Path}BenchmarkResultsPandas.csv')
del BenchmarkResults, end, start
gc.collect()

## 1B 3N 1D 3G
BenchmarkResults = pd.read_csv(f'{Path}BenchmarkResultsPandas.csv')
start = timeit.default_timer()
data.groupby(['Date','Customer','Brand','Category'], as_index=False, sort=False, observed=True, dropna=True).agg({'Daily Liters':'sum', 'Daily Units':'sum', 'Daily Margin':'sum'})
end = timeit.default_timer()
BenchmarkResults.at[58, 'TimeInSeconds'] = end - start
BenchmarkResults.to_csv(f'{Path}BenchmarkResultsPandas.csv')
del BenchmarkResults, end, start
gc.collect()

## 1B 3N 1D 4G
BenchmarkResults = pd.read_csv(f'{Path}BenchmarkResultsPandas.csv')
start = timeit.default_timer()
data.groupby(['Date','Customer','Brand','Category','Beverage Flavor'], as_index=False, sort=False, observed=True, dropna=True).agg({'Daily Liters':'sum', 'Daily Units':'sum', 'Daily Margin':'sum'})
end = timeit.default_timer()
BenchmarkResults.at[59, 'TimeInSeconds'] = end - start
BenchmarkResults.to_csv(f'{Path}BenchmarkResultsPandas.csv')
del data, BenchmarkResults, end, start
gc.collect()

BenchmarkResults = pd.read_csv(f'{Path}BenchmarkResultsPandas.csv')
x = BenchmarkResults.iloc[0:60, :]
y = x['TimeInSeconds'].sum()
BenchmarkResults.at[60, 'TimeInSeconds'] = y
BenchmarkResults.to_csv(f'{Path}BenchmarkResultsPandas.csv')

