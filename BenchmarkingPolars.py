import polars as pl
import timeit
import gc

# Path to source data
Path = "C:/Users/Bizon/Documents/GitHub/rappwd/"

# Create results table
BenchmarkResults = {
  'Framework': ['polars']*46, 
  'Method': ['sum aggregation'] * 46,
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
  'TimeInSeconds': [-1.1]*46
}
BenchmarkResults = pl.DataFrame(BenchmarkResults)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars.csv')
del BenchmarkResults
gc.collect()

# Aggregation 1M

# Sum 1 Numeric Variable:

## 1M 1N 1D 0G
data = pl.read_csv(f'{Path}FakeBevData1M.csv')
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars.csv')
data = data.with_columns(pl.col('Date').str.to_date('%Y-%m-%d'))
start = timeit.default_timer()
data.group_by('Date').agg(pl.sum('Daily Liters'))
end = timeit.default_timer()
BenchmarkResults[0, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars.csv')
del data, BenchmarkResults, end, start


## 1M 1N 1D 1G
data = pl.read_csv(f'{Path}FakeBevData1M.csv')
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars.csv')
data = data.with_columns(pl.col('Date').str.to_date('%Y-%m-%d'))
start = timeit.default_timer()
data.group_by(['Date','Customer']).agg(pl.sum('Daily Liters'))
end = timeit.default_timer()
BenchmarkResults[1, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars.csv')
del data, BenchmarkResults, end, start


## 1M 1N 1D 2G
data = pl.read_csv(f'{Path}FakeBevData1M.csv')
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars.csv')
data = data.with_columns(pl.col('Date').str.to_date('%Y-%m-%d'))
start = timeit.default_timer()
data.group_by(['Date','Customer','Brand']).agg(pl.sum('Daily Liters'))
end = timeit.default_timer()
BenchmarkResults[2, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars.csv')
del data, BenchmarkResults, end, start


## 1M 1N 1D 3G
data = pl.read_csv(f'{Path}FakeBevData1M.csv')
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars.csv')
data = data.with_columns(pl.col('Date').str.to_date('%Y-%m-%d'))
start = timeit.default_timer()
data.group_by(['Date','Customer','Brand','Category']).agg(pl.sum('Daily Liters'))
end = timeit.default_timer()
BenchmarkResults[3, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars.csv')
del data, BenchmarkResults, end, start


## 1M 1N 1D 4G
data = pl.read_csv(f'{Path}FakeBevData1M.csv')
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars.csv')
data = data.with_columns(pl.col('Date').str.to_date('%Y-%m-%d'))
start = timeit.default_timer()
data.group_by(['Date','Customer','Brand','Category','Beverage Flavor']).agg(pl.sum('Daily Liters'))
end = timeit.default_timer()
BenchmarkResults[4, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars.csv')
del data, BenchmarkResults, end, start


## 1M 2N 1D 0G
data = pl.read_csv(f'{Path}FakeBevData1M.csv')
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars.csv')
data = data.with_columns(pl.col('Date').str.to_date('%Y-%m-%d'))
start = timeit.default_timer()
data.group_by('Date').agg(pl.sum('Daily Liters'),pl.sum('Daily Units'))
end = timeit.default_timer()
BenchmarkResults[5, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars.csv')
del data, BenchmarkResults, end, start


## 1M 2N 1D 1G
data = pl.read_csv(f'{Path}FakeBevData1M.csv')
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars.csv')
data = data.with_columns(pl.col('Date').str.to_date('%Y-%m-%d'))
start = timeit.default_timer()
data.group_by(['Date','Customer']).agg(pl.sum('Daily Liters'),pl.sum('Daily Units'))
end = timeit.default_timer()
BenchmarkResults[6, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars.csv')
del data, BenchmarkResults, end, start


## 1M 2N 1D 2G
data = pl.read_csv(f'{Path}FakeBevData1M.csv')
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars.csv')
data = data.with_columns(pl.col('Date').str.to_date('%Y-%m-%d'))
start = timeit.default_timer()
data.group_by(['Date','Customer','Brand']).agg(pl.sum('Daily Liters'),pl.sum('Daily Units'))
end = timeit.default_timer()
BenchmarkResults[7, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars.csv')
del data, BenchmarkResults, end, start


## 1M 2N 1D 3G
data = pl.read_csv(f'{Path}FakeBevData1M.csv')
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars.csv')
data = data.with_columns(pl.col('Date').str.to_date('%Y-%m-%d'))
start = timeit.default_timer()
data.group_by(['Date','Customer','Brand','Category']).agg(pl.sum('Daily Liters'),pl.sum('Daily Units'))
end = timeit.default_timer()
BenchmarkResults[8, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars.csv')
del data, BenchmarkResults, end, start


## 1M 2N 1D 4G
data = pl.read_csv(f'{Path}FakeBevData1M.csv')
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars.csv')
data = data.with_columns(pl.col('Date').str.to_date('%Y-%m-%d'))
start = timeit.default_timer()
data.group_by(['Date','Customer','Brand','Category','Beverage Flavor']).agg(pl.sum('Daily Liters'),pl.sum('Daily Units'))
end = timeit.default_timer()
BenchmarkResults[9, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars.csv')
del data, BenchmarkResults, end, start


## 1M 3N 1D 0G
data = pl.read_csv(f'{Path}FakeBevData1M.csv')
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars.csv')
data = data.with_columns(pl.col('Date').str.to_date('%Y-%m-%d'))
start = timeit.default_timer()
data.group_by('Date').agg(pl.sum('Daily Liters'),pl.sum('Daily Units'),pl.sum('Daily Margin'))
end = timeit.default_timer()
BenchmarkResults[10, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars.csv')
del data, BenchmarkResults, end, start


## 1M 3N 1D 1G
data = pl.read_csv(f'{Path}FakeBevData1M.csv')
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars.csv')
data = data.with_columns(pl.col('Date').str.to_date('%Y-%m-%d'))
start = timeit.default_timer()
data.group_by(['Date','Customer']).agg(pl.sum('Daily Liters'),pl.sum('Daily Units'),pl.sum('Daily Margin'))
end = timeit.default_timer()
BenchmarkResults[11, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars.csv')
del data, BenchmarkResults, end, start


## 1M 3N 1D 2G
data = pl.read_csv(f'{Path}FakeBevData1M.csv')
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars.csv')
data = data.with_columns(pl.col('Date').str.to_date('%Y-%m-%d'))
start = timeit.default_timer()
data.group_by(['Date','Customer','Brand']).agg(pl.sum('Daily Liters'),pl.sum('Daily Units'),pl.sum('Daily Margin'))
end = timeit.default_timer()
BenchmarkResults[12, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars.csv')
del data, BenchmarkResults, end, start


## 1M 3N 1D 3G
data = pl.read_csv(f'{Path}FakeBevData1M.csv')
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars.csv')
data = data.with_columns(pl.col('Date').str.to_date('%Y-%m-%d'))
start = timeit.default_timer()
data.group_by(['Date','Customer','Brand','Category']).agg(pl.sum('Daily Liters'),pl.sum('Daily Units'),pl.sum('Daily Margin'))
end = timeit.default_timer()
BenchmarkResults[13, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars.csv')
del data, BenchmarkResults, end, start


## 1M 3N 1D 4G
data = pl.read_csv(f'{Path}FakeBevData1M.csv')
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars.csv')
data = data.with_columns(pl.col('Date').str.to_date('%Y-%m-%d'))
start = timeit.default_timer()
data.group_by(['Date','Customer','Brand','Category','Beverage Flavor']).agg(pl.sum('Daily Liters'),pl.sum('Daily Units'),pl.sum('Daily Margin'))
end = timeit.default_timer()
BenchmarkResults[14, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars.csv')
del data, BenchmarkResults, end, start


###################################################################################################
###################################################################################################
###################################################################################################
###################################################################################################
###################################################################################################

# Aggregation 10M

# Sum 1 Numeric Variable:

## 10M 1N 1D 0G
data = pl.read_csv(f'{Path}FakeBevData10M.csv')
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars.csv')
data = data.with_columns(pl.col('Date').str.to_date('%Y-%m-%d'))
start = timeit.default_timer()
data.group_by('Date').agg(pl.sum('Daily Liters'))
end = timeit.default_timer()
BenchmarkResults[15, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars.csv')
del data, BenchmarkResults, end, start


## 10M 1N 1D 1G
data = pl.read_csv(f'{Path}FakeBevData10M.csv')
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars.csv')
data = data.with_columns(pl.col('Date').str.to_date('%Y-%m-%d'))
start = timeit.default_timer()
data.group_by(['Date','Customer']).agg(pl.sum('Daily Liters'))
end = timeit.default_timer()
BenchmarkResults[16, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars.csv')
del data, BenchmarkResults, end, start


## 10M 1N 1D 2G
data = pl.read_csv(f'{Path}FakeBevData10M.csv')
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars.csv')
data = data.with_columns(pl.col('Date').str.to_date('%Y-%m-%d'))
start = timeit.default_timer()
data.group_by(['Date','Customer','Brand']).agg(pl.sum('Daily Liters'))
end = timeit.default_timer()
BenchmarkResults[17, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars.csv')
del data, BenchmarkResults, end, start


## 10M 1N 1D 3G
data = pl.read_csv(f'{Path}FakeBevData10M.csv')
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars.csv')
data = data.with_columns(pl.col('Date').str.to_date('%Y-%m-%d'))
start = timeit.default_timer()
data.group_by(['Date','Customer','Brand','Category']).agg(pl.sum('Daily Liters'))
end = timeit.default_timer()
BenchmarkResults[18, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars.csv')
del data, BenchmarkResults, end, start


## 10M 1N 1D 4G
data = pl.read_csv(f'{Path}FakeBevData10M.csv')
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars.csv')
data = data.with_columns(pl.col('Date').str.to_date('%Y-%m-%d'))
start = timeit.default_timer()
data.group_by(['Date','Customer','Brand','Category','Beverage Flavor']).agg(pl.sum('Daily Liters'))
end = timeit.default_timer()
BenchmarkResults[19, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars.csv')
del data, BenchmarkResults, end, start


## 10M 2N 1D 0G
data = pl.read_csv(f'{Path}FakeBevData10M.csv')
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars.csv')
data = data.with_columns(pl.col('Date').str.to_date('%Y-%m-%d'))
start = timeit.default_timer()
data.group_by('Date').agg(pl.sum('Daily Liters'),pl.sum('Daily Units'))
end = timeit.default_timer()
BenchmarkResults[20, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars.csv')
del data, BenchmarkResults, end, start


## 10M 2N 1D 1G
data = pl.read_csv(f'{Path}FakeBevData10M.csv')
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars.csv')
data = data.with_columns(pl.col('Date').str.to_date('%Y-%m-%d'))
start = timeit.default_timer()
data.group_by(['Date','Customer']).agg(pl.sum('Daily Liters'),pl.sum('Daily Units'))
end = timeit.default_timer()
BenchmarkResults[21, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars.csv')
del data, BenchmarkResults, end, start


## 10M 2N 1D 2G
data = pl.read_csv(f'{Path}FakeBevData10M.csv')
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars.csv')
data = data.with_columns(pl.col('Date').str.to_date('%Y-%m-%d'))
start = timeit.default_timer()
data.group_by(['Date','Customer','Brand']).agg(pl.sum('Daily Liters'),pl.sum('Daily Units'))
end = timeit.default_timer()
BenchmarkResults[22, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars.csv')
del data, BenchmarkResults, end, start


## 10M 2N 1D 3G
data = pl.read_csv(f'{Path}FakeBevData10M.csv')
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars.csv')
data = data.with_columns(pl.col('Date').str.to_date('%Y-%m-%d'))
start = timeit.default_timer()
data.group_by(['Date','Customer','Brand','Category']).agg(pl.sum('Daily Liters'),pl.sum('Daily Units'))
end = timeit.default_timer()
BenchmarkResults[23, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars.csv')
del data, BenchmarkResults, end, start


## 10M 2N 1D 4G
data = pl.read_csv(f'{Path}FakeBevData10M.csv')
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars.csv')
data = data.with_columns(pl.col('Date').str.to_date('%Y-%m-%d'))
start = timeit.default_timer()
data.group_by(['Date','Customer','Brand','Category','Beverage Flavor']).agg(pl.sum('Daily Liters'),pl.sum('Daily Units'))
end = timeit.default_timer()
BenchmarkResults[24, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars.csv')
del data, BenchmarkResults, end, start


## 10M 3N 1D 0G
data = pl.read_csv(f'{Path}FakeBevData10M.csv')
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars.csv')
data = data.with_columns(pl.col('Date').str.to_date('%Y-%m-%d'))
start = timeit.default_timer()
data.group_by('Date').agg(pl.sum('Daily Liters'),pl.sum('Daily Units'),pl.sum('Daily Margin'))
end = timeit.default_timer()
BenchmarkResults[25, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars.csv')
del data, BenchmarkResults, end, start


## 10M 3N 1D 1G
data = pl.read_csv(f'{Path}FakeBevData10M.csv')
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars.csv')
data = data.with_columns(pl.col('Date').str.to_date('%Y-%m-%d'))
start = timeit.default_timer()
data.group_by(['Date','Customer']).agg(pl.sum('Daily Liters'),pl.sum('Daily Units'),pl.sum('Daily Margin'))
end = timeit.default_timer()
BenchmarkResults[26, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars.csv')
del data, BenchmarkResults, end, start


## 10M 3N 1D 2G
data = pl.read_csv(f'{Path}FakeBevData10M.csv')
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars.csv')
data = data.with_columns(pl.col('Date').str.to_date('%Y-%m-%d'))
start = timeit.default_timer()
data.group_by(['Date','Customer','Brand']).agg(pl.sum('Daily Liters'),pl.sum('Daily Units'),pl.sum('Daily Margin'))
end = timeit.default_timer()
BenchmarkResults[27, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars.csv')
del data, BenchmarkResults, end, start


## 10M 3N 1D 3G
data = pl.read_csv(f'{Path}FakeBevData10M.csv')
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars.csv')
data = data.with_columns(pl.col('Date').str.to_date('%Y-%m-%d'))
start = timeit.default_timer()
data.group_by(['Date','Customer','Brand','Category']).agg(pl.sum('Daily Liters'),pl.sum('Daily Units'),pl.sum('Daily Margin'))
end = timeit.default_timer()
BenchmarkResults[28, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars.csv')
del data, BenchmarkResults, end, start


## 10M 3N 1D 4G
data = pl.read_csv(f'{Path}FakeBevData10M.csv')
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars.csv')
data = data.with_columns(pl.col('Date').str.to_date('%Y-%m-%d'))
start = timeit.default_timer()
data.group_by(['Date','Customer','Brand','Category','Beverage Flavor']).agg(pl.sum('Daily Liters'),pl.sum('Daily Units'),pl.sum('Daily Margin'))
end = timeit.default_timer()
BenchmarkResults[29, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars.csv')
del data, BenchmarkResults, end, start



###################################################################################################
###################################################################################################
###################################################################################################
###################################################################################################
###################################################################################################

# Aggregation 100M

# Sum 1 Numeric Variable:

## 100M 1N 1D 0G
data = pl.read_csv(f'{Path}FakeBevData100M.csv')
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars.csv')
data = data.with_columns(pl.col('Date').str.to_date('%Y-%m-%d'))
start = timeit.default_timer()
data.group_by('Date').agg(pl.sum('Daily Liters'))
end = timeit.default_timer()
BenchmarkResults[30, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars.csv')
del data, BenchmarkResults, end, start


## 100M 1N 1D 1G
data = pl.read_csv(f'{Path}FakeBevData100M.csv')
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars.csv')
data = data.with_columns(pl.col('Date').str.to_date('%Y-%m-%d'))
start = timeit.default_timer()
data.group_by(['Date','Customer']).agg(pl.sum('Daily Liters'))
end = timeit.default_timer()
BenchmarkResults[31, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars.csv')
del data, BenchmarkResults, end, start


## 100M 1N 1D 2G
data = pl.read_csv(f'{Path}FakeBevData100M.csv')
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars.csv')
data = data.with_columns(pl.col('Date').str.to_date('%Y-%m-%d'))
start = timeit.default_timer()
data.group_by(['Date','Customer','Brand']).agg(pl.sum('Daily Liters'))
end = timeit.default_timer()
BenchmarkResults[32, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars.csv')
del data, BenchmarkResults, end, start


## 100M 1N 1D 3G
data = pl.read_csv(f'{Path}FakeBevData100M.csv')
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars.csv')
data = data.with_columns(pl.col('Date').str.to_date('%Y-%m-%d'))
start = timeit.default_timer()
data.group_by(['Date','Customer','Brand','Category']).agg(pl.sum('Daily Liters'))
end = timeit.default_timer()
BenchmarkResults[33, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars.csv')
del data, BenchmarkResults, end, start


## 100M 1N 1D 4G
data = pl.read_csv(f'{Path}FakeBevData100M.csv')
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars.csv')
data = data.with_columns(pl.col('Date').str.to_date('%Y-%m-%d'))
start = timeit.default_timer()
data.group_by(['Date','Customer','Brand','Category','Beverage Flavor']).agg(pl.sum('Daily Liters'))
end = timeit.default_timer()
BenchmarkResults[34, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars.csv')
del data, BenchmarkResults, end, start


## 100M 2N 1D 0G
data = pl.read_csv(f'{Path}FakeBevData100M.csv')
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars.csv')
data = data.with_columns(pl.col('Date').str.to_date('%Y-%m-%d'))
start = timeit.default_timer()
data.group_by('Date').agg(pl.sum('Daily Liters'),pl.sum('Daily Units'))
end = timeit.default_timer()
BenchmarkResults[35, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars.csv')
del data, BenchmarkResults, end, start


## 100M 2N 1D 1G
data = pl.read_csv(f'{Path}FakeBevData100M.csv')
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars.csv')
data = data.with_columns(pl.col('Date').str.to_date('%Y-%m-%d'))
start = timeit.default_timer()
data.group_by(['Date','Customer']).agg(pl.sum('Daily Liters'),pl.sum('Daily Units'))
end = timeit.default_timer()
BenchmarkResults[36, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars.csv')
del data, BenchmarkResults, end, start


## 100M 2N 1D 2G
data = pl.read_csv(f'{Path}FakeBevData100M.csv')
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars.csv')
data = data.with_columns(pl.col('Date').str.to_date('%Y-%m-%d'))
start = timeit.default_timer()
data.group_by(['Date','Customer','Brand']).agg(pl.sum('Daily Liters'),pl.sum('Daily Units'))
end = timeit.default_timer()
BenchmarkResults[37, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars.csv')
del data, BenchmarkResults, end, start


## 100M 2N 1D 3G
data = pl.read_csv(f'{Path}FakeBevData100M.csv')
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars.csv')
data = data.with_columns(pl.col('Date').str.to_date('%Y-%m-%d'))
start = timeit.default_timer()
data.group_by(['Date','Customer','Brand','Category']).agg(pl.sum('Daily Liters'),pl.sum('Daily Units'))
end = timeit.default_timer()
BenchmarkResults[38, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars.csv')
del data, BenchmarkResults, end, start


## 100M 2N 1D 4G
data = pl.read_csv(f'{Path}FakeBevData100M.csv')
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars.csv')
data = data.with_columns(pl.col('Date').str.to_date('%Y-%m-%d'))
start = timeit.default_timer()
data.group_by(['Date','Customer','Brand','Category','Beverage Flavor']).agg(pl.sum('Daily Liters'),pl.sum('Daily Units'))
end = timeit.default_timer()
BenchmarkResults[39, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars.csv')
del data, BenchmarkResults, end, start


## 100M 3N 1D 0G
data = pl.read_csv(f'{Path}FakeBevData100M.csv')
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars.csv')
data = data.with_columns(pl.col('Date').str.to_date('%Y-%m-%d'))
start = timeit.default_timer()
data.group_by('Date').agg(pl.sum('Daily Liters'),pl.sum('Daily Units'),pl.sum('Daily Margin'))
end = timeit.default_timer()
BenchmarkResults[40, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars.csv')
del data, BenchmarkResults, end, start


## 100M 3N 1D 1G
data = pl.read_csv(f'{Path}FakeBevData100M.csv')
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars.csv')
data = data.with_columns(pl.col('Date').str.to_date('%Y-%m-%d'))
start = timeit.default_timer()
data.group_by(['Date','Customer']).agg(pl.sum('Daily Liters'),pl.sum('Daily Units'),pl.sum('Daily Margin'))
end = timeit.default_timer()
BenchmarkResults[41, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars.csv')
del data, BenchmarkResults, end, start


## 100M 3N 1D 2G
data = pl.read_csv(f'{Path}FakeBevData100M.csv')
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars.csv')
data = data.with_columns(pl.col('Date').str.to_date('%Y-%m-%d'))
start = timeit.default_timer()
data.group_by(['Date','Customer','Brand']).agg(pl.sum('Daily Liters'),pl.sum('Daily Units'),pl.sum('Daily Margin'))
end = timeit.default_timer()
BenchmarkResults[42, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars.csv')
del data, BenchmarkResults, end, start


## 100M 3N 1D 3G
data = pl.read_csv(f'{Path}FakeBevData100M.csv')
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars.csv')
data = data.with_columns(pl.col('Date').str.to_date('%Y-%m-%d'))
start = timeit.default_timer()
data.group_by(['Date','Customer','Brand','Category']).agg(pl.sum('Daily Liters'),pl.sum('Daily Units'),pl.sum('Daily Margin'))
end = timeit.default_timer()
BenchmarkResults[43, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars.csv')
del data, BenchmarkResults, end, start


## 100M 3N 1D 4G
data = pl.read_csv(f'{Path}FakeBevData100M.csv')
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars.csv')
data = data.with_columns(pl.col('Date').str.to_date('%Y-%m-%d'))
start = timeit.default_timer()
data.group_by(['Date','Customer','Brand','Category','Beverage Flavor']).agg(pl.sum('Daily Liters'),pl.sum('Daily Units'),pl.sum('Daily Margin'))
end = timeit.default_timer()
BenchmarkResults[44, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars.csv')
del data, BenchmarkResults, end, start


BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars.csv')
x = BenchmarkResults[0:44]
y = x['TimeInSeconds'].sum()
BenchmarkResults[45, 'TimeInSeconds'] = y
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars.csv')
 
 
 
dt = pl.read_csv(f'{Path}BenchmarkResultsPlot.csv')

from QuickEcharts import Charts
# dt = dt.sort('Experiment')

dt = dt.with_columns(dt['Time In Seconds'].round(3))

rows1M = [[x for x in range(0, 14)], [x for x in range(46, 60)], [x for x in range(92, 107)]]
rows1M_list = [number for sublist in rows1M for number in sublist]
dt1M = dt[rows1M_list]


Charts.Bar(
  dt = dt1M,
  PreAgg = True,
  YVar = 'Time In Seconds',
  XVar = 'Experiment',
  GroupVar = 'variable',
  FacetCols = 1,
  FacetRows = 1,
  FacetLevels = None,
  TimeLine = False,
  AggMethod = 'sum',
  YVarTrans = 'Identity',
  RenderHTML = True,
  ShowLabels = True,
  LabelPosition = 'top',
  Theme = 'dark',
  BackgroundColor = None,
  Width = None,
  Height = None,
  ToolBox = True,
  Brush = True,
  DataZoom = True,
  Title = 'Benchmark Results',
  TitleColor = 'white',
  TitleFontSize = 20,
  SubTitle = None,
  SubTitleColor = '#fff',
  SubTitleFontSize = 12,
  AxisPointerType = 'cross',
  YAxisTitle = 'Time In Seconds',
  YAxisNameLocation = 'middle',
  YAxisNameGap = 70,
  XAxisTitle = 'Experiment',
  XAxisNameLocation = 'middle',
  XAxisNameGap = 42,
  Legend = 'top',
  LegendPosRight = '0%',
  LegendPosTop = '5%',
  LegendBorderSize = 1,
  LegendTextColor = 'white',
  VerticalLine = None,
  VerticalLineName = 'Line Name',
  HorizontalLine = None,
  HorizontalLineName = 'Line Name',
  AnimationThreshold = 2000,
  AnimationDuration = 1000,
  AnimationEasing = 'cubicOut',
  AnimationDelay = 0,
  AnimationDurationUpdate = 300,
  AnimationEasingUpdate = 'cubicOut',
  AnimationDelayUpdate = 0)





