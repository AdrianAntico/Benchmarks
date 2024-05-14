import polars as pl
import timeit
import gc

# Path to source data
Path = "C:/Users/Bizon/Documents/GitHub/rappwd/"

# Create results table
BenchmarkResults = {
  'Framework': ['polars']*61, 
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
  'TimeInSeconds': [-1.1]*61
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
del BenchmarkResults, end, start
gc.collect()

## 1M 1N 1D 1G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars.csv')
start = timeit.default_timer()
data.group_by(['Date','Customer']).agg(pl.sum('Daily Liters'))
end = timeit.default_timer()
BenchmarkResults[1, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 1N 1D 2G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars.csv')
start = timeit.default_timer()
data.group_by(['Date','Customer','Brand']).agg(pl.sum('Daily Liters'))
end = timeit.default_timer()
BenchmarkResults[2, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 1N 1D 3G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars.csv')
start = timeit.default_timer()
data.group_by(['Date','Customer','Brand','Category']).agg(pl.sum('Daily Liters'))
end = timeit.default_timer()
BenchmarkResults[3, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 1N 1D 4G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars.csv')
start = timeit.default_timer()
data.group_by(['Date','Customer','Brand','Category','Beverage Flavor']).agg(pl.sum('Daily Liters'))
end = timeit.default_timer()
BenchmarkResults[4, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 2N 1D 0G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars.csv')
start = timeit.default_timer()
data.group_by('Date').agg(pl.sum('Daily Liters'),pl.sum('Daily Units'))
end = timeit.default_timer()
BenchmarkResults[5, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 2N 1D 1G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars.csv')
start = timeit.default_timer()
data.group_by(['Date','Customer']).agg(pl.sum('Daily Liters'),pl.sum('Daily Units'))
end = timeit.default_timer()
BenchmarkResults[6, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 2N 1D 2G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars.csv')
start = timeit.default_timer()
data.group_by(['Date','Customer','Brand']).agg(pl.sum('Daily Liters'),pl.sum('Daily Units'))
end = timeit.default_timer()
BenchmarkResults[7, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 2N 1D 3G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars.csv')
start = timeit.default_timer()
data.group_by(['Date','Customer','Brand','Category']).agg(pl.sum('Daily Liters'),pl.sum('Daily Units'))
end = timeit.default_timer()
BenchmarkResults[8, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 2N 1D 4G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars.csv')
start = timeit.default_timer()
data.group_by(['Date','Customer','Brand','Category','Beverage Flavor']).agg(pl.sum('Daily Liters'),pl.sum('Daily Units'))
end = timeit.default_timer()
BenchmarkResults[9, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 3N 1D 0G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars.csv')
start = timeit.default_timer()
data.group_by('Date').agg(pl.sum('Daily Liters'),pl.sum('Daily Units'),pl.sum('Daily Margin'))
end = timeit.default_timer()
BenchmarkResults[10, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 3N 1D 1G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars.csv')
start = timeit.default_timer()
data.group_by(['Date','Customer']).agg(pl.sum('Daily Liters'),pl.sum('Daily Units'),pl.sum('Daily Margin'))
end = timeit.default_timer()
BenchmarkResults[11, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 3N 1D 2G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars.csv')
start = timeit.default_timer()
data.group_by(['Date','Customer','Brand']).agg(pl.sum('Daily Liters'),pl.sum('Daily Units'),pl.sum('Daily Margin'))
end = timeit.default_timer()
BenchmarkResults[12, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 3N 1D 3G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars.csv')
start = timeit.default_timer()
data.group_by(['Date','Customer','Brand','Category']).agg(pl.sum('Daily Liters'),pl.sum('Daily Units'),pl.sum('Daily Margin'))
end = timeit.default_timer()
BenchmarkResults[13, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 3N 1D 4G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars.csv')
start = timeit.default_timer()
data.group_by(['Date','Customer','Brand','Category','Beverage Flavor']).agg(pl.sum('Daily Liters'),pl.sum('Daily Units'),pl.sum('Daily Margin'))
end = timeit.default_timer()
BenchmarkResults[14, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars.csv')
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
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars.csv')
data = data.with_columns(pl.col('Date').str.to_date('%Y-%m-%d'))
start = timeit.default_timer()
data.group_by('Date').agg(pl.sum('Daily Liters'))
end = timeit.default_timer()
BenchmarkResults[15, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars.csv')
del BenchmarkResults, end, start


## 10M 1N 1D 1G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars.csv')
start = timeit.default_timer()
data.group_by(['Date','Customer']).agg(pl.sum('Daily Liters'))
end = timeit.default_timer()
BenchmarkResults[16, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars.csv')
del BenchmarkResults, end, start
gc.collect()

## 10M 1N 1D 2G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars.csv')
start = timeit.default_timer()
data.group_by(['Date','Customer','Brand']).agg(pl.sum('Daily Liters'))
end = timeit.default_timer()
BenchmarkResults[17, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars.csv')
del BenchmarkResults, end, start
gc.collect()

## 10M 1N 1D 3G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars.csv')
start = timeit.default_timer()
data.group_by(['Date','Customer','Brand','Category']).agg(pl.sum('Daily Liters'))
end = timeit.default_timer()
BenchmarkResults[18, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars.csv')
del BenchmarkResults, end, start
gc.collect()

## 10M 1N 1D 4G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars.csv')
start = timeit.default_timer()
data.group_by(['Date','Customer','Brand','Category','Beverage Flavor']).agg(pl.sum('Daily Liters'))
end = timeit.default_timer()
BenchmarkResults[19, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars.csv')
del BenchmarkResults, end, start
gc.collect()

## 10M 2N 1D 0G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars.csv')
start = timeit.default_timer()
data.group_by('Date').agg(pl.sum('Daily Liters'),pl.sum('Daily Units'))
end = timeit.default_timer()
BenchmarkResults[20, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars.csv')
del BenchmarkResults, end, start
gc.collect()

## 10M 2N 1D 1G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars.csv')
start = timeit.default_timer()
data.group_by(['Date','Customer']).agg(pl.sum('Daily Liters'),pl.sum('Daily Units'))
end = timeit.default_timer()
BenchmarkResults[21, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars.csv')
del BenchmarkResults, end, start
gc.collect()

## 10M 2N 1D 2G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars.csv')
start = timeit.default_timer()
data.group_by(['Date','Customer','Brand']).agg(pl.sum('Daily Liters'),pl.sum('Daily Units'))
end = timeit.default_timer()
BenchmarkResults[22, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars.csv')
del BenchmarkResults, end, start
gc.collect()

## 10M 2N 1D 3G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars.csv')
start = timeit.default_timer()
data.group_by(['Date','Customer','Brand','Category']).agg(pl.sum('Daily Liters'),pl.sum('Daily Units'))
end = timeit.default_timer()
BenchmarkResults[23, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars.csv')
del BenchmarkResults, end, start
gc.collect()

## 10M 2N 1D 4G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars.csv')
start = timeit.default_timer()
data.group_by(['Date','Customer','Brand','Category','Beverage Flavor']).agg(pl.sum('Daily Liters'),pl.sum('Daily Units'))
end = timeit.default_timer()
BenchmarkResults[24, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars.csv')
del BenchmarkResults, end, start
gc.collect()

## 10M 3N 1D 0G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars.csv')
start = timeit.default_timer()
data.group_by('Date').agg(pl.sum('Daily Liters'),pl.sum('Daily Units'),pl.sum('Daily Margin'))
end = timeit.default_timer()
BenchmarkResults[25, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars.csv')
del BenchmarkResults, end, start
gc.collect()

## 10M 3N 1D 1G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars.csv')
start = timeit.default_timer()
data.group_by(['Date','Customer']).agg(pl.sum('Daily Liters'),pl.sum('Daily Units'),pl.sum('Daily Margin'))
end = timeit.default_timer()
BenchmarkResults[26, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars.csv')
del BenchmarkResults, end, start
gc.collect()

## 10M 3N 1D 2G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars.csv')
start = timeit.default_timer()
data.group_by(['Date','Customer','Brand']).agg(pl.sum('Daily Liters'),pl.sum('Daily Units'),pl.sum('Daily Margin'))
end = timeit.default_timer()
BenchmarkResults[27, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars.csv')
del BenchmarkResults, end, start
gc.collect()

## 10M 3N 1D 3G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars.csv')
start = timeit.default_timer()
data.group_by(['Date','Customer','Brand','Category']).agg(pl.sum('Daily Liters'),pl.sum('Daily Units'),pl.sum('Daily Margin'))
end = timeit.default_timer()
BenchmarkResults[28, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars.csv')
del BenchmarkResults, end, start
gc.collect()

## 10M 3N 1D 4G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars.csv')
start = timeit.default_timer()
data.group_by(['Date','Customer','Brand','Category','Beverage Flavor']).agg(pl.sum('Daily Liters'),pl.sum('Daily Units'),pl.sum('Daily Margin'))
end = timeit.default_timer()
BenchmarkResults[29, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars.csv')
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
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars.csv')
data = data.with_columns(pl.col('Date').str.to_date('%Y-%m-%d'))
start = timeit.default_timer()
data.group_by('Date').agg(pl.sum('Daily Liters'))
end = timeit.default_timer()
BenchmarkResults[30, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars.csv')
del BenchmarkResults, end, start
gc.collect()

## 100M 1N 1D 1G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars.csv')
start = timeit.default_timer()
data.group_by(['Date','Customer']).agg(pl.sum('Daily Liters'))
end = timeit.default_timer()
BenchmarkResults[31, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars.csv')
del BenchmarkResults, end, start
gc.collect()

## 100M 1N 1D 2G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars.csv')
start = timeit.default_timer()
data.group_by(['Date','Customer','Brand']).agg(pl.sum('Daily Liters'))
end = timeit.default_timer()
BenchmarkResults[32, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars.csv')
del BenchmarkResults, end, start
gc.collect()

## 100M 1N 1D 3G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars.csv')
start = timeit.default_timer()
data.group_by(['Date','Customer','Brand','Category']).agg(pl.sum('Daily Liters'))
end = timeit.default_timer()
BenchmarkResults[33, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars.csv')
del BenchmarkResults, end, start
gc.collect()

## 100M 1N 1D 4G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars.csv')
start = timeit.default_timer()
data.group_by(['Date','Customer','Brand','Category','Beverage Flavor']).agg(pl.sum('Daily Liters'))
end = timeit.default_timer()
BenchmarkResults[34, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars.csv')
del BenchmarkResults, end, start
gc.collect()

## 100M 2N 1D 0G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars.csv')
start = timeit.default_timer()
data.group_by('Date').agg(pl.sum('Daily Liters'),pl.sum('Daily Units'))
end = timeit.default_timer()
BenchmarkResults[35, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars.csv')
del BenchmarkResults, end, start
gc.collect()

## 100M 2N 1D 1G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars.csv')
start = timeit.default_timer()
data.group_by(['Date','Customer']).agg(pl.sum('Daily Liters'),pl.sum('Daily Units'))
end = timeit.default_timer()
BenchmarkResults[36, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars.csv')
del BenchmarkResults, end, start
gc.collect()

## 100M 2N 1D 2G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars.csv')
start = timeit.default_timer()
data.group_by(['Date','Customer','Brand']).agg(pl.sum('Daily Liters'),pl.sum('Daily Units'))
end = timeit.default_timer()
BenchmarkResults[37, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars.csv')
del BenchmarkResults, end, start
gc.collect()

## 100M 2N 1D 3G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars.csv')
start = timeit.default_timer()
data.group_by(['Date','Customer','Brand','Category']).agg(pl.sum('Daily Liters'),pl.sum('Daily Units'))
end = timeit.default_timer()
BenchmarkResults[38, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars.csv')
del BenchmarkResults, end, start
gc.collect()

## 100M 2N 1D 4G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars.csv')
start = timeit.default_timer()
data.group_by(['Date','Customer','Brand','Category','Beverage Flavor']).agg(pl.sum('Daily Liters'),pl.sum('Daily Units'))
end = timeit.default_timer()
BenchmarkResults[39, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars.csv')
del BenchmarkResults, end, start
gc.collect()

## 100M 3N 1D 0G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars.csv')
start = timeit.default_timer()
data.group_by('Date').agg(pl.sum('Daily Liters'),pl.sum('Daily Units'),pl.sum('Daily Margin'))
end = timeit.default_timer()
BenchmarkResults[40, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars.csv')
del BenchmarkResults, end, start
gc.collect()

## 100M 3N 1D 1G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars.csv')
start = timeit.default_timer()
data.group_by(['Date','Customer']).agg(pl.sum('Daily Liters'),pl.sum('Daily Units'),pl.sum('Daily Margin'))
end = timeit.default_timer()
BenchmarkResults[41, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars.csv')
del BenchmarkResults, end, start
gc.collect()

## 100M 3N 1D 2G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars.csv')
start = timeit.default_timer()
data.group_by(['Date','Customer','Brand']).agg(pl.sum('Daily Liters'),pl.sum('Daily Units'),pl.sum('Daily Margin'))
end = timeit.default_timer()
BenchmarkResults[42, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars.csv')
del BenchmarkResults, end, start
gc.collect()

## 100M 3N 1D 3G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars.csv')
start = timeit.default_timer()
data.group_by(['Date','Customer','Brand','Category']).agg(pl.sum('Daily Liters'),pl.sum('Daily Units'),pl.sum('Daily Margin'))
end = timeit.default_timer()
BenchmarkResults[43, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars.csv')
del BenchmarkResults, end, start
gc.collect()

## 100M 3N 1D 4G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars.csv')
start = timeit.default_timer()
data.group_by(['Date','Customer','Brand','Category','Beverage Flavor']).agg(pl.sum('Daily Liters'),pl.sum('Daily Units'),pl.sum('Daily Margin'))
end = timeit.default_timer()
BenchmarkResults[44, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars.csv')
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
data = pl.read_csv(f'{Path}FakeBevData1B.csv')
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars.csv')
data = data.with_columns(pl.col('Date').str.to_date('%Y-%m-%d'))
start = timeit.default_timer()
data.group_by('Date').agg(pl.sum('Daily Liters'))
end = timeit.default_timer()
BenchmarkResults[45, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars.csv')
del BenchmarkResults, end, start
gc.collect()

## 1B 1N 1D 1G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars.csv')
start = timeit.default_timer()
data.group_by(['Date','Customer']).agg(pl.sum('Daily Liters'))
end = timeit.default_timer()
BenchmarkResults[46, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars.csv')
del BenchmarkResults, end, start
gc.collect()

## 1B 1N 1D 2G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars.csv')
start = timeit.default_timer()
data.group_by(['Date','Customer','Brand']).agg(pl.sum('Daily Liters'))
end = timeit.default_timer()
BenchmarkResults[47, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars.csv')
del BenchmarkResults, end, start
gc.collect()

## 1B 1N 1D 3G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars.csv')
start = timeit.default_timer()
data.group_by(['Date','Customer','Brand','Category']).agg(pl.sum('Daily Liters'))
end = timeit.default_timer()
BenchmarkResults[48, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars.csv')
del BenchmarkResults, end, start
gc.collect()

## 1B 1N 1D 4G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars.csv')
start = timeit.default_timer()
data.group_by(['Date','Customer','Brand','Category','Beverage Flavor']).agg(pl.sum('Daily Liters'))
end = timeit.default_timer()
BenchmarkResults[49, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars.csv')
del BenchmarkResults, end, start
gc.collect()

## 1B 2N 1D 0G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars.csv')
start = timeit.default_timer()
data.group_by('Date').agg(pl.sum('Daily Liters'),pl.sum('Daily Units'))
end = timeit.default_timer()
BenchmarkResults[50, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars.csv')
del BenchmarkResults, end, start
gc.collect()

## 1B 2N 1D 1G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars.csv')
start = timeit.default_timer()
data.group_by(['Date','Customer']).agg(pl.sum('Daily Liters'),pl.sum('Daily Units'))
end = timeit.default_timer()
BenchmarkResults[51, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars.csv')
del BenchmarkResults, end, start
gc.collect()

## 1B 2N 1D 2G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars.csv')
start = timeit.default_timer()
data.group_by(['Date','Customer','Brand']).agg(pl.sum('Daily Liters'),pl.sum('Daily Units'))
end = timeit.default_timer()
BenchmarkResults[52, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars.csv')
del BenchmarkResults, end, start
gc.collect()

## 1B 2N 1D 3G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars.csv')
start = timeit.default_timer()
data.group_by(['Date','Customer','Brand','Category']).agg(pl.sum('Daily Liters'),pl.sum('Daily Units'))
end = timeit.default_timer()
BenchmarkResults[53, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars.csv')
del BenchmarkResults, end, start
gc.collect()

## 1B 2N 1D 4G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars.csv')
start = timeit.default_timer()
data.group_by(['Date','Customer','Brand','Category','Beverage Flavor']).agg(pl.sum('Daily Liters'),pl.sum('Daily Units'))
end = timeit.default_timer()
BenchmarkResults[54, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars.csv')
del BenchmarkResults, end, start
gc.collect()

## 1B 3N 1D 0G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars.csv')
start = timeit.default_timer()
data.group_by('Date').agg(pl.sum('Daily Liters'),pl.sum('Daily Units'),pl.sum('Daily Margin'))
end = timeit.default_timer()
BenchmarkResults[55, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars.csv')
del BenchmarkResults, end, start
gc.collect()

## 1B 3N 1D 1G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars.csv')
start = timeit.default_timer()
data.group_by(['Date','Customer']).agg(pl.sum('Daily Liters'),pl.sum('Daily Units'),pl.sum('Daily Margin'))
end = timeit.default_timer()
BenchmarkResults[56, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars.csv')
del BenchmarkResults, end, start
gc.collect()

## 1B 3N 1D 2G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars.csv')
start = timeit.default_timer()
data.group_by(['Date','Customer','Brand']).agg(pl.sum('Daily Liters'),pl.sum('Daily Units'),pl.sum('Daily Margin'))
end = timeit.default_timer()
BenchmarkResults[57, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars.csv')
del BenchmarkResults, end, start
gc.collect()

## 1B 3N 1D 3G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars.csv')
start = timeit.default_timer()
data.group_by(['Date','Customer','Brand','Category']).agg(pl.sum('Daily Liters'),pl.sum('Daily Units'),pl.sum('Daily Margin'))
end = timeit.default_timer()
BenchmarkResults[58, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars.csv')
del BenchmarkResults, end, start
gc.collect()

## 1B 3N 1D 4G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars.csv')
start = timeit.default_timer()
data.group_by(['Date','Customer','Brand','Category','Beverage Flavor']).agg(pl.sum('Daily Liters'),pl.sum('Daily Units'),pl.sum('Daily Margin'))
end = timeit.default_timer()
BenchmarkResults[59, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars.csv')
del data, BenchmarkResults, end, start
gc.collect()

BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars.csv')
x = BenchmarkResults[0:59]
y = x['TimeInSeconds'].sum()
BenchmarkResults[60, 'TimeInSeconds'] = y
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars.csv')

