import polars as pl
import timeit
import gc

# Path to source data
Path = "C:/Users/Bizon/Documents/GitHub/rappwd/"

# Create results table
BenchmarkResults = {
  'Framework': ['polars']*3, 
  'Method': ['sum aggregation'] * 3,
  'Experiment': [
    '1M 3N 1D 4G',
    '10M 3N 1D 4G',
    '100M 3N 1D 4G'
  ],
  'TimeInSeconds': [-0.1]*3
}
BenchmarkResults = pl.DataFrame(BenchmarkResults)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars.csv')
del BenchmarkResults
gc.collect()

# Aggregation 1M

# Sum 1 Numeric Variable:

## 1M 1N 1D 0G
data = pl.read_csv(f'{Path}FakeBevData1M.csv', rechunk=True)

## 1M 3N 1D 4G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars.csv')
start = timeit.default_timer()
data.group_by(['Date','Customer','Brand','Category','Beverage Flavor']).agg(pl.sum('Daily Liters'),pl.sum('Daily Units'),pl.sum('Daily Margin'))
end = timeit.default_timer()
BenchmarkResults[0, 'TimeInSeconds'] = end - start
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
data = pl.read_csv(f'{Path}FakeBevData10M.csv', rechunk=True)


## 10M 3N 1D 4G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars.csv')
start = timeit.default_timer()
data.group_by(['Date','Customer','Brand','Category','Beverage Flavor']).agg(pl.sum('Daily Liters'),pl.sum('Daily Units'),pl.sum('Daily Margin'))
end = timeit.default_timer()
BenchmarkResults[1, 'TimeInSeconds'] = end - start
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
data = pl.read_csv(f'{Path}FakeBevData100M.csv', rechunk=True)


## 100M 3N 1D 4G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars.csv')
start = timeit.default_timer()
data.group_by(['Date','Customer','Brand','Category','Beverage Flavor']).agg(pl.sum('Daily Liters'),pl.sum('Daily Units'),pl.sum('Daily Margin'))
end = timeit.default_timer()
BenchmarkResults[2, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars.csv')
del data, BenchmarkResults, end, start
gc.collect()
