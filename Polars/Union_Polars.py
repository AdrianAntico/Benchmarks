import polars as pl
import statistics as stats
import timeit
import gc

# Path to source data
Path = "C:/Users/Bizon/Documents/GitHub/rappwd/"

# Create results table
BenchmarkResults = {
  'Framework': ['polars']*3, 
  'Method': ['union'] * 3,
  'Experiment': [
    '1M 3N 1D 4G',
    '10M 3N 1D 4G',
    '100M 3N 1D 4G'
  ],
  'TimeInSeconds': [-0.1]*3
}
BenchmarkResults = pl.DataFrame(BenchmarkResults)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars_Union.csv')
del BenchmarkResults
gc.collect()

# Aggregation 1M

# Sum 1 Numeric Variable:

## 1M 1N 1D 0G
data = pl.read_csv(f'{Path}FakeBevData1M.csv', rechunk=True)


## 1M 3N 1D 4G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars_Union.csv')
cols = ['Date', 'Customer', 'Brand', 'Category', 'Beverage Flavor', 'Daily Liters', 'Daily Units', 'Daily Margin']
temp = data.select(cols)
rts = [1.1]*3
for i in range(0,3):
  print(i)
  start = timeit.default_timer()
  pl.concat([temp, temp], rechunk = True)
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[0, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars_Union.csv')
del data, BenchmarkResults, end, start, rts
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


## 1M 3N 1D 4G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars_Union.csv')
cols = ['Date', 'Customer', 'Brand', 'Category', 'Beverage Flavor', 'Daily Liters', 'Daily Units', 'Daily Margin']
temp = data.select(cols)
rts = [1.1]*3
for i in range(0,3):
  print(i)
  start = timeit.default_timer()
  pl.concat([temp, temp], rechunk = True)
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[1, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars_Union.csv')
del data, BenchmarkResults, end, start, rts
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


## 1M 3N 1D 4G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars_Union.csv')
cols = ['Date', 'Customer', 'Brand', 'Category', 'Beverage Flavor', 'Daily Liters', 'Daily Units', 'Daily Margin']
temp = data.select(cols)
rts = [1.1]*3
for i in range(0,3):
  print(i)
  start = timeit.default_timer()
  pl.concat([temp, temp], rechunk = True)
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[2, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars_Union.csv')
del data, BenchmarkResults, end, start, rts
gc.collect()
