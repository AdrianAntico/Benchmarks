import polars as pl
import statistics as stats
import timeit
import gc

# Path to source data
Path = "C:/Users/Bizon/Documents/GitHub/rappwd/"

# Create results table
BenchmarkResults = {
  'Framework': ['polars']*3,
  'Method': ['unpivot'] * 3,
  'Experiment': [
    '1M 4N 1D 4G',
    '10M 4N 1D 4G',
    '100M 4N 1D 4G'
  ],
  'TimeInSeconds': [-0.1]*3
}
BenchmarkResults = pl.DataFrame(BenchmarkResults)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars_Melt.csv')
del BenchmarkResults
gc.collect()

# Aggregation 1M

# unpivot Numeric Variable:


## 1M 4N 1D 4G
data = pl.read_csv(f'{Path}FakeBevData1M.csv', rechunk=True)
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars_Melt.csv')
rts = [1.1]*3
for i in range(0,3):
  start = timeit.default_timer()
  data.unpivot(index = ['Date','Customer','Brand','Category','Beverage Flavor'], on = ['Daily Liters','Daily Units','Daily Margin','Daily Revenue'])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[0, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars_Melt.csv')
del data, BenchmarkResults, end, start, rts
gc.collect()

###################################################################################################
###################################################################################################
###################################################################################################
###################################################################################################
###################################################################################################

# Aggregation 10M

# unpivot Numeric Variable:


## 10M 4N 1D 4G
data = pl.read_csv(f'{Path}FakeBevData10M.csv', rechunk=True)
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars_Melt.csv')
rts = [1.1]*3
for i in range(0,3):
  start = timeit.default_timer()
  data.unpivot(index = ['Date','Customer','Brand','Category','Beverage Flavor'], on = ['Daily Liters','Daily Units','Daily Margin','Daily Revenue'])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[1, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars_Melt.csv')
del data, BenchmarkResults, end, start, rts
gc.collect()


###################################################################################################
###################################################################################################
###################################################################################################
###################################################################################################
###################################################################################################

# Aggregation 100M

# unpivot Numeric Variable:


## 100M 4N 1D 4G
data = pl.read_csv(f'{Path}FakeBevData100M.csv', rechunk=True)
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars_Melt.csv')
rts = [1.1]*3
for i in range(0,3):
  start = timeit.default_timer()
  data.unpivot(index = ['Date','Customer','Brand','Category','Beverage Flavor'], on = ['Daily Liters','Daily Units','Daily Margin','Daily Revenue'])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[2, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars_Melt.csv')
del data, BenchmarkResults, end, start, rts
gc.collect()
