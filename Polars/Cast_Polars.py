import polars as pl
import statistics as stats
import timeit
import gc

# Path to source data
Path = "C:/Users/Bizon/Documents/GitHub/rappwd/"

# Create results table
BenchmarkResults = {
  'Framework': ['polars']*3,
  'Method': ['cast'] * 3,
  'Experiment': [
    '1M 4N 1D 4G',
    '10M 4N 1D 4G',
    '100M 4N 1D 4G'
  ],
  'TimeInSeconds': [-0.1]*3
}
BenchmarkResults = pl.DataFrame(BenchmarkResults)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars_Cast.csv')
del BenchmarkResults
gc.collect()

# Aggregatiindex 1M

# Melt Numeric Variable:

## 1M 2N 1D 0G
data = pl.read_csv(f'{Path}FakeBevData1M.csv', rechunk=True)
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars_Cast.csv')
data = data.with_columns(pl.col('Date').str.to_date('%Y-%m-%d'))
vals = [f"Location {i}" for i in range(1,44)]
temp = data.unpivot(index = ['Date','Customer','Brand','Category','Beverage Flavor'], on = ['Daily Liters','Daily Units','Daily Margin','Daily Revenue'])
temp = temp.group_by(['Date','Customer','Brand','Category','Beverage Flavor','variable']).agg(pl.sum('value'))
temp = temp.filter(pl.col('Customer').is_in(vals))


## 1M 2N 1D 4G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars_Cast.csv')
rts = [1.1]*3
for i in range(0,3):
  print(i)
  start = timeit.default_timer()
  x = temp.pivot(index = ["Date","Customer","Brand","Category","Beverage Flavor"], on = "variable", values = "value", aggregate_function = "sum").sort("Date")
  x = x.fill_null(0)
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[0, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars_Cast.csv')
del BenchmarkResults, end, start, x, rts
gc.collect()

###################################################################################################
###################################################################################################
###################################################################################################
###################################################################################################
###################################################################################################

# Aggregatiindex 10M

# Melt Numeric Variable:

## 10M 2N 1D 0G
data = pl.read_csv(f'{Path}FakeBevData10M.csv', rechunk=True)
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars_Cast.csv')
data = data.with_columns(pl.col('Date').str.to_date('%Y-%m-%d'))
vals = [f"Location {i}" for i in range(1,483)]
temp = data.unpivot(index = ['Date','Customer','Brand','Category','Beverage Flavor'], on = ['Daily Liters','Daily Units','Daily Margin','Daily Revenue'])
temp = temp.group_by(['Date','Customer','Brand','Category','Beverage Flavor','variable']).agg(pl.sum('value'))
temp = temp.filter(pl.col('Customer').is_in(vals))

## 10M 2N 1D 4G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars_Cast.csv')
rts = [1.1]*3
for i in range(0,3):
  print(i)
  start = timeit.default_timer()
  x = temp.pivot(index = ["Date","Customer","Brand","Category","Beverage Flavor"], on = "variable", values = "value", aggregate_function = "sum").sort("Date")
  x = x.fill_null(0)
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[1, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars_Cast.csv')
del BenchmarkResults, end, start, x, rts
gc.collect()

###################################################################################################
###################################################################################################
###################################################################################################
###################################################################################################
###################################################################################################

# Aggregatiindex 100M

# Melt Numeric Variable:

## 100M 2N 1D 0G
data = pl.read_csv(f'{Path}FakeBevData100M.csv', rechunk=True)
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars_Cast.csv')
data = data.with_columns(pl.col('Date').str.to_date('%Y-%m-%d'))
vals = [f"Location {i}" for i in range(1,4882)]
temp = data.unpivot(index = ['Date','Customer','Brand','Category','Beverage Flavor'], on = ['Daily Liters','Daily Units','Daily Margin','Daily Revenue'])
temp = temp.group_by(['Date','Customer','Brand','Category','Beverage Flavor','variable']).agg(pl.sum('value'))
temp = temp.filter(pl.col('Customer').is_in(vals))

## 100M 2N 1D 4G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars_Cast.csv')
rts = [1.1]*3
for i in range(0,3):
  print(i)
  start = timeit.default_timer()
  x = temp.pivot(index = ["Date","Customer","Brand","Category","Beverage Flavor"], on = "variable", values = "value", aggregate_function = "sum").sort("Date")
  x = x.fill_null(0)
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[2, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars_Cast.csv')
del BenchmarkResults, end, start, x, rts
gc.collect()
