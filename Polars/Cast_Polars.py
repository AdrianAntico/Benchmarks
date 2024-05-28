import polars as pl
import statistics as stats
import timeit
import gc

# Path to source data
Path = "C:/Users/Bizon/Documents/GitHub/rappwd/"

# Create results table
BenchmarkResults = {
  'Framework': ['polars']*16,
  'Method': ['cast'] * 16,
  'Experiment': [
    '1M 2N 1D 0G',
    '1M 2N 1D 1G',
    '1M 2N 1D 2G',
    '1M 2N 1D 3G',
    '1M 2N 1D 4G',
    
    '10M 2N 1D 0G',
    '10M 2N 1D 1G',
    '10M 2N 1D 2G',
    '10M 2N 1D 3G',
    '10M 2N 1D 4G',
    
    '100M 2N 1D 0G',
    '100M 2N 1D 1G',
    '100M 2N 1D 2G',
    '100M 2N 1D 3G',
    '100M 2N 1D 4G',

    'Total Runtime'],
  'TimeInSeconds': [-0.1]*16
}
BenchmarkResults = pl.DataFrame(BenchmarkResults)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars_Cast.csv')
del BenchmarkResults
gc.collect()

# Aggregation 1M

# Melt Numeric Variable:

## 1M 2N 1D 0G
data = pl.read_csv(f'{Path}FakeBevData1M.csv')
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars_Cast.csv')
data = data.with_columns(pl.col('Date').str.to_date('%Y-%m-%d'))
vals = [f"Location {i}" for i in range(1,44)]
temp = data.melt(id_vars = ['Date','Customer','Brand','Category','Beverage Flavor'], value_vars = ['Daily Liters','Daily Units','Daily Margin','Daily Revenue'])
temp = temp.group_by(['Date','Customer','Brand','Category','Beverage Flavor','variable']).agg(pl.sum('value'))
temp = temp.filter(pl.col('Customer').is_in(vals))
rts = [1.1]*10
for i in range(0,10):
  print(i)
  start = timeit.default_timer()
  x = temp.pivot(index = "Date", columns = "variable", values = "value", aggregate_function = "sum").sort("Date")
  x = x.fill_null(0)
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[0, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars_Cast.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 2N 1D 1G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars_Cast.csv')
rts = [1.1]*10
for i in range(0,10):
  print(i)
  start = timeit.default_timer()
  x = temp.pivot(index = ["Date","Customer"], columns = "variable", values = "value", aggregate_function = "sum").sort("Date")
  x = x.fill_null(0)
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[1, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars_Cast.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 2N 1D 2G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars_Cast.csv')
rts = [1.1]*10
for i in range(0,10):
  print(i)
  start = timeit.default_timer()
  x = temp.pivot(index = ["Date","Customer","Brand"], columns = "variable", values = "value", aggregate_function = "sum").sort("Date")
  x = x.fill_null(0)
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[2, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars_Cast.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 2N 1D 3G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars_Cast.csv')
rts = [1.1]*10
for i in range(0,10):
  print(i)
  start = timeit.default_timer()
  x = temp.pivot(index = ["Date","Customer","Brand","Category"], columns = "variable", values = "value", aggregate_function = "sum").sort("Date")
  x = x.fill_null(0)
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[3, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars_Cast.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 2N 1D 4G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars_Cast.csv')
rts = [1.1]*10
for i in range(0,10):
  print(i)
  start = timeit.default_timer()
  x = temp.pivot(index = ["Date","Customer","Brand","Category","Beverage Flavor"], columns = "variable", values = "value", aggregate_function = "sum").sort("Date")
  x = x.fill_null(0)
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[4, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars_Cast.csv')
del BenchmarkResults, end, start
gc.collect()

###################################################################################################
###################################################################################################
###################################################################################################
###################################################################################################
###################################################################################################

# Aggregation 10M

# Melt Numeric Variable:

## 10M 2N 1D 0G
data = pl.read_csv(f'{Path}FakeBevData10M.csv')
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars_Cast.csv')
data = data.with_columns(pl.col('Date').str.to_date('%Y-%m-%d'))
vals = [f"Location {i}" for i in range(1,483)]
temp = data.melt(id_vars = ['Date','Customer','Brand','Category','Beverage Flavor'], value_vars = ['Daily Liters','Daily Units','Daily Margin','Daily Revenue'])
temp = temp.group_by(['Date','Customer','Brand','Category','Beverage Flavor','variable']).agg(pl.sum('value'))
temp = temp.filter(pl.col('Customer').is_in(vals))
rts = [1.1]*10
for i in range(0,10):
  print(i)
  start = timeit.default_timer()
  x = temp.pivot(index = "Date", columns = "variable", values = "value", aggregate_function = "sum").sort("Date")
  x = x.fill_null(0)
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[5, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars_Cast.csv')
del BenchmarkResults, end, start
gc.collect()

## 10M 2N 1D 1G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars_Cast.csv')
rts = [1.1]*10
for i in range(0,10):
  print(i)
  start = timeit.default_timer()
  x = temp.pivot(index = ["Date","Customer"], columns = "variable", values = "value", aggregate_function = "sum").sort("Date")
  x = x.fill_null(0)
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[6, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars_Cast.csv')
del BenchmarkResults, end, start
gc.collect()

## 10M 2N 1D 2G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars_Cast.csv')
rts = [1.1]*10
for i in range(0,10):
  print(i)
  start = timeit.default_timer()
  x = temp.pivot(index = ["Date","Customer","Brand"], columns = "variable", values = "value", aggregate_function = "sum").sort("Date")
  x = x.fill_null(0)
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[7, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars_Cast.csv')
del BenchmarkResults, end, start
gc.collect()

## 10M 2N 1D 3G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars_Cast.csv')
rts = [1.1]*10
for i in range(0,10):
  print(i)
  start = timeit.default_timer()
  x = temp.pivot(index = ["Date","Customer","Brand","Category"], columns = "variable", values = "value", aggregate_function = "sum").sort("Date")
  x = x.fill_null(0)
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[8, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars_Cast.csv')
del BenchmarkResults, end, start
gc.collect()

## 10M 2N 1D 4G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars_Cast.csv')
rts = [1.1]*10
for i in range(0,10):
  print(i)
  start = timeit.default_timer()
  x = temp.pivot(index = ["Date","Customer","Brand","Category","Beverage Flavor"], columns = "variable", values = "value", aggregate_function = "sum").sort("Date")
  x = x.fill_null(0)
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[9, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars_Cast.csv')
del BenchmarkResults, end, start
gc.collect()

###################################################################################################
###################################################################################################
###################################################################################################
###################################################################################################
###################################################################################################

# Aggregation 100M

# Melt Numeric Variable:

## 100M 2N 1D 0G
data = pl.read_csv(f'{Path}FakeBevData100M.csv')
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars_Cast.csv')
data = data.with_columns(pl.col('Date').str.to_date('%Y-%m-%d'))
vals = [f"Location {i}" for i in range(1,4882)]
temp = data.melt(id_vars = ['Date','Customer','Brand','Category','Beverage Flavor'], value_vars = ['Daily Liters','Daily Units','Daily Margin','Daily Revenue'])
temp = temp.group_by(['Date','Customer','Brand','Category','Beverage Flavor','variable']).agg(pl.sum('value'))
temp = temp.filter(pl.col('Customer').is_in(vals))
rts = [1.1]*10
for i in range(0,10):
  print(i)
  start = timeit.default_timer()
  x = temp.pivot(index = "Date", columns = "variable", values = "value", aggregate_function = "sum").sort("Date")
  x = x.fill_null(0)
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[10, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars_Cast.csv')
del BenchmarkResults, end, start
gc.collect()

## 100M 2N 1D 1G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars_Cast.csv')
rts = [1.1]*10
for i in range(0,10):
  print(i)
  start = timeit.default_timer()
  x = temp.pivot(index = ["Date","Customer"], columns = "variable", values = "value", aggregate_function = "sum").sort("Date")
  x = x.fill_null(0)
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[11, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars_Cast.csv')
del BenchmarkResults, end, start
gc.collect()

## 100M 2N 1D 2G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars_Cast.csv')
rts = [1.1]*10
for i in range(0,10):
  print(i)
  start = timeit.default_timer()
  x = temp.pivot(index = ["Date","Customer","Brand"], columns = "variable", values = "value", aggregate_function = "sum").sort("Date")
  x = x.fill_null(0)
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[12, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars_Cast.csv')
del BenchmarkResults, end, start
gc.collect()

## 100M 2N 1D 3G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars_Cast.csv')
rts = [1.1]*10
for i in range(0,10):
  print(i)
  start = timeit.default_timer()
  x = temp.pivot(index = ["Date","Customer","Brand","Category"], columns = "variable", values = "value", aggregate_function = "sum").sort("Date")
  x = x.fill_null(0)
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[13, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars_Cast.csv')
del BenchmarkResults, end, start
gc.collect()

## 100M 2N 1D 4G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars_Cast.csv')
rts = [1.1]*10
for i in range(0,10):
  print(i)
  start = timeit.default_timer()
  x = temp.pivot(index = ["Date","Customer","Brand","Category","Beverage Flavor"], columns = "variable", values = "value", aggregate_function = "sum").sort("Date")
  x = x.fill_null(0)
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[14, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars_Cast.csv')
del BenchmarkResults, end, start
gc.collect()


BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars_Cast.csv')
x = BenchmarkResults[0:14]
y = x['TimeInSeconds'].sum()
BenchmarkResults[15, 'TimeInSeconds'] = y
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars_Cast.csv')

