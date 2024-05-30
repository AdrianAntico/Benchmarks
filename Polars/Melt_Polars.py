import polars as pl
import statistics as stats
import timeit
import gc

# Path to source data
Path = "C:/Users/Bizon/Documents/GitHub/rappwd/"

# Create results table
BenchmarkResults = {
  'Framework': ['polars']*46,
  'Method': ['melt'] * 46,
  'Experiment': [
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
    '1M 4N 1D 0G',
    '1M 4N 1D 1G',
    '1M 4N 1D 2G',
    '1M 4N 1D 3G',
    '1M 4N 1D 4G',
    
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
    '10M 4N 1D 0G',
    '10M 4N 1D 1G',
    '10M 4N 1D 2G',
    '10M 4N 1D 3G',
    '10M 4N 1D 4G',
    
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
    '100M 4N 1D 0G',
    '100M 4N 1D 1G',
    '100M 4N 1D 2G',
    '100M 4N 1D 3G',
    '100M 4N 1D 4G',

    'Total Runtime'],
  'TimeInSeconds': [-0.1]*46
}
BenchmarkResults = pl.DataFrame(BenchmarkResults)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars_Melt.csv')
del BenchmarkResults
gc.collect()

# Aggregation 1M

# Melt Numeric Variable:

## 1M 2N 1D 0G
data = pl.read_csv(f'{Path}FakeBevData1M.csv')
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars_Melt.csv')
data = data.with_columns(pl.col('Date').str.to_date('%Y-%m-%d'))
rts = [1.1]*10
for i in range(0,10):
  start = timeit.default_timer()
  data.melt(id_vars = 'Date', value_vars = ['Daily Liters','Daily Units'])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[0, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars_Melt.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 2N 1D 1G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars_Melt.csv')
rts = [1.1]*10
for i in range(0,10):
  start = timeit.default_timer()
  data.melt(id_vars = ['Date','Customer'], value_vars = ['Daily Liters','Daily Units'])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[1, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars_Melt.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 2N 1D 2G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars_Melt.csv')
rts = [1.1]*10
for i in range(0,10):
  start = timeit.default_timer()
  data.melt(id_vars = ['Date','Customer','Brand'], value_vars = ['Daily Liters','Daily Units'])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[2, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars_Melt.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 2N 1D 3G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars_Melt.csv')
rts = [1.1]*10
for i in range(0,10):
  start = timeit.default_timer()
  data.melt(id_vars = ['Date','Customer','Brand','Category'], value_vars = ['Daily Liters','Daily Units'])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[3, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars_Melt.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 2N 1D 4G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars_Melt.csv')
rts = [1.1]*10
for i in range(0,10):
  start = timeit.default_timer()
  data.melt(id_vars = ['Date','Customer','Brand','Category','Beverage Flavor'], value_vars = ['Daily Liters','Daily Units'])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[4, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars_Melt.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 3N 1D 0G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars_Melt.csv')
rts = [1.1]*10
for i in range(0,10):
  start = timeit.default_timer()
  data.melt(id_vars = 'Date', value_vars = ['Daily Liters','Daily Units','Daily Margin'])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[5, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars_Melt.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 3N 1D 1G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars_Melt.csv')
rts = [1.1]*10
for i in range(0,10):
  start = timeit.default_timer()
  data.melt(id_vars = ['Date','Customer'], value_vars = ['Daily Liters','Daily Units','Daily Margin'])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[6, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars_Melt.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 3N 1D 2G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars_Melt.csv')
rts = [1.1]*10
for i in range(0,10):
  start = timeit.default_timer()
  data.melt(id_vars = ['Date','Customer','Brand'], value_vars = ['Daily Liters','Daily Units','Daily Margin'])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[7, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars_Melt.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 3N 1D 3G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars_Melt.csv')
rts = [1.1]*10
for i in range(0,10):
  start = timeit.default_timer()
  data.melt(id_vars = ['Date','Customer','Brand','Category'], value_vars = ['Daily Liters','Daily Units','Daily Margin'])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[8, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars_Melt.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 3N 1D 4G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars_Melt.csv')
rts = [1.1]*10
for i in range(0,10):
  start = timeit.default_timer()
  data.melt(id_vars = ['Date','Customer','Brand','Category','Beverage Flavor'], value_vars = ['Daily Liters','Daily Units','Daily Margin'])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[9, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars_Melt.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 4N 1D 0G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars_Melt.csv')
rts = [1.1]*10
for i in range(0,10):
  start = timeit.default_timer()
  data.melt(id_vars = 'Date', value_vars = ['Daily Liters','Daily Units','Daily Margin','Daily Revenue'])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[10, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars_Melt.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 4N 1D 1G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars_Melt.csv')
rts = [1.1]*10
for i in range(0,10):
  start = timeit.default_timer()
  data.melt(id_vars = ['Date','Customer'], value_vars = ['Daily Liters','Daily Units','Daily Margin','Daily Revenue'])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[11, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars_Melt.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 4N 1D 2G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars_Melt.csv')
rts = [1.1]*10
for i in range(0,10):
  start = timeit.default_timer()
  data.melt(id_vars = ['Date','Customer','Brand'], value_vars = ['Daily Liters','Daily Units','Daily Margin','Daily Revenue'])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[12, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars_Melt.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 4N 1D 3G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars_Melt.csv')
rts = [1.1]*10
for i in range(0,10):
  start = timeit.default_timer()
  data.melt(id_vars = ['Date','Customer','Brand','Category'], value_vars = ['Daily Liters','Daily Units','Daily Margin','Daily Revenue'])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[13, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars_Melt.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 4N 1D 4G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars_Melt.csv')
rts = [1.1]*10
for i in range(0,10):
  start = timeit.default_timer()
  data.melt(id_vars = ['Date','Customer','Brand','Category','Beverage Flavor'], value_vars = ['Daily Liters','Daily Units','Daily Margin','Daily Revenue'])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[14, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars_Melt.csv')
del data, BenchmarkResults, end, start
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
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars_Melt.csv')
data = data.with_columns(pl.col('Date').str.to_date('%Y-%m-%d'))
rts = [1.1]*10
for i in range(0,10):
  start = timeit.default_timer()
  data.melt(id_vars = 'Date', value_vars = ['Daily Liters','Daily Units'])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[15, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars_Melt.csv')
del BenchmarkResults, end, start
gc.collect()

## 10M 2N 1D 1G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars_Melt.csv')
rts = [1.1]*10
for i in range(0,10):
  start = timeit.default_timer()
  data.melt(id_vars = ['Date','Customer'], value_vars = ['Daily Liters','Daily Units'])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[16, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars_Melt.csv')
del BenchmarkResults, end, start
gc.collect()

## 10M 2N 1D 2G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars_Melt.csv')
rts = [1.1]*10
for i in range(0,10):
  start = timeit.default_timer()
  data.melt(id_vars = ['Date','Customer','Brand'], value_vars = ['Daily Liters','Daily Units'])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[17, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars_Melt.csv')
del BenchmarkResults, end, start
gc.collect()

## 10M 2N 1D 3G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars_Melt.csv')
rts = [1.1]*10
for i in range(0,10):
  start = timeit.default_timer()
  data.melt(id_vars = ['Date','Customer','Brand','Category'], value_vars = ['Daily Liters','Daily Units'])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[18, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars_Melt.csv')
del BenchmarkResults, end, start
gc.collect()

## 10M 2N 1D 4G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars_Melt.csv')
rts = [1.1]*10
for i in range(0,10):
  start = timeit.default_timer()
  data.melt(id_vars = ['Date','Customer','Brand','Category','Beverage Flavor'], value_vars = ['Daily Liters','Daily Units'])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[19, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars_Melt.csv')
del BenchmarkResults, end, start
gc.collect()

## 10M 3N 1D 0G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars_Melt.csv')
rts = [1.1]*10
for i in range(0,10):
  start = timeit.default_timer()
  data.melt(id_vars = 'Date', value_vars = ['Daily Liters','Daily Units','Daily Margin'])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[20, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars_Melt.csv')
del BenchmarkResults, end, start
gc.collect()

## 10M 3N 1D 1G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars_Melt.csv')
rts = [1.1]*10
for i in range(0,10):
  start = timeit.default_timer()
  data.melt(id_vars = ['Date','Customer'], value_vars = ['Daily Liters','Daily Units','Daily Margin'])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[21, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars_Melt.csv')
del BenchmarkResults, end, start
gc.collect()

## 10M 3N 1D 2G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars_Melt.csv')
rts = [1.1]*10
for i in range(0,10):
  start = timeit.default_timer()
  data.melt(id_vars = ['Date','Customer','Brand'], value_vars = ['Daily Liters','Daily Units','Daily Margin'])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[22, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars_Melt.csv')
del BenchmarkResults, end, start
gc.collect()

## 10M 3N 1D 3G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars_Melt.csv')
rts = [1.1]*10
for i in range(0,10):
  start = timeit.default_timer()
  data.melt(id_vars = ['Date','Customer','Brand','Category'], value_vars = ['Daily Liters','Daily Units','Daily Margin'])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[23, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars_Melt.csv')
del BenchmarkResults, end, start
gc.collect()

## 10M 3N 1D 4G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars_Melt.csv')
rts = [1.1]*10
for i in range(0,10):
  start = timeit.default_timer()
  data.melt(id_vars = ['Date','Customer','Brand','Category','Beverage Flavor'], value_vars = ['Daily Liters','Daily Units','Daily Margin'])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[24, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars_Melt.csv')
del BenchmarkResults, end, start
gc.collect()

## 10M 4N 1D 0G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars_Melt.csv')
rts = [1.1]*10
for i in range(0,10):
  start = timeit.default_timer()
  data.melt(id_vars = 'Date', value_vars = ['Daily Liters','Daily Units','Daily Margin','Daily Revenue'])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[25, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars_Melt.csv')
del BenchmarkResults, end, start
gc.collect()

## 10M 4N 1D 1G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars_Melt.csv')
rts = [1.1]*10
for i in range(0,10):
  start = timeit.default_timer()
  data.melt(id_vars = ['Date','Customer'], value_vars = ['Daily Liters','Daily Units','Daily Margin','Daily Revenue'])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[26, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars_Melt.csv')
del BenchmarkResults, end, start
gc.collect()

## 10M 4N 1D 2G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars_Melt.csv')
rts = [1.1]*10
for i in range(0,10):
  start = timeit.default_timer()
  data.melt(id_vars = ['Date','Customer','Brand'], value_vars = ['Daily Liters','Daily Units','Daily Margin','Daily Revenue'])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[27, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars_Melt.csv')
del BenchmarkResults, end, start
gc.collect()

## 10M 4N 1D 3G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars_Melt.csv')
rts = [1.1]*10
for i in range(0,10):
  start = timeit.default_timer()
  data.melt(id_vars = ['Date','Customer','Brand','Category'], value_vars = ['Daily Liters','Daily Units','Daily Margin','Daily Revenue'])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[28, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars_Melt.csv')
del BenchmarkResults, end, start
gc.collect()

## 10M 4N 1D 4G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars_Melt.csv')
rts = [1.1]*10
for i in range(0,10):
  start = timeit.default_timer()
  data.melt(id_vars = ['Date','Customer','Brand','Category','Beverage Flavor'], value_vars = ['Daily Liters','Daily Units','Daily Margin','Daily Revenue'])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[29, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars_Melt.csv')
del data, BenchmarkResults, end, start
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
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars_Melt.csv')
data = data.with_columns(pl.col('Date').str.to_date('%Y-%m-%d'))
rts = [1.1]*10
for i in range(0,10):
  start = timeit.default_timer()
  data.melt(id_vars = 'Date', value_vars = ['Daily Liters','Daily Units'])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[30, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars_Melt.csv')
del BenchmarkResults, end, start
gc.collect()

## 100M 2N 1D 1G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars_Melt.csv')
rts = [1.1]*10
for i in range(0,10):
  start = timeit.default_timer()
  data.melt(id_vars = ['Date','Customer'], value_vars = ['Daily Liters','Daily Units'])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[31, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars_Melt.csv')
del BenchmarkResults, end, start
gc.collect()

## 100M 2N 1D 2G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars_Melt.csv')
rts = [1.1]*10
for i in range(0,10):
  start = timeit.default_timer()
  data.melt(id_vars = ['Date','Customer','Brand'], value_vars = ['Daily Liters','Daily Units'])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[32, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars_Melt.csv')
del BenchmarkResults, end, start
gc.collect()

## 100M 2N 1D 3G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars_Melt.csv')
rts = [1.1]*10
for i in range(0,10):
  start = timeit.default_timer()
  data.melt(id_vars = ['Date','Customer','Brand','Category'], value_vars = ['Daily Liters','Daily Units'])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[33, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars_Melt.csv')
del BenchmarkResults, end, start
gc.collect()

## 100M 2N 1D 4G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars_Melt.csv')
rts = [1.1]*10
for i in range(0,10):
  start = timeit.default_timer()
  data.melt(id_vars = ['Date','Customer','Brand','Category','Beverage Flavor'], value_vars = ['Daily Liters','Daily Units'])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[34, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars_Melt.csv')
del BenchmarkResults, end, start
gc.collect()

## 100M 3N 1D 0G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars_Melt.csv')
rts = [1.1]*10
for i in range(0,10):
  start = timeit.default_timer()
  data.melt(id_vars = 'Date', value_vars = ['Daily Liters','Daily Units','Daily Margin'])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[35, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars_Melt.csv')
del BenchmarkResults, end, start
gc.collect()

## 100M 3N 1D 1G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars_Melt.csv')
rts = [1.1]*10
for i in range(0,10):
  start = timeit.default_timer()
  data.melt(id_vars = ['Date','Customer'], value_vars = ['Daily Liters','Daily Units','Daily Margin'])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[36, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars_Melt.csv')
del BenchmarkResults, end, start
gc.collect()

## 100M 3N 1D 2G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars_Melt.csv')
rts = [1.1]*10
for i in range(0,10):
  start = timeit.default_timer()
  data.melt(id_vars = ['Date','Customer','Brand'], value_vars = ['Daily Liters','Daily Units','Daily Margin'])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[37, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars_Melt.csv')
del BenchmarkResults, end, start
gc.collect()

## 100M 3N 1D 3G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars_Melt.csv')
rts = [1.1]*10
for i in range(0,10):
  start = timeit.default_timer()
  data.melt(id_vars = ['Date','Customer','Brand','Category'], value_vars = ['Daily Liters','Daily Units','Daily Margin'])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[38, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars_Melt.csv')
del BenchmarkResults, end, start
gc.collect()

## 100M 3N 1D 4G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars_Melt.csv')
rts = [1.1]*10
for i in range(0,10):
  start = timeit.default_timer()
  data.melt(id_vars = ['Date','Customer','Brand','Category','Beverage Flavor'], value_vars = ['Daily Liters','Daily Units','Daily Margin'])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[39, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars_Melt.csv')
del BenchmarkResults, end, start
gc.collect()

## 100M 4N 1D 0G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars_Melt.csv')
rts = [1.1]*10
for i in range(0,10):
  start = timeit.default_timer()
  data.melt(id_vars = 'Date', value_vars = ['Daily Liters','Daily Units','Daily Margin','Daily Revenue'])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[40, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars_Melt.csv')
del BenchmarkResults, end, start
gc.collect()

## 100M 4N 1D 1G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars_Melt.csv')
rts = [1.1]*10
for i in range(0,10):
  start = timeit.default_timer()
  data.melt(id_vars = ['Date','Customer'], value_vars = ['Daily Liters','Daily Units','Daily Margin','Daily Revenue'])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[41, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars_Melt.csv')
del BenchmarkResults, end, start
gc.collect()

## 100M 4N 1D 2G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars_Melt.csv')
rts = [1.1]*10
for i in range(0,10):
  start = timeit.default_timer()
  data.melt(id_vars = ['Date','Customer','Brand'], value_vars = ['Daily Liters','Daily Units','Daily Margin','Daily Revenue'])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[42, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars_Melt.csv')
del BenchmarkResults, end, start
gc.collect()

## 100M 4N 1D 3G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars_Melt.csv')
rts = [1.1]*10
for i in range(0,10):
  start = timeit.default_timer()
  data.melt(id_vars = ['Date','Customer','Brand','Category'], value_vars = ['Daily Liters','Daily Units','Daily Margin','Daily Revenue'])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[43, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars_Melt.csv')
del BenchmarkResults, end, start
gc.collect()

## 100M 4N 1D 4G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars_Melt.csv')
rts = [1.1]*10
for i in range(0,10):
  start = timeit.default_timer()
  data.melt(id_vars = ['Date','Customer','Brand','Category','Beverage Flavor'], value_vars = ['Daily Liters','Daily Units','Daily Margin','Daily Revenue'])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[44, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars_Melt.csv')
del data, BenchmarkResults, end, start
gc.collect()


BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars_Melt.csv')
x = BenchmarkResults[0:44]
y = x['TimeInSeconds'].sum()
BenchmarkResults[45, 'TimeInSeconds'] = y
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars_Melt.csv')

