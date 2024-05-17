import pandas as pd
import pyarrow as pa
import statistics as stats
import timeit
import gc

# Path to source data
Path = "C:/Users/Bizon/Documents/GitHub/rappwd/"

# Create results table
BenchmarkResults = {
  'Framework': ['pandas']*46,
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
BenchmarkResults = pd.DataFrame(BenchmarkResults)
BenchmarkResults.to_csv(f'{Path}BenchmarkResultsPandas_Melt.csv')
del BenchmarkResults
gc.collect()

# Aggregation 1M

# Melt Numeric Variable:

## 1M 2N 1D 0G
data = pd.read_csv(f'{Path}FakeBevData1M.csv')
BenchmarkResults = pd.read_csv(f'{Path}BenchmarkResultsPandas_Melt.csv')
rts = [1.1]*30
for i in range(0,30):
  start = timeit.default_timer()
  data.melt(id_vars = 'Date', value_vars = ['Daily Liters','Daily Units'])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults.at[0, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.to_csv(f'{Path}BenchmarkResultsPandas_Melt.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 2N 1D 1G
BenchmarkResults = pd.read_csv(f'{Path}BenchmarkResultsPandas_Melt.csv')
rts = [1.1]*30
for i in range(0,30):
  start = timeit.default_timer()
  data.melt(id_vars = ['Date','Customer'], value_vars = ['Daily Liters','Daily Units'])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults.at[1, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.to_csv(f'{Path}BenchmarkResultsPandas_Melt.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 2N 1D 2G
BenchmarkResults = pd.read_csv(f'{Path}BenchmarkResultsPandas_Melt.csv')
rts = [1.1]*30
for i in range(0,30):
  start = timeit.default_timer()
  data.melt(id_vars = ['Date','Customer','Brand'], value_vars = ['Daily Liters','Daily Units'])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults.at[2, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.to_csv(f'{Path}BenchmarkResultsPandas_Melt.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 2N 1D 3G
BenchmarkResults = pd.read_csv(f'{Path}BenchmarkResultsPandas_Melt.csv')
rts = [1.1]*30
for i in range(0,30):
  start = timeit.default_timer()
  data.melt(id_vars = ['Date','Customer','Brand','Category'], value_vars = ['Daily Liters','Daily Units'])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults.at[3, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.to_csv(f'{Path}BenchmarkResultsPandas_Melt.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 2N 1D 4G
BenchmarkResults = pd.read_csv(f'{Path}BenchmarkResultsPandas_Melt.csv')
rts = [1.1]*30
for i in range(0,30):
  start = timeit.default_timer()
  data.melt(id_vars = ['Date','Customer','Brand','Category','Beverage Flavor'], value_vars = ['Daily Liters','Daily Units'])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults.at[4, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.to_csv(f'{Path}BenchmarkResultsPandas_Melt.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 3N 1D 0G
BenchmarkResults = pd.read_csv(f'{Path}BenchmarkResultsPandas_Melt.csv')
rts = [1.1]*30
for i in range(0,30):
  start = timeit.default_timer()
  data.melt(id_vars = 'Date', value_vars = ['Daily Liters','Daily Units','Daily Margin'])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults.at[5, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.to_csv(f'{Path}BenchmarkResultsPandas_Melt.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 3N 1D 1G
BenchmarkResults = pd.read_csv(f'{Path}BenchmarkResultsPandas_Melt.csv')
rts = [1.1]*30
for i in range(0,30):
  start = timeit.default_timer()
  data.melt(id_vars = ['Date','Customer'], value_vars = ['Daily Liters','Daily Units','Daily Margin'])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults.at[6, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.to_csv(f'{Path}BenchmarkResultsPandas_Melt.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 3N 1D 2G
BenchmarkResults = pd.read_csv(f'{Path}BenchmarkResultsPandas_Melt.csv')
rts = [1.1]*30
for i in range(0,30):
  start = timeit.default_timer()
  data.melt(id_vars = ['Date','Customer','Brand'], value_vars = ['Daily Liters','Daily Units','Daily Margin'])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults.at[7, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.to_csv(f'{Path}BenchmarkResultsPandas_Melt.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 3N 1D 3G
BenchmarkResults = pd.read_csv(f'{Path}BenchmarkResultsPandas_Melt.csv')
rts = [1.1]*30
for i in range(0,30):
  start = timeit.default_timer()
  data.melt(id_vars = ['Date','Customer','Brand','Category'], value_vars = ['Daily Liters','Daily Units','Daily Margin'])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults.at[8, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.to_csv(f'{Path}BenchmarkResultsPandas_Melt.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 3N 1D 4G
BenchmarkResults = pd.read_csv(f'{Path}BenchmarkResultsPandas_Melt.csv')
rts = [1.1]*30
for i in range(0,30):
  start = timeit.default_timer()
  data.melt(id_vars = ['Date','Customer','Brand','Category','Beverage Flavor'], value_vars = ['Daily Liters','Daily Units','Daily Margin'])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults.at[9, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.to_csv(f'{Path}BenchmarkResultsPandas_Melt.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 4N 1D 0G
BenchmarkResults = pd.read_csv(f'{Path}BenchmarkResultsPandas_Melt.csv')
rts = [1.1]*30
for i in range(0,30):
  start = timeit.default_timer()
  data.melt(id_vars = 'Date', value_vars = ['Daily Liters','Daily Units','Daily Margin','Daily Revenue'])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults.at[10, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.to_csv(f'{Path}BenchmarkResultsPandas_Melt.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 4N 1D 1G
BenchmarkResults = pd.read_csv(f'{Path}BenchmarkResultsPandas_Melt.csv')
rts = [1.1]*30
for i in range(0,30):
  start = timeit.default_timer()
  data.melt(id_vars = ['Date','Customer'], value_vars = ['Daily Liters','Daily Units','Daily Margin','Daily Revenue'])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults.at[11, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.to_csv(f'{Path}BenchmarkResultsPandas_Melt.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 4N 1D 2G
BenchmarkResults = pd.read_csv(f'{Path}BenchmarkResultsPandas_Melt.csv')
rts = [1.1]*30
for i in range(0,30):
  start = timeit.default_timer()
  data.melt(id_vars = ['Date','Customer','Brand'], value_vars = ['Daily Liters','Daily Units','Daily Margin','Daily Revenue'])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults.at[12, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.to_csv(f'{Path}BenchmarkResultsPandas_Melt.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 4N 1D 3G
BenchmarkResults = pd.read_csv(f'{Path}BenchmarkResultsPandas_Melt.csv')
rts = [1.1]*30
for i in range(0,30):
  start = timeit.default_timer()
  data.melt(id_vars = ['Date','Customer','Brand','Category'], value_vars = ['Daily Liters','Daily Units','Daily Margin','Daily Revenue'])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults.at[13, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.to_csv(f'{Path}BenchmarkResultsPandas_Melt.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 4N 1D 4G
BenchmarkResults = pd.read_csv(f'{Path}BenchmarkResultsPandas_Melt.csv')
rts = [1.1]*30
for i in range(0,30):
  start = timeit.default_timer()
  data.melt(id_vars = ['Date','Customer','Brand','Category','Beverage Flavor'], value_vars = ['Daily Liters','Daily Units','Daily Margin','Daily Revenue'])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults.at[14, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.to_csv(f'{Path}BenchmarkResultsPandas_Melt.csv')
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
data = pd.read_csv(f'{Path}FakeBevData10M.csv')
BenchmarkResults = pd.read_csv(f'{Path}BenchmarkResultsPandas_Melt.csv')
rts = [1.1]*30
for i in range(0,30):
  start = timeit.default_timer()
  data.melt(id_vars = 'Date', value_vars = ['Daily Liters','Daily Units'])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults.at[15, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.to_csv(f'{Path}BenchmarkResultsPandas_Melt.csv')
del BenchmarkResults, end, start
gc.collect()

## 10M 2N 1D 1G
BenchmarkResults = pd.read_csv(f'{Path}BenchmarkResultsPandas_Melt.csv')
rts = [1.1]*30
for i in range(0,30):
  start = timeit.default_timer()
  data.melt(id_vars = ['Date','Customer'], value_vars = ['Daily Liters','Daily Units'])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults.at[16, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.to_csv(f'{Path}BenchmarkResultsPandas_Melt.csv')
del BenchmarkResults, end, start
gc.collect()

## 10M 2N 1D 2G
BenchmarkResults = pd.read_csv(f'{Path}BenchmarkResultsPandas_Melt.csv')
rts = [1.1]*30
for i in range(0,30):
  start = timeit.default_timer()
  data.melt(id_vars = ['Date','Customer','Brand'], value_vars = ['Daily Liters','Daily Units'])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults.at[17, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.to_csv(f'{Path}BenchmarkResultsPandas_Melt.csv')
del BenchmarkResults, end, start
gc.collect()

## 10M 2N 1D 3G
BenchmarkResults = pd.read_csv(f'{Path}BenchmarkResultsPandas_Melt.csv')
rts = [1.1]*30
for i in range(0,30):
  start = timeit.default_timer()
  data.melt(id_vars = ['Date','Customer','Brand','Category'], value_vars = ['Daily Liters','Daily Units'])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults.at[18, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.to_csv(f'{Path}BenchmarkResultsPandas_Melt.csv')
del BenchmarkResults, end, start
gc.collect()

## 10M 2N 1D 4G
BenchmarkResults = pd.read_csv(f'{Path}BenchmarkResultsPandas_Melt.csv')
rts = [1.1]*30
for i in range(0,30):
  start = timeit.default_timer()
  data.melt(id_vars = ['Date','Customer','Brand','Category','Beverage Flavor'], value_vars = ['Daily Liters','Daily Units'])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults.at[19, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.to_csv(f'{Path}BenchmarkResultsPandas_Melt.csv')
del BenchmarkResults, end, start
gc.collect()

## 10M 3N 1D 0G
BenchmarkResults = pd.read_csv(f'{Path}BenchmarkResultsPandas_Melt.csv')
rts = [1.1]*30
for i in range(0,30):
  start = timeit.default_timer()
  data.melt(id_vars = 'Date', value_vars = ['Daily Liters','Daily Units','Daily Margin'])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults.at[20, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.to_csv(f'{Path}BenchmarkResultsPandas_Melt.csv')
del BenchmarkResults, end, start
gc.collect()

## 10M 3N 1D 1G
BenchmarkResults = pd.read_csv(f'{Path}BenchmarkResultsPandas_Melt.csv')
rts = [1.1]*30
for i in range(0,30):
  start = timeit.default_timer()
  data.melt(id_vars = ['Date','Customer'], value_vars = ['Daily Liters','Daily Units','Daily Margin'])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults.at[21, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.to_csv(f'{Path}BenchmarkResultsPandas_Melt.csv')
del BenchmarkResults, end, start
gc.collect()

## 10M 3N 1D 2G
BenchmarkResults = pd.read_csv(f'{Path}BenchmarkResultsPandas_Melt.csv')
rts = [1.1]*30
for i in range(0,30):
  start = timeit.default_timer()
  data.melt(id_vars = ['Date','Customer','Brand'], value_vars = ['Daily Liters','Daily Units','Daily Margin'])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults.at[22, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.to_csv(f'{Path}BenchmarkResultsPandas_Melt.csv')
del BenchmarkResults, end, start
gc.collect()

## 10M 3N 1D 3G
BenchmarkResults = pd.read_csv(f'{Path}BenchmarkResultsPandas_Melt.csv')
rts = [1.1]*30
for i in range(0,30):
  start = timeit.default_timer()
  data.melt(id_vars = ['Date','Customer','Brand','Category'], value_vars = ['Daily Liters','Daily Units','Daily Margin'])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults.at[23, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.to_csv(f'{Path}BenchmarkResultsPandas_Melt.csv')
del BenchmarkResults, end, start
gc.collect()

## 10M 3N 1D 4G
BenchmarkResults = pd.read_csv(f'{Path}BenchmarkResultsPandas_Melt.csv')
rts = [1.1]*30
for i in range(0,30):
  start = timeit.default_timer()
  data.melt(id_vars = ['Date','Customer','Brand','Category','Beverage Flavor'], value_vars = ['Daily Liters','Daily Units','Daily Margin'])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults.at[24, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.to_csv(f'{Path}BenchmarkResultsPandas_Melt.csv')
del BenchmarkResults, end, start
gc.collect()

## 10M 4N 1D 0G
BenchmarkResults = pd.read_csv(f'{Path}BenchmarkResultsPandas_Melt.csv')
rts = [1.1]*30
for i in range(0,30):
  start = timeit.default_timer()
  data.melt(id_vars = 'Date', value_vars = ['Daily Liters','Daily Units','Daily Margin','Daily Revenue'])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults.at[25, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.to_csv(f'{Path}BenchmarkResultsPandas_Melt.csv')
del BenchmarkResults, end, start
gc.collect()

## 10M 4N 1D 1G
BenchmarkResults = pd.read_csv(f'{Path}BenchmarkResultsPandas_Melt.csv')
rts = [1.1]*30
for i in range(0,30):
  start = timeit.default_timer()
  data.melt(id_vars = ['Date','Customer'], value_vars = ['Daily Liters','Daily Units','Daily Margin','Daily Revenue'])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults.at[26, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.to_csv(f'{Path}BenchmarkResultsPandas_Melt.csv')
del BenchmarkResults, end, start
gc.collect()

## 10M 4N 1D 2G
BenchmarkResults = pd.read_csv(f'{Path}BenchmarkResultsPandas_Melt.csv')
rts = [1.1]*30
for i in range(0,30):
  start = timeit.default_timer()
  data.melt(id_vars = ['Date','Customer','Brand'], value_vars = ['Daily Liters','Daily Units','Daily Margin','Daily Revenue'])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults.at[27, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.to_csv(f'{Path}BenchmarkResultsPandas_Melt.csv')
del BenchmarkResults, end, start
gc.collect()

## 10M 4N 1D 3G
BenchmarkResults = pd.read_csv(f'{Path}BenchmarkResultsPandas_Melt.csv')
rts = [1.1]*30
for i in range(0,30):
  start = timeit.default_timer()
  data.melt(id_vars = ['Date','Customer','Brand','Category'], value_vars = ['Daily Liters','Daily Units','Daily Margin','Daily Revenue'])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults.at[28, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.to_csv(f'{Path}BenchmarkResultsPandas_Melt.csv')
del BenchmarkResults, end, start
gc.collect()

## 10M 4N 1D 4G
BenchmarkResults = pd.read_csv(f'{Path}BenchmarkResultsPandas_Melt.csv')
rts = [1.1]*30
for i in range(0,30):
  start = timeit.default_timer()
  data.melt(id_vars = ['Date','Customer','Brand','Category','Beverage Flavor'], value_vars = ['Daily Liters','Daily Units','Daily Margin','Daily Revenue'])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults.at[29, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.to_csv(f'{Path}BenchmarkResultsPandas_Melt.csv')
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
data = pd.read_csv(f'{Path}FakeBevData100M.csv')
BenchmarkResults = pd.read_csv(f'{Path}BenchmarkResultsPandas_Melt.csv')
rts = [1.1]*30
for i in range(0,30):
  start = timeit.default_timer()
  data.melt(id_vars = 'Date', value_vars = ['Daily Liters','Daily Units'])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults.at[30, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.to_csv(f'{Path}BenchmarkResultsPandas_Melt.csv')
del BenchmarkResults, end, start
gc.collect()

## 100M 2N 1D 1G
BenchmarkResults = pd.read_csv(f'{Path}BenchmarkResultsPandas_Melt.csv')
rts = [1.1]*30
for i in range(0,30):
  start = timeit.default_timer()
  data.melt(id_vars = ['Date','Customer'], value_vars = ['Daily Liters','Daily Units'])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults.at[31, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.to_csv(f'{Path}BenchmarkResultsPandas_Melt.csv')
del BenchmarkResults, end, start
gc.collect()

## 100M 2N 1D 2G
BenchmarkResults = pd.read_csv(f'{Path}BenchmarkResultsPandas_Melt.csv')
rts = [1.1]*30
for i in range(0,30):
  start = timeit.default_timer()
  data.melt(id_vars = ['Date','Customer','Brand'], value_vars = ['Daily Liters','Daily Units'])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults.at[32, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.to_csv(f'{Path}BenchmarkResultsPandas_Melt.csv')
del BenchmarkResults, end, start
gc.collect()

## 100M 2N 1D 3G
BenchmarkResults = pd.read_csv(f'{Path}BenchmarkResultsPandas_Melt.csv')
rts = [1.1]*30
for i in range(0,30):
  start = timeit.default_timer()
  data.melt(id_vars = ['Date','Customer','Brand','Category'], value_vars = ['Daily Liters','Daily Units'])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults.at[33, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.to_csv(f'{Path}BenchmarkResultsPandas_Melt.csv')
del BenchmarkResults, end, start
gc.collect()

## 100M 2N 1D 4G
BenchmarkResults = pd.read_csv(f'{Path}BenchmarkResultsPandas_Melt.csv')
rts = [1.1]*30
for i in range(0,30):
  start = timeit.default_timer()
  data.melt(id_vars = ['Date','Customer','Brand','Category','Beverage Flavor'], value_vars = ['Daily Liters','Daily Units'])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults.at[34, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.to_csv(f'{Path}BenchmarkResultsPandas_Melt.csv')
del BenchmarkResults, end, start
gc.collect()

## 100M 3N 1D 0G
BenchmarkResults = pd.read_csv(f'{Path}BenchmarkResultsPandas_Melt.csv')
rts = [1.1]*30
for i in range(0,30):
  start = timeit.default_timer()
  data.melt(id_vars = 'Date', value_vars = ['Daily Liters','Daily Units','Daily Margin'])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults.at[35, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.to_csv(f'{Path}BenchmarkResultsPandas_Melt.csv')
del BenchmarkResults, end, start
gc.collect()

## 100M 3N 1D 1G
BenchmarkResults = pd.read_csv(f'{Path}BenchmarkResultsPandas_Melt.csv')
rts = [1.1]*30
for i in range(0,30):
  start = timeit.default_timer()
  data.melt(id_vars = ['Date','Customer'], value_vars = ['Daily Liters','Daily Units','Daily Margin'])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults.at[36, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.to_csv(f'{Path}BenchmarkResultsPandas_Melt.csv')
del BenchmarkResults, end, start
gc.collect()

## 100M 3N 1D 2G
BenchmarkResults = pd.read_csv(f'{Path}BenchmarkResultsPandas_Melt.csv')
rts = [1.1]*30
for i in range(0,30):
  start = timeit.default_timer()
  data.melt(id_vars = ['Date','Customer','Brand'], value_vars = ['Daily Liters','Daily Units','Daily Margin'])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults.at[37, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.to_csv(f'{Path}BenchmarkResultsPandas_Melt.csv')
del BenchmarkResults, end, start
gc.collect()

## 100M 3N 1D 3G
BenchmarkResults = pd.read_csv(f'{Path}BenchmarkResultsPandas_Melt.csv')
rts = [1.1]*30
for i in range(0,30):
  start = timeit.default_timer()
  data.melt(id_vars = ['Date','Customer','Brand','Category'], value_vars = ['Daily Liters','Daily Units','Daily Margin'])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults.at[38, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.to_csv(f'{Path}BenchmarkResultsPandas_Melt.csv')
del BenchmarkResults, end, start
gc.collect()

## 100M 3N 1D 4G
BenchmarkResults = pd.read_csv(f'{Path}BenchmarkResultsPandas_Melt.csv')
rts = [1.1]*30
for i in range(0,30):
  start = timeit.default_timer()
  data.melt(id_vars = ['Date','Customer','Brand','Category','Beverage Flavor'], value_vars = ['Daily Liters','Daily Units','Daily Margin'])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults.at[39, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.to_csv(f'{Path}BenchmarkResultsPandas_Melt.csv')
del BenchmarkResults, end, start
gc.collect()

## 100M 4N 1D 0G
BenchmarkResults = pd.read_csv(f'{Path}BenchmarkResultsPandas_Melt.csv')
rts = [1.1]*30
for i in range(0,30):
  start = timeit.default_timer()
  data.melt(id_vars = 'Date', value_vars = ['Daily Liters','Daily Units','Daily Margin','Daily Revenue'])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults.at[40, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.to_csv(f'{Path}BenchmarkResultsPandas_Melt.csv')
del BenchmarkResults, end, start
gc.collect()

## 100M 4N 1D 1G
BenchmarkResults = pd.read_csv(f'{Path}BenchmarkResultsPandas_Melt.csv')
rts = [1.1]*30
for i in range(0,30):
  start = timeit.default_timer()
  data.melt(id_vars = ['Date','Customer'], value_vars = ['Daily Liters','Daily Units','Daily Margin','Daily Revenue'])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults.at[41, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.to_csv(f'{Path}BenchmarkResultsPandas_Melt.csv')
del BenchmarkResults, end, start
gc.collect()

## 100M 4N 1D 2G
BenchmarkResults = pd.read_csv(f'{Path}BenchmarkResultsPandas_Melt.csv')
rts = [1.1]*30
for i in range(0,30):
  start = timeit.default_timer()
  data.melt(id_vars = ['Date','Customer','Brand'], value_vars = ['Daily Liters','Daily Units','Daily Margin','Daily Revenue'])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults.at[42, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.to_csv(f'{Path}BenchmarkResultsPandas_Melt.csv')
del BenchmarkResults, end, start
gc.collect()

## 100M 4N 1D 3G
BenchmarkResults = pd.read_csv(f'{Path}BenchmarkResultsPandas_Melt.csv')
rts = [1.1]*30
for i in range(0,30):
  start = timeit.default_timer()
  data.melt(id_vars = ['Date','Customer','Brand','Category'], value_vars = ['Daily Liters','Daily Units','Daily Margin','Daily Revenue'])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults.at[43, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.to_csv(f'{Path}BenchmarkResultsPandas_Melt.csv')
del BenchmarkResults, end, start
gc.collect()

## 100M 4N 1D 4G
BenchmarkResults = pd.read_csv(f'{Path}BenchmarkResultsPandas_Melt.csv')
rts = [1.1]*30
for i in range(0,30):
  start = timeit.default_timer()
  data.melt(id_vars = ['Date','Customer','Brand','Category','Beverage Flavor'], value_vars = ['Daily Liters','Daily Units','Daily Margin','Daily Revenue'])
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults.at[44, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.to_csv(f'{Path}BenchmarkResultsPandas_Melt.csv')
del data, BenchmarkResults, end, start
gc.collect()


BenchmarkResults = pd.read_csv(f'{Path}BenchmarkResultsPandas_Melt.csv')
x = BenchmarkResults[0:44]
y = x['TimeInSeconds'].sum()
BenchmarkResults.at[45, 'TimeInSeconds'] = y
BenchmarkResults.to_csv(f'{Path}BenchmarkResultsPandas_Melt.csv')

