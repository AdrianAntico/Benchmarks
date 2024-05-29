import polars as pl
import timeit
import gc

# Path to source data
Path = "C:/Users/Bizon/Documents/GitHub/rappwd/"

# Create results table
BenchmarkResults = {
  'Framework': ['polars']*46, 
  'Method': ['lags'] * 46,
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
  'TimeInSeconds': [-0.1]*46
}
BenchmarkResults = pl.DataFrame(BenchmarkResults)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars_Lags.csv')
del BenchmarkResults
gc.collect()

# Aggregation 1M

# Sum 1 Numeric Variable:

## 1M 1N 1D 0G
data = pl.read_csv(f'{Path}FakeBevData1M.csv')
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars_Lags.csv')
data = data.with_columns(pl.col('Date').str.to_date('%Y-%m-%d'))
start = timeit.default_timer()
data.with_columns([pl.col("Daily Liters").shift(x).alias(f"Lag Daily Liters {x}") for x in range(1,6)])
end = timeit.default_timer()
BenchmarkResults[0, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars_Lags.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 1N 1D 1G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars_Lags.csv')
start = timeit.default_timer()
data.with_columns([pl.col("Daily Liters").shift(x).over("Customer").alias(f"Lag Daily Liters {x}") for x in range(1,6)])
end = timeit.default_timer()
BenchmarkResults[1, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars_Lags.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 1N 1D 2G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars_Lags.csv')
start = timeit.default_timer()
data.with_columns([pl.col("Daily Liters").shift(x).over(["Customer","Brand"]).alias(f"Lag Daily Liters {x}") for x in range(1,6)])
end = timeit.default_timer()
BenchmarkResults[2, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars_Lags.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 1N 1D 3G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars_Lags.csv')
start = timeit.default_timer()
data.with_columns([pl.col("Daily Liters").shift(x).over(["Customer","Brand","Category"]).alias(f"Lag Daily Liters {x}") for x in range(1,6)])
end = timeit.default_timer()
BenchmarkResults[3, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars_Lags.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 1N 1D 4G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars_Lags.csv')
start = timeit.default_timer()
data.with_columns([pl.col("Daily Liters").shift(x).over(["Customer","Brand","Category","Beverage Flavor"]).alias(f"Lag Daily Liters {x}") for x in range(1,6)])
end = timeit.default_timer()
BenchmarkResults[4, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars_Lags.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 2N 1D 0G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars_Lags.csv')
start = timeit.default_timer()
columns_to_lag = ['Daily Liters', 'Daily Units']
lagged_columns = []
for col in columns_to_lag:
  for lag in range(1, 6):
    lagged_columns.append(pl.col(col).shift(lag).alias(f"Lag {col} {lag}"))
data.with_columns(lagged_columns)
end = timeit.default_timer()
BenchmarkResults[5, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars_Lags.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 2N 1D 1G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars_Lags.csv')
start = timeit.default_timer()
columns_to_lag = ['Daily Liters', 'Daily Units']
lagged_columns = []
for col in columns_to_lag:
  for lag in range(1, 6):
    lagged_columns.append(pl.col(col).shift(lag).over(["Customer"]).alias(f"Lag {col} {lag}"))
data.with_columns(lagged_columns)
end = timeit.default_timer()
BenchmarkResults[6, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars_Lags.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 2N 1D 2G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars_Lags.csv')
start = timeit.default_timer()
columns_to_lag = ['Daily Liters', 'Daily Units']
lagged_columns = []
for col in columns_to_lag:
  for lag in range(1, 6):
    lagged_columns.append(pl.col(col).shift(lag).over(["Customer", "Brand"]).alias(f"Lag {col} {lag}"))
data.with_columns(lagged_columns)
end = timeit.default_timer()
BenchmarkResults[7, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars_Lags.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 2N 1D 3G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars_Lags.csv')
start = timeit.default_timer()
columns_to_lag = ['Daily Liters', 'Daily Units']
lagged_columns = []
for col in columns_to_lag:
  for lag in range(1, 6):
    lagged_columns.append(pl.col(col).shift(lag).over(["Customer", "Brand", "Category"]).alias(f"Lag {col} {lag}"))
data.with_columns(lagged_columns)
end = timeit.default_timer()
BenchmarkResults[8, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars_Lags.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 2N 1D 4G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars_Lags.csv')
start = timeit.default_timer()
columns_to_lag = ['Daily Liters', 'Daily Units']
lagged_columns = []
for col in columns_to_lag:
  for lag in range(1, 6):
    lagged_columns.append(pl.col(col).shift(lag).over(["Customer", "Brand", "Category", "Beverage Flavor"]).alias(f"Lag {col} {lag}"))
data.with_columns(lagged_columns)
end = timeit.default_timer()
BenchmarkResults[9, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars_Lags.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 3N 1D 0G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars_Lags.csv')
start = timeit.default_timer()
columns_to_lag = ['Daily Liters', 'Daily Units', 'Daily Margin']
lagged_columns = []
for col in columns_to_lag:
  for lag in range(1, 6):
    lagged_columns.append(pl.col(col).shift(lag).alias(f"Lag {col} {lag}"))
data.with_columns(lagged_columns)
end = timeit.default_timer()
BenchmarkResults[10, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars_Lags.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 3N 1D 1G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars_Lags.csv')
start = timeit.default_timer()
columns_to_lag = ['Daily Liters', 'Daily Units', 'Daily Margin']
lagged_columns = []
for col in columns_to_lag:
  for lag in range(1, 6):
    lagged_columns.append(pl.col(col).shift(lag).over(["Customer"]).alias(f"Lag {col} {lag}"))
data.with_columns(lagged_columns)
end = timeit.default_timer()
BenchmarkResults[11, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars_Lags.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 3N 1D 2G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars_Lags.csv')
start = timeit.default_timer()
columns_to_lag = ['Daily Liters', 'Daily Units', 'Daily Margin']
lagged_columns = []
for col in columns_to_lag:
  for lag in range(1, 6):
    lagged_columns.append(pl.col(col).shift(lag).over(["Customer", "Brand"]).alias(f"Lag {col} {lag}"))
data.with_columns(lagged_columns)
end = timeit.default_timer()
BenchmarkResults[12, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars_Lags.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 3N 1D 3G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars_Lags.csv')
start = timeit.default_timer()
columns_to_lag = ['Daily Liters', 'Daily Units', 'Daily Margin']
lagged_columns = []
for col in columns_to_lag:
  for lag in range(1, 6):
    lagged_columns.append(pl.col(col).shift(lag).over(["Customer", "Brand", "Category"]).alias(f"Lag {col} {lag}"))
data.with_columns(lagged_columns)
end = timeit.default_timer()
BenchmarkResults[13, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars_Lags.csv')
del BenchmarkResults, end, start
gc.collect()

## 1M 3N 1D 4G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars_Lags.csv')
start = timeit.default_timer()
columns_to_lag = ['Daily Liters', 'Daily Units', 'Daily Margin']
lagged_columns = []
for col in columns_to_lag:
  for lag in range(1, 6):
    lagged_columns.append(pl.col(col).shift(lag).over(["Customer", "Brand", "Category", "Beverage Flavor"]).alias(f"Lag {col} {lag}"))
data.with_columns(lagged_columns)
end = timeit.default_timer()
BenchmarkResults[14, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars_Lags.csv')
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
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars_Lags.csv')
data = data.with_columns(pl.col('Date').str.to_date('%Y-%m-%d'))
start = timeit.default_timer()
data.with_columns([pl.col("Daily Liters").shift(x).alias(f"Lag Daily Liters {x}") for x in range(1,6)])
end = timeit.default_timer()
BenchmarkResults[15, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars_Lags.csv')
del BenchmarkResults, end, start
gc.collect()

## 10M 1N 1D 1G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars_Lags.csv')
start = timeit.default_timer()
data.with_columns([pl.col("Daily Liters").shift(x).over("Customer").alias(f"Lag Daily Liters {x}") for x in range(1,6)])
end = timeit.default_timer()
BenchmarkResults[16, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars_Lags.csv')
del BenchmarkResults, end, start
gc.collect()

## 10M 1N 1D 2G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars_Lags.csv')
start = timeit.default_timer()
data.with_columns([pl.col("Daily Liters").shift(x).over(["Customer","Brand"]).alias(f"Lag Daily Liters {x}") for x in range(1,6)])
end = timeit.default_timer()
BenchmarkResults[17, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars_Lags.csv')
del BenchmarkResults, end, start
gc.collect()

## 10M 1N 1D 3G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars_Lags.csv')
start = timeit.default_timer()
data.with_columns([pl.col("Daily Liters").shift(x).over(["Customer","Brand","Category"]).alias(f"Lag Daily Liters {x}") for x in range(1,6)])
end = timeit.default_timer()
BenchmarkResults[18, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars_Lags.csv')
del BenchmarkResults, end, start
gc.collect()

## 10M 1N 1D 4G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars_Lags.csv')
start = timeit.default_timer()
data.with_columns([pl.col("Daily Liters").shift(x).over(["Customer","Brand","Category","Beverage Flavor"]).alias(f"Lag Daily Liters {x}") for x in range(1,6)])
end = timeit.default_timer()
BenchmarkResults[19, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars_Lags.csv')
del BenchmarkResults, end, start
gc.collect()

## 10M 2N 1D 0G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars_Lags.csv')
start = timeit.default_timer()
columns_to_lag = ['Daily Liters', 'Daily Units']
lagged_columns = []
for col in columns_to_lag:
  for lag in range(1, 6):
    lagged_columns.append(pl.col(col).shift(lag).alias(f"Lag {col} {lag}"))
data.with_columns(lagged_columns)
end = timeit.default_timer()
BenchmarkResults[20, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars_Lags.csv')
del BenchmarkResults, end, start
gc.collect()

## 10M 2N 1D 1G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars_Lags.csv')
start = timeit.default_timer()
columns_to_lag = ['Daily Liters', 'Daily Units']
lagged_columns = []
for col in columns_to_lag:
  for lag in range(1, 6):
    lagged_columns.append(pl.col(col).shift(lag).over(["Customer"]).alias(f"Lag {col} {lag}"))
data.with_columns(lagged_columns)
end = timeit.default_timer()
BenchmarkResults[21, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars_Lags.csv')
del BenchmarkResults, end, start
gc.collect()

## 10M 2N 1D 2G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars_Lags.csv')
start = timeit.default_timer()
columns_to_lag = ['Daily Liters', 'Daily Units']
lagged_columns = []
for col in columns_to_lag:
  for lag in range(1, 6):
    lagged_columns.append(pl.col(col).shift(lag).over(["Customer", "Brand"]).alias(f"Lag {col} {lag}"))
data.with_columns(lagged_columns)
end = timeit.default_timer()
BenchmarkResults[22, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars_Lags.csv')
del BenchmarkResults, end, start
gc.collect()

## 10M 2N 1D 3G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars_Lags.csv')
start = timeit.default_timer()
columns_to_lag = ['Daily Liters', 'Daily Units']
lagged_columns = []
for col in columns_to_lag:
  for lag in range(1, 6):
    lagged_columns.append(pl.col(col).shift(lag).over(["Customer", "Brand", "Category"]).alias(f"Lag {col} {lag}"))
data.with_columns(lagged_columns)
end = timeit.default_timer()
BenchmarkResults[23, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars_Lags.csv')
del BenchmarkResults, end, start
gc.collect()

## 10M 2N 1D 4G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars_Lags.csv')
start = timeit.default_timer()
columns_to_lag = ['Daily Liters', 'Daily Units']
lagged_columns = []
for col in columns_to_lag:
  for lag in range(1, 6):
    lagged_columns.append(pl.col(col).shift(lag).over(["Customer", "Brand", "Category", "Beverage Flavor"]).alias(f"Lag {col} {lag}"))
data.with_columns(lagged_columns)
end = timeit.default_timer()
BenchmarkResults[24, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars_Lags.csv')
del BenchmarkResults, end, start
gc.collect()

## 10M 3N 1D 0G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars_Lags.csv')
start = timeit.default_timer()
columns_to_lag = ['Daily Liters', 'Daily Units', 'Daily Margin']
lagged_columns = []
for col in columns_to_lag:
  for lag in range(1, 6):
    lagged_columns.append(pl.col(col).shift(lag).alias(f"Lag {col} {lag}"))
data.with_columns(lagged_columns)
end = timeit.default_timer()
BenchmarkResults[25, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars_Lags.csv')
del BenchmarkResults, end, start
gc.collect()

## 10M 3N 1D 1G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars_Lags.csv')
start = timeit.default_timer()
columns_to_lag = ['Daily Liters', 'Daily Units', 'Daily Margin']
lagged_columns = []
for col in columns_to_lag:
  for lag in range(1, 6):
    lagged_columns.append(pl.col(col).shift(lag).over(["Customer"]).alias(f"Lag {col} {lag}"))
data.with_columns(lagged_columns)
end = timeit.default_timer()
BenchmarkResults[26, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars_Lags.csv')
del BenchmarkResults, end, start
gc.collect()

## 10M 3N 1D 2G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars_Lags.csv')
start = timeit.default_timer()
columns_to_lag = ['Daily Liters', 'Daily Units', 'Daily Margin']
lagged_columns = []
for col in columns_to_lag:
  for lag in range(1, 6):
    lagged_columns.append(pl.col(col).shift(lag).over(["Customer", "Brand"]).alias(f"Lag {col} {lag}"))
data.with_columns(lagged_columns)
end = timeit.default_timer()
BenchmarkResults[27, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars_Lags.csv')
del BenchmarkResults, end, start
gc.collect()

## 10M 3N 1D 3G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars_Lags.csv')
start = timeit.default_timer()
columns_to_lag = ['Daily Liters', 'Daily Units', 'Daily Margin']
lagged_columns = []
for col in columns_to_lag:
  for lag in range(1, 6):
    lagged_columns.append(pl.col(col).shift(lag).over(["Customer", "Brand", "Category"]).alias(f"Lag {col} {lag}"))
data.with_columns(lagged_columns)
end = timeit.default_timer()
BenchmarkResults[28, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars_Lags.csv')
del BenchmarkResults, end, start
gc.collect()

## 10M 3N 1D 4G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars_Lags.csv')
start = timeit.default_timer()
columns_to_lag = ['Daily Liters', 'Daily Units', 'Daily Margin']
lagged_columns = []
for col in columns_to_lag:
  for lag in range(1, 6):
    lagged_columns.append(pl.col(col).shift(lag).over(["Customer", "Brand", "Category", "Beverage Flavor"]).alias(f"Lag {col} {lag}"))
data.with_columns(lagged_columns)
end = timeit.default_timer()
BenchmarkResults[29, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars_Lags.csv')
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
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars_Lags.csv')
data = data.with_columns(pl.col('Date').str.to_date('%Y-%m-%d'))
start = timeit.default_timer()
data.with_columns([pl.col("Daily Liters").shift(x).alias(f"Lag Daily Liters {x}") for x in range(1,6)])
end = timeit.default_timer()
BenchmarkResults[30, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars_Lags.csv')
del BenchmarkResults, end, start
gc.collect()

## 100M 1N 1D 1G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars_Lags.csv')
start = timeit.default_timer()
data.with_columns([pl.col("Daily Liters").shift(x).over("Customer").alias(f"Lag Daily Liters {x}") for x in range(1,6)])
end = timeit.default_timer()
BenchmarkResults[31, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars_Lags.csv')
del BenchmarkResults, end, start
gc.collect()

## 100M 1N 1D 2G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars_Lags.csv')
start = timeit.default_timer()
data.with_columns([pl.col("Daily Liters").shift(x).over(["Customer","Brand"]).alias(f"Lag Daily Liters {x}") for x in range(1,6)])
end = timeit.default_timer()
BenchmarkResults[32, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars_Lags.csv')
del BenchmarkResults, end, start
gc.collect()

## 100M 1N 1D 3G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars_Lags.csv')
start = timeit.default_timer()
data.with_columns([pl.col("Daily Liters").shift(x).over(["Customer","Brand","Category"]).alias(f"Lag Daily Liters {x}") for x in range(1,6)])
end = timeit.default_timer()
BenchmarkResults[33, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars_Lags.csv')
del BenchmarkResults, end, start
gc.collect()

## 100M 1N 1D 4G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars_Lags.csv')
start = timeit.default_timer()
data.with_columns([pl.col("Daily Liters").shift(x).over(["Customer","Brand","Category","Beverage Flavor"]).alias(f"Lag Daily Liters {x}") for x in range(1,6)])
end = timeit.default_timer()
BenchmarkResults[34, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars_Lags.csv')
del BenchmarkResults, end, start
gc.collect()

## 100M 2N 1D 0G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars_Lags.csv')
start = timeit.default_timer()
columns_to_lag = ['Daily Liters', 'Daily Units']
lagged_columns = []
for col in columns_to_lag:
  for lag in range(1, 6):
    lagged_columns.append(pl.col(col).shift(lag).alias(f"Lag {col} {lag}"))
data.with_columns(lagged_columns)
end = timeit.default_timer()
BenchmarkResults[35, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars_Lags.csv')
del BenchmarkResults, end, start
gc.collect()

## 100M 2N 1D 1G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars_Lags.csv')
start = timeit.default_timer()
columns_to_lag = ['Daily Liters', 'Daily Units']
lagged_columns = []
for col in columns_to_lag:
  for lag in range(1, 6):
    lagged_columns.append(pl.col(col).shift(lag).over(["Customer"]).alias(f"Lag {col} {lag}"))
data.with_columns(lagged_columns)
end = timeit.default_timer()
BenchmarkResults[36, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars_Lags.csv')
del BenchmarkResults, end, start
gc.collect()

## 100M 2N 1D 2G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars_Lags.csv')
start = timeit.default_timer()
columns_to_lag = ['Daily Liters', 'Daily Units']
lagged_columns = []
for col in columns_to_lag:
  for lag in range(1, 6):
    lagged_columns.append(pl.col(col).shift(lag).over(["Customer", "Brand"]).alias(f"Lag {col} {lag}"))
data.with_columns(lagged_columns)
end = timeit.default_timer()
BenchmarkResults[37, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars_Lags.csv')
del BenchmarkResults, end, start
gc.collect()

## 100M 2N 1D 3G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars_Lags.csv')
start = timeit.default_timer()
columns_to_lag = ['Daily Liters', 'Daily Units']
lagged_columns = []
for col in columns_to_lag:
  for lag in range(1, 6):
    lagged_columns.append(pl.col(col).shift(lag).over(["Customer", "Brand", "Category"]).alias(f"Lag {col} {lag}"))
data.with_columns(lagged_columns)
end = timeit.default_timer()
BenchmarkResults[38, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars_Lags.csv')
del BenchmarkResults, end, start
gc.collect()

## 100M 2N 1D 4G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars_Lags.csv')
start = timeit.default_timer()
columns_to_lag = ['Daily Liters', 'Daily Units']
lagged_columns = []
for col in columns_to_lag:
  for lag in range(1, 6):
    lagged_columns.append(pl.col(col).shift(lag).over(["Customer", "Brand", "Category", "Beverage Flavor"]).alias(f"Lag {col} {lag}"))
data.with_columns(lagged_columns)
end = timeit.default_timer()
BenchmarkResults[39, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars_Lags.csv')
del BenchmarkResults, end, start
gc.collect()

## 100M 3N 1D 0G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars_Lags.csv')
start = timeit.default_timer()
columns_to_lag = ['Daily Liters', 'Daily Units', 'Daily Margin']
lagged_columns = []
for col in columns_to_lag:
  for lag in range(1, 6):
    lagged_columns.append(pl.col(col).shift(lag).alias(f"Lag {col} {lag}"))
data.with_columns(lagged_columns)
end = timeit.default_timer()
BenchmarkResults[40, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars_Lags.csv')
del BenchmarkResults, end, start
gc.collect()

## 100M 3N 1D 1G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars_Lags.csv')
start = timeit.default_timer()
columns_to_lag = ['Daily Liters', 'Daily Units', 'Daily Margin']
lagged_columns = []
for col in columns_to_lag:
  for lag in range(1, 6):
    lagged_columns.append(pl.col(col).shift(lag).over(["Customer"]).alias(f"Lag {col} {lag}"))
data.with_columns(lagged_columns)
end = timeit.default_timer()
BenchmarkResults[41, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars_Lags.csv')
del BenchmarkResults, end, start
gc.collect()

## 100M 3N 1D 2G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars_Lags.csv')
start = timeit.default_timer()
columns_to_lag = ['Daily Liters', 'Daily Units', 'Daily Margin']
lagged_columns = []
for col in columns_to_lag:
  for lag in range(1, 6):
    lagged_columns.append(pl.col(col).shift(lag).over(["Customer", "Brand"]).alias(f"Lag {col} {lag}"))
data.with_columns(lagged_columns)
end = timeit.default_timer()
BenchmarkResults[42, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars_Lags.csv')
del BenchmarkResults, end, start
gc.collect()

## 100M 3N 1D 3G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars_Lags.csv')
start = timeit.default_timer()
columns_to_lag = ['Daily Liters', 'Daily Units', 'Daily Margin']
lagged_columns = []
for col in columns_to_lag:
  for lag in range(1, 6):
    lagged_columns.append(pl.col(col).shift(lag).over(["Customer", "Brand", "Category"]).alias(f"Lag {col} {lag}"))
data.with_columns(lagged_columns)
end = timeit.default_timer()
BenchmarkResults[43, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars_Lags.csv')
del BenchmarkResults, end, start
gc.collect()

## 100M 3N 1D 4G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars_Lags.csv')
start = timeit.default_timer()
columns_to_lag = ['Daily Liters', 'Daily Units', 'Daily Margin']
lagged_columns = []
for col in columns_to_lag:
  for lag in range(1, 6):
    lagged_columns.append(pl.col(col).shift(lag).over(["Customer", "Brand", "Category", "Beverage Flavor"]).alias(f"Lag {col} {lag}"))
data.with_columns(lagged_columns)
end = timeit.default_timer()
BenchmarkResults[44, 'TimeInSeconds'] = end - start
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars_Lags.csv')
del data, BenchmarkResults, end, start
gc.collect()

BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars_Lags.csv')
x = BenchmarkResults[0:44]
y = x['TimeInSeconds'].sum()
BenchmarkResults[45, 'TimeInSeconds'] = y
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars_Lags.csv')

