import polars as pl
import timeit
import statistics as stats
import datetime as d
import gc

# Path to source data
Path = "C:/Users/Bizon/Documents/GitHub/rappwd/"

# Create results table
BenchmarkResults = {
  'Framework': ['polars']*3,
  'Method': ['filter'] * 3,
  'Experiment': [
    '1M 2N 1D 4G',
    '10M 2N 1D 4G',
    '100M 2N 1D 4G'
  ],
  'TimeInSeconds': [-0.1]*3
}
BenchmarkResults = pl.DataFrame(BenchmarkResults)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars_Filter.csv')
del BenchmarkResults
gc.collect()

# Filter 1M
def subset_list(original_list, indices):
    return [original_list[i] for i in indices]

# Sum 1 Numeric Variable:
data = pl.read_csv(f'{Path}FakeBevData1M.csv', rechunk=True)
data = data.with_columns(pl.col('Date').str.to_date('%Y-%m-%d'))

# Filters
CustList = ["Location " + str(i) for i in range(1,1+len(data["Customer"].unique().sort().to_list()), 2)]
BrandList = subset_list(list(data["Brand"].unique().sort()), [0,2,4,8,10,12])
CatList = subset_list(list(data["Category"].unique().sort().to_list()), [0,2,4])
BevFlavList = subset_list(list(data["Beverage Flavor"].unique().sort().to_list()), [x for x in range(19) if x % 2 == 0])



## 1M 1N 1D 4G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars_Filter.csv')
rts = [1.1]*3
for i in range(0,3):
  print(i)
  start = timeit.default_timer()
  x = data.filter((pl.col('Date') > d.date(2021,6,1)) & (pl.col('Customer').is_in(CustList)) & (pl.col('Brand').is_in(BrandList)) & (pl.col('Category').is_in(CatList)) & (pl.col('Beverage Flavor').is_in(BevFlavList)) & (pl.col('Daily Liters') > 20) & (pl.col('Daily Margin') < 100))
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[0, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars_Filter.csv')
del BenchmarkResults, end, start
gc.collect()

###################################################################################################
###################################################################################################
###################################################################################################
###################################################################################################
###################################################################################################

# Filter 10M

# Sum 1 Numeric Variable:
data = pl.read_csv(f'{Path}FakeBevData10M.csv', rechunk=True)
data = data.with_columns(pl.col('Date').str.to_date('%Y-%m-%d'))

# Filters
CustList = ["Location " + str(i) for i in range(1,1+len(data["Customer"].unique().sort().to_list()), 2)]

## 1M 1N 1D 4G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars_Filter.csv')
rts = [1.1]*3
for i in range(0,3):
  print(i)
  start = timeit.default_timer()
  x = data.filter((pl.col('Date') > d.date(2021,6,1)) & (pl.col('Customer').is_in(CustList)) & (pl.col('Brand').is_in(BrandList)) & (pl.col('Category').is_in(CatList)) & (pl.col('Beverage Flavor').is_in(BevFlavList)) & (pl.col('Daily Liters') > 20) & (pl.col('Daily Margin') < 100))
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[1, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars_Filter.csv')
del BenchmarkResults, end, start
gc.collect()


###################################################################################################
###################################################################################################
###################################################################################################
###################################################################################################
###################################################################################################

# Filter 100M

# Sum 1 Numeric Variable:
data = pl.read_csv(f'{Path}FakeBevData100M.csv', rechunk=True)
data = data.with_columns(pl.col('Date').str.to_date('%Y-%m-%d'))

# Filters
CustList = ["Location " + str(i) for i in range(1,1+len(data["Customer"].unique().sort().to_list()), 2)]


## 1M 1N 1D 4G
BenchmarkResults = pl.read_csv(f'{Path}BenchmarkResultsPolars_Filter.csv')
rts = [1.1]*3
for i in range(0,3):
  print(i)
  start = timeit.default_timer()
  x = data.filter((pl.col('Date') > d.date(2021,6,1)) & (pl.col('Customer').is_in(CustList)) & (pl.col('Brand').is_in(BrandList)) & (pl.col('Category').is_in(CatList)) & (pl.col('Beverage Flavor').is_in(BevFlavList)) & (pl.col('Daily Liters') > 20) & (pl.col('Daily Margin') < 100))
  end = timeit.default_timer()
  rts[i] = end - start
BenchmarkResults[2, 'TimeInSeconds'] = stats.median(rts)
BenchmarkResults.write_csv(f'{Path}BenchmarkResultsPolars_Filter.csv')
del BenchmarkResults, end, start
gc.collect()
