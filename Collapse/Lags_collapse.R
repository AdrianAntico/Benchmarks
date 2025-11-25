# Path to data storage
Path <- "C:/Users/Bizon/Documents/GitHub/rappwd/"

# Create results table
BenchmarkResults <- data.table::data.table(
  Framework = 'collapse',
  Method = 'lags',
  Experiment = c(
    "1M 3N 1D 4G 5L",
    "10M 3N 1D 4G 5L",
    "100M 3N 1D 4G 5L"
  ),

  TimeInSeconds = c(rep(-0.1, 3))
)

data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsCollapse_Lags.csv"))
rm(BenchmarkResults)

library(data.table)
library(collapse)

# Aggregation 1M

# Sum 1 Numeric Variable:
data <- fread(paste0(Path, "FakeBevData1M.csv"))
setorderv(data, cols = c("Customer","Brand","Category","Date","Beverage Flavor"), c(1,1,1,1,1))
set_collapse(nthreads = data.table::getDTthreads(), mask = "all", na.rm = anyNA(num_vars(data)), stable.algo = FALSE, sort = FALSE)
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsCollapse_Lags.csv"))

## 1M 3N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsCollapse_Lags.csv"))
start <- Sys.time()
ftransformv(data, c("Daily Liters", "Daily Units", "Daily Margin"), flag, 1:5, list(Customer, Brand, Category, `Beverage Flavor`), apply = FALSE)
end <- Sys.time()
BenchmarkResults[1, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsCollapse_Lags.csv"))
rm(list = c("BenchmarkResults","data","end","start"))
gc()

###################################################################################################
###################################################################################################
###################################################################################################
###################################################################################################
###################################################################################################

# Aggregation 10M

# Sum 1 Numeric Variable:
data <- fread(paste0(Path, "FakeBevData10M.csv"))
setorderv(data, cols = c("Customer","Brand","Category","Date","Beverage Flavor"), c(1,1,1,1,1))
set_collapse(nthreads = data.table::getDTthreads(), mask = "all", na.rm = anyNA(num_vars(data)), stable.algo = FALSE, sort = FALSE)


## 10M 3N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsCollapse_Lags.csv"))
start <- Sys.time()
ftransformv(data, c("Daily Liters", "Daily Units", "Daily Margin"), flag, 1:5, list(Customer, Brand, Category, `Beverage Flavor`), apply = FALSE)
end <- Sys.time()
BenchmarkResults[2, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsCollapse_Lags.csv"))
rm(list = c("BenchmarkResults","data","end","start"))
gc()


###################################################################################################
###################################################################################################
###################################################################################################
###################################################################################################
###################################################################################################

# Aggregation 100M

# Sum 1 Numeric Variable:
data <- fread(paste0(Path, "FakeBevData100M.csv"))
setorderv(data, cols = c("Customer","Brand","Category","Date","Beverage Flavor"), c(1,1,1,1,1))
set_collapse(nthreads = data.table::getDTthreads(), mask = "all", na.rm = anyNA(num_vars(data)), stable.algo = FALSE, sort = FALSE)


## 100M 3N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsCollapse_Lags.csv"))
start <- Sys.time()
ftransformv(data, c("Daily Liters", "Daily Units", "Daily Margin"), flag, 1:5, list(Customer, Brand, Category, `Beverage Flavor`), apply = FALSE)
end <- Sys.time()
BenchmarkResults[3, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsCollapse_Lags.csv"))
rm(list = c("BenchmarkResults","data","end","start"))
gc()
