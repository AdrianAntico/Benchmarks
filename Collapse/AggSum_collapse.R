# Path to data storage
Path <- "C:/Users/Bizon/Documents/GitHub/rappwd/"

# Create results table
BenchmarkResults <- data.table::data.table(
  Framework = 'collapse',
  Method = 'sum aggregation',
  Experiment = c(
    "1M 3N 1D 4G",
    "10M 3N 1D 4G",
    "100M 3N 1D 4G",
    "1B 3N 1D 4G"
  ),
  TimeInSeconds = c(rep(-0.1, 4))
)

data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsCollapse.csv"))
rm(BenchmarkResults)

library(data.table)
library(collapse)

# Aggregation 1M

# Sum 1 Numeric Variable:

## 1M 1N 1D 0G
data <- fread(paste0(Path, "FakeBevData1M.csv"))
set_collapse(nthreads = data.table::getDTthreads(), na.rm = anyNA(num_vars(data)), stable.algo = FALSE, sort = FALSE)

## 1M 3N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsCollapse.csv"))
start <- Sys.time()
collap(data, `Daily Liters` + `Daily Units` + `Daily Margin` ~ Date + Customer + Brand + Category + `Beverage Flavor`, fsum)
end <- Sys.time()
BenchmarkResults[1, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsCollapse.csv"))
rm(list = c("BenchmarkResults","data","end","start"))
gc()

###################################################################################################
###################################################################################################
###################################################################################################
###################################################################################################
###################################################################################################

# Aggregation 10M

# Sum 1 Numeric Variable:

## 10M 1N 1D 0G
data <- fread(paste0(Path, "FakeBevData10M.csv"))
set_collapse(nthreads = data.table::getDTthreads(), na.rm = anyNA(num_vars(data)), stable.algo = FALSE, sort = FALSE)

## 10M 3N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsCollapse.csv"))
start <- Sys.time()
collap(data, `Daily Liters` + `Daily Units` + `Daily Margin` ~ Date + Customer + Brand + Category + `Beverage Flavor`, fsum)
end <- Sys.time()
BenchmarkResults[2, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsCollapse.csv"))
rm(list = c("BenchmarkResults","data","end","start"))
gc()


###################################################################################################
###################################################################################################
###################################################################################################
###################################################################################################
###################################################################################################

# Aggregation 100M

# Sum 1 Numeric Variable:

## 100M 1N 1D 0G
data <- fread(paste0(Path, "FakeBevData100M.csv"))
set_collapse(nthreads = data.table::getDTthreads(), na.rm = anyNA(num_vars(data)), stable.algo = FALSE, sort = FALSE)

## 100M 3N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsCollapse.csv"))
start <- Sys.time()
collap(data, `Daily Liters` + `Daily Units` + `Daily Margin` ~ Date + Customer + Brand + Category + `Beverage Flavor`, fsum)
end <- Sys.time()
BenchmarkResults[3, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsCollapse.csv"))
rm(list = c("BenchmarkResults","data","end","start"))
gc()

###################################################################################################
###################################################################################################
###################################################################################################
###################################################################################################
###################################################################################################

# Aggregation 1B

# Sum 1 Numeric Variable:

## 1B 1N 1D 0G
data <- fread(paste0(Path, "FakeBevData1B.csv"))
set_collapse(nthreads = data.table::getDTthreads(), na.rm = anyNA(num_vars(data)), stable.algo = FALSE, sort = FALSE)

## 1B 3N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsCollapse.csv"))
start <- Sys.time()
collap(data, `Daily Liters` + `Daily Units` + `Daily Margin` ~ Date + Customer + Brand + Category + `Beverage Flavor`, fsum)
end <- Sys.time()
BenchmarkResults[4, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsCollapse.csv"))
rm(list = c("BenchmarkResults","data","end","start"))
gc()
