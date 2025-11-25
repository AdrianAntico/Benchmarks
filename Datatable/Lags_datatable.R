# Path to data storage
Path <- "C:/Users/Bizon/Documents/GitHub/rappwd/"

# Create results table
BenchmarkResults <- data.table::data.table(
  Framework = 'data.table',
  Method = 'lags',
  Experiment = c(
    "1M 3N 1D 4G 5L",
    "10M 3N 1D 4G 5L",
    "100M 3N 1D 4G 5L"
  ),

  TimeInSeconds = c(rep(-0.1, 3))
)

data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Lags.csv"))
rm(BenchmarkResults)

library(data.table)
# setDTthreads(percent = 100)


# Aggregation 1M

# Sum 1 Numeric Variable:
data <- fread(paste0(Path, "FakeBevData1M.csv"))
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Lags.csv"))
setorderv(x = data, cols = c("Customer","Brand","Category","Beverage Flavor", "Date"), order = c(1,1,1,1,1))

## 1M 3N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Lags.csv"))
start <- Sys.time()
data[, c(paste0("Lag Daily Liters ", 1L:5L), paste0("Lag Daily Units ", 1L:5L), paste0("Lag Daily Margin ", 1L:5L)) := shift(x = list(`Daily Liters`,`Daily Units`,`Daily Margin`), n = 1L:5L), by = list(Customer,Brand,Category,`Beverage Flavor`)]
end <- Sys.time()
data.table::set(data, j = c(paste0("Lag Daily Liters ", 1L:5L), paste0("Lag Daily Units ", 1L:5L)), value = NULL)
BenchmarkResults[1, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Lags.csv"))
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
setorderv(x = data, cols = c("Customer","Brand","Category","Beverage Flavor", "Date"), order = c(1,1,1,1,1))


## 10M 3N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Lags.csv"))
start <- Sys.time()
data[, c(paste0("Lag Daily Liters ", 1L:5L), paste0("Lag Daily Units ", 1L:5L), paste0("Lag Daily Margin ", 1L:5L)) := shift(x = list(`Daily Liters`,`Daily Units`,`Daily Margin`), n = 1L:5L), by = list(Customer,Brand,Category,`Beverage Flavor`)]
end <- Sys.time()
data.table::set(data, j = c(paste0("Lag Daily Liters ", 1L:5L), paste0("Lag Daily Units ", 1L:5L)), value = NULL)
BenchmarkResults[2, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Lags.csv"))
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
setorderv(x = data, cols = c("Customer","Brand","Category","Beverage Flavor", "Date"), order = c(1,1,1,1,1))


## 100M 3N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Lags.csv"))
start <- Sys.time()
data[, c(paste0("Lag Daily Liters ", 1L:5L), paste0("Lag Daily Units ", 1L:5L), paste0("Lag Daily Margin ", 1L:5L)) := shift(x = list(`Daily Liters`,`Daily Units`,`Daily Margin`), n = 1L:5L), by = list(Customer,Brand,Category,`Beverage Flavor`)]
end <- Sys.time()
data.table::set(data, j = c(paste0("Lag Daily Liters ", 1L:5L), paste0("Lag Daily Units ", 1L:5L)), value = NULL)
BenchmarkResults[3, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Lags.csv"))
rm(list = c("BenchmarkResults","data","end","start"))
gc()

