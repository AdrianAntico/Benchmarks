# Path to data storage
Path <- "C:/Users/Bizon/Documents/GitHub/rappwd/"

# Create results table
BenchmarkResults <- data.table::data.table(
  Framework = 'data.table',
  Method = 'lags',
  Experiment = c(
    "1M 1N 1D 0G 5L",
    "1M 1N 1D 1G 5L",
    "1M 1N 1D 2G 5L",
    "1M 1N 1D 3G 5L",
    "1M 1N 1D 4G 5L",
    "1M 2N 1D 0G 5L",
    "1M 2N 1D 1G 5L",
    "1M 2N 1D 2G 5L",
    "1M 2N 1D 3G 5L",
    "1M 2N 1D 4G 5L",
    "1M 3N 1D 0G 5L",
    "1M 3N 1D 1G 5L",
    "1M 3N 1D 2G 5L",
    "1M 3N 1D 3G 5L",
    "1M 3N 1D 4G 5L",

    "10M 1N 1D 0G 5L",
    "10M 1N 1D 1G 5L",
    "10M 1N 1D 2G 5L",
    "10M 1N 1D 3G 5L",
    "10M 1N 1D 4G 5L",
    "10M 2N 1D 0G 5L",
    "10M 2N 1D 1G 5L",
    "10M 2N 1D 2G 5L",
    "10M 2N 1D 3G 5L",
    "10M 2N 1D 4G 5L",
    "10M 3N 1D 0G 5L",
    "10M 3N 1D 1G 5L",
    "10M 3N 1D 2G 5L",
    "10M 3N 1D 3G 5L",
    "10M 3N 1D 4G 5L",

    "100M 1N 1D 0G 5L",
    "100M 1N 1D 1G 5L",
    "100M 1N 1D 2G 5L",
    "100M 1N 1D 3G 5L",
    "100M 1N 1D 4G 5L",
    "100M 2N 1D 0G 5L",
    "100M 2N 1D 1G 5L",
    "100M 2N 1D 2G 5L",
    "100M 2N 1D 3G 5L",
    "100M 2N 1D 4G 5L",
    "100M 3N 1D 0G 5L",
    "100M 3N 1D 1G 5L",
    "100M 3N 1D 2G 5L",
    "100M 3N 1D 3G 5L",
    "100M 3N 1D 4G 5L",

    "Total Runtime"),

  TimeInSeconds = c(rep(-0.1, 46))
)

data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Lags.csv"))
rm(BenchmarkResults)

library(data.table)
# setDTthreads(percent = 100)


# Aggregation 1M

# Sum 1 Numeric Variable:

## 1M 1N 1D 0G
data <- fread(paste0(Path, "FakeBevData1M.csv"))
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Lags.csv"))
start <- Sys.time()
data[, paste0("Lag Daily Liters ", 1L:5L) := shift(x = `Daily Liters`, n = 1L:5L)]
end <- Sys.time()
data.table::set(data, j = paste0("Lag Daily Liters ", 1L:5L), value = NULL)
BenchmarkResults[1, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Lags.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 1M 1N 1D 1G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Lags.csv"))
start <- Sys.time()
data[, paste0("Lag Daily Liters ", 1L:5L) := shift(x = `Daily Liters`, n = 1L:5L), by = list(Customer)]
end <- Sys.time()
data.table::set(data, j = paste0("Lag Daily Liters ", 1L:5L), value = NULL)
BenchmarkResults[2, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Lags.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 1M 1N 1D 2G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Lags.csv"))
start <- Sys.time()
data[, paste0("Lag Daily Liters ", 1L:5L) := shift(x = `Daily Liters`, n = 1L:5L), by = list(Customer,Brand)]
end <- Sys.time()
data.table::set(data, j = paste0("Lag Daily Liters ", 1L:5L), value = NULL)
BenchmarkResults[3, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Lags.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 1M 1N 1D 3G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Lags.csv"))
start <- Sys.time()
data[, paste0("Lag Daily Liters ", 1L:5L) := shift(x = `Daily Liters`, n = 1L:5L), by = list(Customer,Brand,Category)]
end <- Sys.time()
data.table::set(data, j = paste0("Lag Daily Liters ", 1L:5L), value = NULL)
BenchmarkResults[4, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Lags.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 1M 1N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Lags.csv"))
start <- Sys.time()
data[, paste0("Lag Daily Liters ", 1L:5L) := shift(x = `Daily Liters`, n = 1L:5L), by = list(Customer,Brand,Category,`Beverage Flavor`)]
end <- Sys.time()
data.table::set(data, j = paste0("Lag Daily Liters ", 1L:5L), value = NULL)
BenchmarkResults[5, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Lags.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 1M 2N 1D 0G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Lags.csv"))
start <- Sys.time()
data[, c(paste0("Lag Daily Liters ", 1L:5L), paste0("Lag Daily Units ", 1L:5L)) := shift(x = list(`Daily Liters`,`Daily Units`), n = 1L:5L)]
end <- Sys.time()
data.table::set(data, j = c(paste0("Lag Daily Liters ", 1L:5L), paste0("Lag Daily Units ", 1L:5L)), value = NULL)
BenchmarkResults[6, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Lags.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 1M 2N 1D 1G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Lags.csv"))
start <- Sys.time()
data[, c(paste0("Lag Daily Liters ", 1L:5L), paste0("Lag Daily Units ", 1L:5L)) := shift(x = list(`Daily Liters`,`Daily Units`), n = 1L:5L), by = list(Customer)]
end <- Sys.time()
data.table::set(data, j = c(paste0("Lag Daily Liters ", 1L:5L), paste0("Lag Daily Units ", 1L:5L)), value = NULL)
BenchmarkResults[7, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Lags.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 1M 2N 1D 2G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Lags.csv"))
start <- Sys.time()
data[, c(paste0("Lag Daily Liters ", 1L:5L), paste0("Lag Daily Units ", 1L:5L)) := shift(x = list(`Daily Liters`,`Daily Units`), n = 1L:5L), by = list(Customer,Brand)]
end <- Sys.time()
data.table::set(data, j = c(paste0("Lag Daily Liters ", 1L:5L), paste0("Lag Daily Units ", 1L:5L)), value = NULL)
BenchmarkResults[8, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Lags.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 1M 2N 1D 3G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Lags.csv"))
start <- Sys.time()
data[, c(paste0("Lag Daily Liters ", 1L:5L), paste0("Lag Daily Units ", 1L:5L)) := shift(x = list(`Daily Liters`,`Daily Units`), n = 1L:5L), by = list(Customer,Brand,Category)]
end <- Sys.time()
data.table::set(data, j = c(paste0("Lag Daily Liters ", 1L:5L), paste0("Lag Daily Units ", 1L:5L)), value = NULL)
BenchmarkResults[9, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Lags.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 1M 2N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Lags.csv"))
start <- Sys.time()
data[, c(paste0("Lag Daily Liters ", 1L:5L), paste0("Lag Daily Units ", 1L:5L)) := shift(x = list(`Daily Liters`,`Daily Units`), n = 1L:5L), by = list(Customer,Brand,Category,`Beverage Flavor`)]
end <- Sys.time()
data.table::set(data, j = c(paste0("Lag Daily Liters ", 1L:5L), paste0("Lag Daily Units ", 1L:5L)), value = NULL)
BenchmarkResults[10, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Lags.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 1M 3N 1D 0G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Lags.csv"))
start <- Sys.time()
data[, c(paste0("Lag Daily Liters ", 1L:5L), paste0("Lag Daily Units ", 1L:5L), paste0("Lag Daily Margin ", 1L:5L)) := shift(x = list(`Daily Liters`,`Daily Units`,`Daily Margin`), n = 1L:5L)]
end <- Sys.time()
data.table::set(data, j = c(paste0("Lag Daily Liters ", 1L:5L), paste0("Lag Daily Units ", 1L:5L)), value = NULL)
BenchmarkResults[11, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Lags.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 1M 3N 1D 1G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Lags.csv"))
start <- Sys.time()
data[, c(paste0("Lag Daily Liters ", 1L:5L), paste0("Lag Daily Units ", 1L:5L), paste0("Lag Daily Margin ", 1L:5L)) := shift(x = list(`Daily Liters`,`Daily Units`,`Daily Margin`), n = 1L:5L), by = list(Customer)]
end <- Sys.time()
data.table::set(data, j = c(paste0("Lag Daily Liters ", 1L:5L), paste0("Lag Daily Units ", 1L:5L)), value = NULL)
BenchmarkResults[12, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Lags.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 1M 3N 1D 2G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Lags.csv"))
start <- Sys.time()
data[, c(paste0("Lag Daily Liters ", 1L:5L), paste0("Lag Daily Units ", 1L:5L), paste0("Lag Daily Margin ", 1L:5L)) := shift(x = list(`Daily Liters`,`Daily Units`,`Daily Margin`), n = 1L:5L), by = list(Customer,Brand)]
end <- Sys.time()
data.table::set(data, j = c(paste0("Lag Daily Liters ", 1L:5L), paste0("Lag Daily Units ", 1L:5L)), value = NULL)
BenchmarkResults[13, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Lags.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 1M 3N 1D 3G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Lags.csv"))
start <- Sys.time()
data[, c(paste0("Lag Daily Liters ", 1L:5L), paste0("Lag Daily Units ", 1L:5L), paste0("Lag Daily Margin ", 1L:5L)) := shift(x = list(`Daily Liters`,`Daily Units`,`Daily Margin`), n = 1L:5L), by = list(Customer,Brand,Category)]
end <- Sys.time()
data.table::set(data, j = c(paste0("Lag Daily Liters ", 1L:5L), paste0("Lag Daily Units ", 1L:5L)), value = NULL)
BenchmarkResults[14, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Lags.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 1M 3N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Lags.csv"))
start <- Sys.time()
data[, c(paste0("Lag Daily Liters ", 1L:5L), paste0("Lag Daily Units ", 1L:5L), paste0("Lag Daily Margin ", 1L:5L)) := shift(x = list(`Daily Liters`,`Daily Units`,`Daily Margin`), n = 1L:5L), by = list(Customer,Brand,Category,`Beverage Flavor`)]
end <- Sys.time()
data.table::set(data, j = c(paste0("Lag Daily Liters ", 1L:5L), paste0("Lag Daily Units ", 1L:5L)), value = NULL)
BenchmarkResults[15, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
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

## 10M 1N 1D 0G
data <- fread(paste0(Path, "FakeBevData10M.csv"))
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Lags.csv"))
start <- Sys.time()
data[, paste0("Lag Daily Liters ", 1L:5L) := shift(x = `Daily Liters`, n = 1L:5L)]
end <- Sys.time()
data.table::set(data, j = paste0("Lag Daily Liters ", 1L:5L), value = NULL)
BenchmarkResults[16, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Lags.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 10M 1N 1D 1G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Lags.csv"))
start <- Sys.time()
data[, paste0("Lag Daily Liters ", 1L:5L) := shift(x = `Daily Liters`, n = 1L:5L), by = list(Customer)]
end <- Sys.time()
data.table::set(data, j = paste0("Lag Daily Liters ", 1L:5L), value = NULL)
BenchmarkResults[17, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Lags.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 10M 1N 1D 2G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Lags.csv"))
start <- Sys.time()
data[, paste0("Lag Daily Liters ", 1L:5L) := shift(x = `Daily Liters`, n = 1L:5L), by = list(Customer,Brand)]
end <- Sys.time()
data.table::set(data, j = paste0("Lag Daily Liters ", 1L:5L), value = NULL)
BenchmarkResults[18, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Lags.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 10M 1N 1D 3G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Lags.csv"))
start <- Sys.time()
data[, paste0("Lag Daily Liters ", 1L:5L) := shift(x = `Daily Liters`, n = 1L:5L), by = list(Customer,Brand,Category)]
end <- Sys.time()
data.table::set(data, j = paste0("Lag Daily Liters ", 1L:5L), value = NULL)
BenchmarkResults[19, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Lags.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 10M 1N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Lags.csv"))
start <- Sys.time()
data[, paste0("Lag Daily Liters ", 1L:5L) := shift(x = `Daily Liters`, n = 1L:5L), by = list(Customer,Brand,Category,`Beverage Flavor`)]
end <- Sys.time()
data.table::set(data, j = paste0("Lag Daily Liters ", 1L:5L), value = NULL)
BenchmarkResults[20, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Lags.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 10M 2N 1D 0G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Lags.csv"))
start <- Sys.time()
data[, c(paste0("Lag Daily Liters ", 1L:5L), paste0("Lag Daily Units ", 1L:5L)) := shift(x = list(`Daily Liters`,`Daily Units`), n = 1L:5L)]
end <- Sys.time()
data.table::set(data, j = c(paste0("Lag Daily Liters ", 1L:5L), paste0("Lag Daily Units ", 1L:5L)), value = NULL)
BenchmarkResults[21, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Lags.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 10M 2N 1D 1G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Lags.csv"))
start <- Sys.time()
data[, c(paste0("Lag Daily Liters ", 1L:5L), paste0("Lag Daily Units ", 1L:5L)) := shift(x = list(`Daily Liters`,`Daily Units`), n = 1L:5L), by = list(Customer)]
end <- Sys.time()
data.table::set(data, j = c(paste0("Lag Daily Liters ", 1L:5L), paste0("Lag Daily Units ", 1L:5L)), value = NULL)
BenchmarkResults[22, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Lags.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 10M 2N 1D 2G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Lags.csv"))
start <- Sys.time()
data[, c(paste0("Lag Daily Liters ", 1L:5L), paste0("Lag Daily Units ", 1L:5L)) := shift(x = list(`Daily Liters`,`Daily Units`), n = 1L:5L), by = list(Customer,Brand)]
end <- Sys.time()
data.table::set(data, j = c(paste0("Lag Daily Liters ", 1L:5L), paste0("Lag Daily Units ", 1L:5L)), value = NULL)
BenchmarkResults[23, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Lags.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 10M 2N 1D 3G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Lags.csv"))
start <- Sys.time()
data[, c(paste0("Lag Daily Liters ", 1L:5L), paste0("Lag Daily Units ", 1L:5L)) := shift(x = list(`Daily Liters`,`Daily Units`), n = 1L:5L), by = list(Customer,Brand,Category)]
end <- Sys.time()
data.table::set(data, j = c(paste0("Lag Daily Liters ", 1L:5L), paste0("Lag Daily Units ", 1L:5L)), value = NULL)
BenchmarkResults[24, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Lags.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 10M 2N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Lags.csv"))
start <- Sys.time()
data[, c(paste0("Lag Daily Liters ", 1L:5L), paste0("Lag Daily Units ", 1L:5L)) := shift(x = list(`Daily Liters`,`Daily Units`), n = 1L:5L), by = list(Customer,Brand,Category,`Beverage Flavor`)]
end <- Sys.time()
data.table::set(data, j = c(paste0("Lag Daily Liters ", 1L:5L), paste0("Lag Daily Units ", 1L:5L)), value = NULL)
BenchmarkResults[25, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Lags.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 10M 3N 1D 0G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Lags.csv"))
start <- Sys.time()
data[, c(paste0("Lag Daily Liters ", 1L:5L), paste0("Lag Daily Units ", 1L:5L), paste0("Lag Daily Margin ", 1L:5L)) := shift(x = list(`Daily Liters`,`Daily Units`,`Daily Margin`), n = 1L:5L)]
end <- Sys.time()
data.table::set(data, j = c(paste0("Lag Daily Liters ", 1L:5L), paste0("Lag Daily Units ", 1L:5L)), value = NULL)
BenchmarkResults[26, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Lags.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 10M 3N 1D 1G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Lags.csv"))
start <- Sys.time()
data[, c(paste0("Lag Daily Liters ", 1L:5L), paste0("Lag Daily Units ", 1L:5L), paste0("Lag Daily Margin ", 1L:5L)) := shift(x = list(`Daily Liters`,`Daily Units`,`Daily Margin`), n = 1L:5L), by = list(Customer)]
end <- Sys.time()
data.table::set(data, j = c(paste0("Lag Daily Liters ", 1L:5L), paste0("Lag Daily Units ", 1L:5L)), value = NULL)
BenchmarkResults[27, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Lags.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 10M 3N 1D 2G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Lags.csv"))
start <- Sys.time()
data[, c(paste0("Lag Daily Liters ", 1L:5L), paste0("Lag Daily Units ", 1L:5L), paste0("Lag Daily Margin ", 1L:5L)) := shift(x = list(`Daily Liters`,`Daily Units`,`Daily Margin`), n = 1L:5L), by = list(Customer,Brand)]
end <- Sys.time()
data.table::set(data, j = c(paste0("Lag Daily Liters ", 1L:5L), paste0("Lag Daily Units ", 1L:5L)), value = NULL)
BenchmarkResults[28, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Lags.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 10M 3N 1D 3G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Lags.csv"))
start <- Sys.time()
data[, c(paste0("Lag Daily Liters ", 1L:5L), paste0("Lag Daily Units ", 1L:5L), paste0("Lag Daily Margin ", 1L:5L)) := shift(x = list(`Daily Liters`,`Daily Units`,`Daily Margin`), n = 1L:5L), by = list(Customer,Brand,Category)]
end <- Sys.time()
data.table::set(data, j = c(paste0("Lag Daily Liters ", 1L:5L), paste0("Lag Daily Units ", 1L:5L)), value = NULL)
BenchmarkResults[29, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Lags.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 10M 3N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Lags.csv"))
start <- Sys.time()
data[, c(paste0("Lag Daily Liters ", 1L:5L), paste0("Lag Daily Units ", 1L:5L), paste0("Lag Daily Margin ", 1L:5L)) := shift(x = list(`Daily Liters`,`Daily Units`,`Daily Margin`), n = 1L:5L), by = list(Customer,Brand,Category,`Beverage Flavor`)]
end <- Sys.time()
data.table::set(data, j = c(paste0("Lag Daily Liters ", 1L:5L), paste0("Lag Daily Units ", 1L:5L)), value = NULL)
BenchmarkResults[30, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
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

## 100M 1N 1D 0G
data <- fread(paste0(Path, "FakeBevData100M.csv"))
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Lags.csv"))
start <- Sys.time()
data[, paste0("Lag Daily Liters ", 1L:5L) := shift(x = `Daily Liters`, n = 1L:5L)]
end <- Sys.time()
data.table::set(data, j = paste0("Lag Daily Liters ", 1L:5L), value = NULL)
BenchmarkResults[31, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Lags.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 100M 1N 1D 1G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Lags.csv"))
start <- Sys.time()
data[, paste0("Lag Daily Liters ", 1L:5L) := shift(x = `Daily Liters`, n = 1L:5L), by = list(Customer)]
end <- Sys.time()
data.table::set(data, j = paste0("Lag Daily Liters ", 1L:5L), value = NULL)
BenchmarkResults[32, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Lags.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 100M 1N 1D 2G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Lags.csv"))
start <- Sys.time()
data[, paste0("Lag Daily Liters ", 1L:5L) := shift(x = `Daily Liters`, n = 1L:5L), by = list(Customer,Brand)]
end <- Sys.time()
data.table::set(data, j = paste0("Lag Daily Liters ", 1L:5L), value = NULL)
BenchmarkResults[33, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Lags.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 100M 1N 1D 3G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Lags.csv"))
start <- Sys.time()
data[, paste0("Lag Daily Liters ", 1L:5L) := shift(x = `Daily Liters`, n = 1L:5L), by = list(Customer,Brand,Category)]
end <- Sys.time()
data.table::set(data, j = paste0("Lag Daily Liters ", 1L:5L), value = NULL)
BenchmarkResults[34, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Lags.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 100M 1N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Lags.csv"))
start <- Sys.time()
data[, paste0("Lag Daily Liters ", 1L:5L) := shift(x = `Daily Liters`, n = 1L:5L), by = list(Customer,Brand,Category,`Beverage Flavor`)]
end <- Sys.time()
data.table::set(data, j = paste0("Lag Daily Liters ", 1L:5L), value = NULL)
BenchmarkResults[35, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Lags.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 100M 2N 1D 0G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Lags.csv"))
start <- Sys.time()
data[, c(paste0("Lag Daily Liters ", 1L:5L), paste0("Lag Daily Units ", 1L:5L)) := shift(x = list(`Daily Liters`,`Daily Units`), n = 1L:5L)]
end <- Sys.time()
data.table::set(data, j = c(paste0("Lag Daily Liters ", 1L:5L), paste0("Lag Daily Units ", 1L:5L)), value = NULL)
BenchmarkResults[36, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Lags.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 100M 2N 1D 1G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Lags.csv"))
start <- Sys.time()
data[, c(paste0("Lag Daily Liters ", 1L:5L), paste0("Lag Daily Units ", 1L:5L)) := shift(x = list(`Daily Liters`,`Daily Units`), n = 1L:5L), by = list(Customer)]
end <- Sys.time()
data.table::set(data, j = c(paste0("Lag Daily Liters ", 1L:5L), paste0("Lag Daily Units ", 1L:5L)), value = NULL)
BenchmarkResults[37, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Lags.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 100M 2N 1D 2G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Lags.csv"))
start <- Sys.time()
data[, c(paste0("Lag Daily Liters ", 1L:5L), paste0("Lag Daily Units ", 1L:5L)) := shift(x = list(`Daily Liters`,`Daily Units`), n = 1L:5L), by = list(Customer,Brand)]
end <- Sys.time()
data.table::set(data, j = c(paste0("Lag Daily Liters ", 1L:5L), paste0("Lag Daily Units ", 1L:5L)), value = NULL)
BenchmarkResults[38, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Lags.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 100M 2N 1D 3G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Lags.csv"))
start <- Sys.time()
data[, c(paste0("Lag Daily Liters ", 1L:5L), paste0("Lag Daily Units ", 1L:5L)) := shift(x = list(`Daily Liters`,`Daily Units`), n = 1L:5L), by = list(Customer,Brand,Category)]
end <- Sys.time()
data.table::set(data, j = c(paste0("Lag Daily Liters ", 1L:5L), paste0("Lag Daily Units ", 1L:5L)), value = NULL)
BenchmarkResults[39, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Lags.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 100M 2N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Lags.csv"))
start <- Sys.time()
data[, c(paste0("Lag Daily Liters ", 1L:5L), paste0("Lag Daily Units ", 1L:5L)) := shift(x = list(`Daily Liters`,`Daily Units`), n = 1L:5L), by = list(Customer,Brand,Category,`Beverage Flavor`)]
end <- Sys.time()
data.table::set(data, j = c(paste0("Lag Daily Liters ", 1L:5L), paste0("Lag Daily Units ", 1L:5L)), value = NULL)
BenchmarkResults[40, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Lags.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 100M 3N 1D 0G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Lags.csv"))
start <- Sys.time()
data[, c(paste0("Lag Daily Liters ", 1L:5L), paste0("Lag Daily Units ", 1L:5L), paste0("Lag Daily Margin ", 1L:5L)) := shift(x = list(`Daily Liters`,`Daily Units`,`Daily Margin`), n = 1L:5L)]
end <- Sys.time()
data.table::set(data, j = c(paste0("Lag Daily Liters ", 1L:5L), paste0("Lag Daily Units ", 1L:5L)), value = NULL)
BenchmarkResults[41, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Lags.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 100M 3N 1D 1G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Lags.csv"))
start <- Sys.time()
data[, c(paste0("Lag Daily Liters ", 1L:5L), paste0("Lag Daily Units ", 1L:5L), paste0("Lag Daily Margin ", 1L:5L)) := shift(x = list(`Daily Liters`,`Daily Units`,`Daily Margin`), n = 1L:5L), by = list(Customer)]
end <- Sys.time()
data.table::set(data, j = c(paste0("Lag Daily Liters ", 1L:5L), paste0("Lag Daily Units ", 1L:5L)), value = NULL)
BenchmarkResults[42, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Lags.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 100M 3N 1D 2G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Lags.csv"))
start <- Sys.time()
data[, c(paste0("Lag Daily Liters ", 1L:5L), paste0("Lag Daily Units ", 1L:5L), paste0("Lag Daily Margin ", 1L:5L)) := shift(x = list(`Daily Liters`,`Daily Units`,`Daily Margin`), n = 1L:5L), by = list(Customer,Brand)]
end <- Sys.time()
data.table::set(data, j = c(paste0("Lag Daily Liters ", 1L:5L), paste0("Lag Daily Units ", 1L:5L)), value = NULL)
BenchmarkResults[43, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Lags.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 100M 3N 1D 3G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Lags.csv"))
start <- Sys.time()
data[, c(paste0("Lag Daily Liters ", 1L:5L), paste0("Lag Daily Units ", 1L:5L), paste0("Lag Daily Margin ", 1L:5L)) := shift(x = list(`Daily Liters`,`Daily Units`,`Daily Margin`), n = 1L:5L), by = list(Customer,Brand,Category)]
end <- Sys.time()
data.table::set(data, j = c(paste0("Lag Daily Liters ", 1L:5L), paste0("Lag Daily Units ", 1L:5L)), value = NULL)
BenchmarkResults[44, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Lags.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 100M 3N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Lags.csv"))
start <- Sys.time()
data[, c(paste0("Lag Daily Liters ", 1L:5L), paste0("Lag Daily Units ", 1L:5L), paste0("Lag Daily Margin ", 1L:5L)) := shift(x = list(`Daily Liters`,`Daily Units`,`Daily Margin`), n = 1L:5L), by = list(Customer,Brand,Category,`Beverage Flavor`)]
end <- Sys.time()
data.table::set(data, j = c(paste0("Lag Daily Liters ", 1L:5L), paste0("Lag Daily Units ", 1L:5L)), value = NULL)
BenchmarkResults[45, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Lags.csv"))
rm(list = c("BenchmarkResults","data","end","start"))
gc()


BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Lags.csv"))
BenchmarkResults[46, TimeInSeconds := BenchmarkResults[1:45, sum(TimeInSeconds)]]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Lags.csv"))



