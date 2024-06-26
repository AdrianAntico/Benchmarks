# Path to data storage
Path <- "C:/Users/Bizon/Documents/GitHub/rappwd/"

# Create results table
BenchmarkResults <- data.table::data.table(
  Framework = 'data.table',
  Method = 'sum aggregation',
  Experiment = c(
    "1M 1N 1D 0G",
    "1M 1N 1D 1G",
    "1M 1N 1D 2G",
    "1M 1N 1D 3G",
    "1M 1N 1D 4G",
    "1M 2N 1D 0G",
    "1M 2N 1D 1G",
    "1M 2N 1D 2G",
    "1M 2N 1D 3G",
    "1M 2N 1D 4G",
    "1M 3N 1D 0G",
    "1M 3N 1D 1G",
    "1M 3N 1D 2G",
    "1M 3N 1D 3G",
    "1M 3N 1D 4G",

    "10M 1N 1D 0G",
    "10M 1N 1D 1G",
    "10M 1N 1D 2G",
    "10M 1N 1D 3G",
    "10M 1N 1D 4G",
    "10M 2N 1D 0G",
    "10M 2N 1D 1G",
    "10M 2N 1D 2G",
    "10M 2N 1D 3G",
    "10M 2N 1D 4G",
    "10M 3N 1D 0G",
    "10M 3N 1D 1G",
    "10M 3N 1D 2G",
    "10M 3N 1D 3G",
    "10M 3N 1D 4G",

    "100M 1N 1D 0G",
    "100M 1N 1D 1G",
    "100M 1N 1D 2G",
    "100M 1N 1D 3G",
    "100M 1N 1D 4G",
    "100M 2N 1D 0G",
    "100M 2N 1D 1G",
    "100M 2N 1D 2G",
    "100M 2N 1D 3G",
    "100M 2N 1D 4G",
    "100M 3N 1D 0G",
    "100M 3N 1D 1G",
    "100M 3N 1D 2G",
    "100M 3N 1D 3G",
    "100M 3N 1D 4G",

    "1B 1N 1D 0G",
    "1B 1N 1D 1G",
    "1B 1N 1D 2G",
    "1B 1N 1D 3G",
    "1B 1N 1D 4G",
    "1B 2N 1D 0G",
    "1B 2N 1D 1G",
    "1B 2N 1D 2G",
    "1B 2N 1D 3G",
    "1B 2N 1D 4G",
    "1B 3N 1D 0G",
    "1B 3N 1D 1G",
    "1B 3N 1D 2G",
    "1B 3N 1D 3G",
    "1B 3N 1D 4G",
    "Total Runtime"),

  TimeInSeconds = c(rep(-0.1, 61))
)

data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults.csv"))
rm(BenchmarkResults)

library(data.table)
# setDTthreads(percent = 100)


# Aggregation 1M

# Sum 1 Numeric Variable:

## 1M 1N 1D 0G
data <- fread(paste0(Path, "FakeBevData1M.csv"))
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults.csv"))
start <- Sys.time()
data[, .(v1 = sum(`Daily Liters`, na.rm = TRUE)), by = Date]
end <- Sys.time()
BenchmarkResults[1, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 1M 1N 1D 1G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults.csv"))
start <- Sys.time()
data[, .(v1 = sum(`Daily Liters`, na.rm = TRUE)), by = list(Date, Customer)]
end <- Sys.time()
BenchmarkResults[2, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 1M 1N 1D 2G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults.csv"))
start <- Sys.time()
data[, .(v1 = sum(`Daily Liters`, na.rm = TRUE)), by = list(Date, Customer, Brand)]
end <- Sys.time()
BenchmarkResults[3, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 1M 1N 1D 3G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults.csv"))
start <- Sys.time()
data[, .(v1 = sum(`Daily Liters`, na.rm = TRUE)), by = list(Date, Customer, Brand, Category)]
end <- Sys.time()
BenchmarkResults[4, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 1M 1N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults.csv"))
start <- Sys.time()
data[, .(v1 = sum(`Daily Liters`, na.rm = TRUE)), by = list(Date, Customer, Brand, Category, `Beverage Flavor`)]
end <- Sys.time()
BenchmarkResults[5, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 1M 2N 1D 0G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults.csv"))
start <- Sys.time()
data[, .(v1 = sum(`Daily Liters`, na.rm = TRUE), v2 = sum(`Daily Units`, na.rm = TRUE)), by = Date]
end <- Sys.time()
BenchmarkResults[6, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 1M 2N 1D 1G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults.csv"))
start <- Sys.time()
data[, .(v1 = sum(`Daily Liters`, na.rm = TRUE), v2 = sum(`Daily Units`, na.rm = TRUE)), by = list(Date,Customer)]
end <- Sys.time()
BenchmarkResults[7, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 1M 2N 1D 2G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults.csv"))
start <- Sys.time()
data[, .(v1 = sum(`Daily Liters`, na.rm = TRUE), v2 = sum(`Daily Units`, na.rm = TRUE)), by = list(Date,Customer,Brand)]
end <- Sys.time()
BenchmarkResults[8, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 1M 2N 1D 3G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults.csv"))
start <- Sys.time()
data[, .(v1 = sum(`Daily Liters`, na.rm = TRUE), v2 = sum(`Daily Units`, na.rm = TRUE)), by = list(Date,Customer,Brand,Category)]
end <- Sys.time()
BenchmarkResults[9, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 1M 2N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults.csv"))
start <- Sys.time()
data[, .(v1 = sum(`Daily Liters`, na.rm = TRUE), v2 = sum(`Daily Units`, na.rm = TRUE)), by = list(Date,Customer,Brand,Category,`Beverage Flavor`)]
end <- Sys.time()
BenchmarkResults[10, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 1M 3N 1D 0G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults.csv"))
start <- Sys.time()
data[, .(v1 = sum(`Daily Liters`, na.rm = TRUE), v2 = sum(`Daily Units`, na.rm = TRUE), v3 = sum(`Daily Margin`, na.rm = TRUE)), by = Date]
end <- Sys.time()
BenchmarkResults[11, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 1M 3N 1D 1G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults.csv"))
start <- Sys.time()
data[, .(v1 = sum(`Daily Liters`, na.rm = TRUE), v2 = sum(`Daily Units`, na.rm = TRUE), v3 = sum(`Daily Margin`, na.rm = TRUE)), by = list(Date,Customer)]
end <- Sys.time()
BenchmarkResults[12, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 1M 3N 1D 2G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults.csv"))
start <- Sys.time()
data[, .(v1 = sum(`Daily Liters`, na.rm = TRUE), v2 = sum(`Daily Units`, na.rm = TRUE), v3 = sum(`Daily Margin`, na.rm = TRUE)), by = list(Date,Customer,Brand)]
end <- Sys.time()
BenchmarkResults[13, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 1M 3N 1D 3G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults.csv"))
start <- Sys.time()
data[, .(v1 = sum(`Daily Liters`, na.rm = TRUE), v2 = sum(`Daily Units`, na.rm = TRUE), v3 = sum(`Daily Margin`, na.rm = TRUE)), by = list(Date,Customer,Brand,Category)]
end <- Sys.time()
BenchmarkResults[14, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 1M 3N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults.csv"))
start <- Sys.time()
data[, .(v1 = sum(`Daily Liters`, na.rm = TRUE), v2 = sum(`Daily Units`, na.rm = TRUE), v3 = sum(`Daily Margin`, na.rm = TRUE)), by = list(Date,Customer,Brand,Category,`Beverage Flavor`)]
end <- Sys.time()
BenchmarkResults[15, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults.csv"))
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
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults.csv"))
start <- Sys.time()
data[, sum(`Daily Liters`), by = Date]
end <- Sys.time()
BenchmarkResults[16, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 10M 1N 1D 1G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults.csv"))
start <- Sys.time()
data[, sum(`Daily Liters`), by = list(Date, Customer)]
end <- Sys.time()
BenchmarkResults[17, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 10M 1N 1D 2G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults.csv"))
start <- Sys.time()
data[, sum(`Daily Liters`), by = list(Date, Customer, Brand)]
end <- Sys.time()
BenchmarkResults[18, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 10M 1N 1D 3G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults.csv"))
start <- Sys.time()
data[, sum(`Daily Liters`), by = list(Date, Customer, Brand, Category)]
end <- Sys.time()
BenchmarkResults[19, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 10M 1N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults.csv"))
start <- Sys.time()
data[, sum(`Daily Liters`), by = list(Date, Customer, Brand, Category, `Beverage Flavor`)]
end <- Sys.time()
BenchmarkResults[20, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 10M 2N 1D 0G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults.csv"))
start <- Sys.time()
data[, .(v1 = sum(`Daily Liters`, na.rm = TRUE), v2 = sum(`Daily Units`, na.rm = TRUE)), by = Date]
end <- Sys.time()
BenchmarkResults[21, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 10M 2N 1D 1G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults.csv"))
start <- Sys.time()
data[, .(v1 = sum(`Daily Liters`, na.rm = TRUE), v2 = sum(`Daily Units`, na.rm = TRUE)), by = list(Date,Customer)]
end <- Sys.time()
BenchmarkResults[22, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 10M 2N 1D 2G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults.csv"))
start <- Sys.time()
data[, .(v1 = sum(`Daily Liters`, na.rm = TRUE), v2 = sum(`Daily Units`, na.rm = TRUE)), by = list(Date,Customer,Brand)]
end <- Sys.time()
BenchmarkResults[23, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 10M 2N 1D 3G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults.csv"))
start <- Sys.time()
data[, .(v1 = sum(`Daily Liters`, na.rm = TRUE), v2 = sum(`Daily Units`, na.rm = TRUE)), by = list(Date,Customer,Brand,Category)]
end <- Sys.time()
BenchmarkResults[24, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 10M 2N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults.csv"))
start <- Sys.time()
data[, .(v1 = sum(`Daily Liters`, na.rm = TRUE), v2 = sum(`Daily Units`, na.rm = TRUE)), by = list(Date,Customer,Brand,Category,`Beverage Flavor`)]
end <- Sys.time()
BenchmarkResults[25, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 10M 3N 1D 0G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults.csv"))
start <- Sys.time()
data[, .(v1 = sum(`Daily Liters`, na.rm = TRUE), v2 = sum(`Daily Units`, na.rm = TRUE), v3 = sum(`Daily Margin`, na.rm = TRUE)), by = Date]
end <- Sys.time()
BenchmarkResults[26, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 10M 3N 1D 1G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults.csv"))
start <- Sys.time()
data[, .(v1 = sum(`Daily Liters`, na.rm = TRUE), v2 = sum(`Daily Units`, na.rm = TRUE), v3 = sum(`Daily Margin`, na.rm = TRUE)), by = list(Date,Customer)]
end <- Sys.time()
BenchmarkResults[27, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 10M 3N 1D 2G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults.csv"))
start <- Sys.time()
data[, .(v1 = sum(`Daily Liters`, na.rm = TRUE), v2 = sum(`Daily Units`, na.rm = TRUE), v3 = sum(`Daily Margin`, na.rm = TRUE)), by = list(Date,Customer,Brand)]
end <- Sys.time()
BenchmarkResults[28, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 10M 3N 1D 3G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults.csv"))
start <- Sys.time()
data[, .(v1 = sum(`Daily Liters`, na.rm = TRUE), v2 = sum(`Daily Units`, na.rm = TRUE), v3 = sum(`Daily Margin`, na.rm = TRUE)), by = list(Date,Customer,Brand,Category)]
end <- Sys.time()
BenchmarkResults[29, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 10M 3N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults.csv"))
start <- Sys.time()
data[, .(v1 = sum(`Daily Liters`, na.rm = TRUE), v2 = sum(`Daily Units`, na.rm = TRUE), v3 = sum(`Daily Margin`, na.rm = TRUE)), by = list(Date,Customer,Brand,Category,`Beverage Flavor`)]
end <- Sys.time()
BenchmarkResults[30, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults.csv"))
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
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults.csv"))
start <- Sys.time()
data[, sum(`Daily Liters`), by = Date]
end <- Sys.time()
BenchmarkResults[31, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 100M 1N 1D 1G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults.csv"))
start <- Sys.time()
data[, sum(`Daily Liters`), by = list(Date, Customer)]
end <- Sys.time()
BenchmarkResults[32, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 100M 1N 1D 2G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults.csv"))
start <- Sys.time()
data[, sum(`Daily Liters`), by = list(Date, Customer, Brand)]
end <- Sys.time()
BenchmarkResults[33, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 100M 1N 1D 3G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults.csv"))
start <- Sys.time()
data[, sum(`Daily Liters`), by = list(Date, Customer, Brand, Category)]
end <- Sys.time()
BenchmarkResults[34, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 100M 1N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults.csv"))
start <- Sys.time()
data[, sum(`Daily Liters`), by = list(Date, Customer, Brand, Category, `Beverage Flavor`)]
end <- Sys.time()
BenchmarkResults[35, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 100M 2N 1D 0G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults.csv"))
start <- Sys.time()
data[, .(v1 = sum(`Daily Liters`, na.rm = TRUE), v2 = sum(`Daily Units`, na.rm = TRUE)), by = Date]
end <- Sys.time()
BenchmarkResults[36, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 100M 2N 1D 1G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults.csv"))
start <- Sys.time()
data[, .(v1 = sum(`Daily Liters`, na.rm = TRUE), v2 = sum(`Daily Units`, na.rm = TRUE)), by = list(Date,Customer)]
end <- Sys.time()
BenchmarkResults[37, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 100M 2N 1D 2G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults.csv"))
start <- Sys.time()
data[, .(v1 = sum(`Daily Liters`, na.rm = TRUE), v2 = sum(`Daily Units`, na.rm = TRUE)), by = list(Date,Customer,Brand)]
end <- Sys.time()
BenchmarkResults[38, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 100M 2N 1D 3G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults.csv"))
start <- Sys.time()
data[, .(v1 = sum(`Daily Liters`, na.rm = TRUE), v2 = sum(`Daily Units`, na.rm = TRUE)), by = list(Date,Customer,Brand,Category)]
end <- Sys.time()
BenchmarkResults[39, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 100M 2N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults.csv"))
start <- Sys.time()
data[, .(v1 = sum(`Daily Liters`, na.rm = TRUE), v2 = sum(`Daily Units`, na.rm = TRUE)), by = list(Date,Customer,Brand,Category,`Beverage Flavor`)]
end <- Sys.time()
BenchmarkResults[40, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 100M 3N 1D 0G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults.csv"))
start <- Sys.time()
data[, .(v1 = sum(`Daily Liters`, na.rm = TRUE), v2 = sum(`Daily Units`, na.rm = TRUE), v3 = sum(`Daily Margin`, na.rm = TRUE)), by = Date]
end <- Sys.time()
BenchmarkResults[41, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 100M 3N 1D 1G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults.csv"))
start <- Sys.time()
data[, .(v1 = sum(`Daily Liters`, na.rm = TRUE), v2 = sum(`Daily Units`, na.rm = TRUE), v3 = sum(`Daily Margin`, na.rm = TRUE)), by = list(Date,Customer)]
end <- Sys.time()
BenchmarkResults[42, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 100M 3N 1D 2G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults.csv"))
start <- Sys.time()
data[, .(v1 = sum(`Daily Liters`, na.rm = TRUE), v2 = sum(`Daily Units`, na.rm = TRUE), v3 = sum(`Daily Margin`, na.rm = TRUE)), by = list(Date,Customer,Brand)]
end <- Sys.time()
BenchmarkResults[43, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 100M 3N 1D 3G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults.csv"))
start <- Sys.time()
data[, .(v1 = sum(`Daily Liters`, na.rm = TRUE), v2 = sum(`Daily Units`, na.rm = TRUE), v3 = sum(`Daily Margin`, na.rm = TRUE)), by = list(Date,Customer,Brand,Category)]
end <- Sys.time()
BenchmarkResults[44, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 100M 3N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults.csv"))
start <- Sys.time()
data[, .(v1 = sum(`Daily Liters`, na.rm = TRUE), v2 = sum(`Daily Units`, na.rm = TRUE), v3 = sum(`Daily Margin`, na.rm = TRUE)), by = list(Date,Customer,Brand,Category,`Beverage Flavor`)]
end <- Sys.time()
BenchmarkResults[45, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults.csv"))
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
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults.csv"))
start <- Sys.time()
data[, sum(`Daily Liters`), by = Date]
end <- Sys.time()
BenchmarkResults[46, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 1B 1N 1D 1G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults.csv"))
start <- Sys.time()
data[, sum(`Daily Liters`), by = list(Date, Customer)]
end <- Sys.time()
BenchmarkResults[47, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 1B 1N 1D 2G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults.csv"))
start <- Sys.time()
data[, sum(`Daily Liters`), by = list(Date, Customer, Brand)]
end <- Sys.time()
BenchmarkResults[48, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 1B 1N 1D 3G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults.csv"))
start <- Sys.time()
data[, sum(`Daily Liters`), by = list(Date, Customer, Brand, Category)]
end <- Sys.time()
BenchmarkResults[49, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 1B 1N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults.csv"))
start <- Sys.time()
data[, sum(`Daily Liters`), by = list(Date, Customer, Brand, Category, `Beverage Flavor`)]
end <- Sys.time()
BenchmarkResults[50, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 1B 2N 1D 0G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults.csv"))
start <- Sys.time()
data[, .(v1 = sum(`Daily Liters`, na.rm = TRUE), v2 = sum(`Daily Units`, na.rm = TRUE)), by = Date]
end <- Sys.time()
BenchmarkResults[51, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 1B 2N 1D 1G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults.csv"))
start <- Sys.time()
data[, .(v1 = sum(`Daily Liters`, na.rm = TRUE), v2 = sum(`Daily Units`, na.rm = TRUE)), by = list(Date,Customer)]
end <- Sys.time()
BenchmarkResults[52, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 1B 2N 1D 2G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults.csv"))
start <- Sys.time()
data[, .(v1 = sum(`Daily Liters`, na.rm = TRUE), v2 = sum(`Daily Units`, na.rm = TRUE)), by = list(Date,Customer,Brand)]
end <- Sys.time()
BenchmarkResults[53, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 1B 2N 1D 3G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults.csv"))
start <- Sys.time()
data[, .(v1 = sum(`Daily Liters`, na.rm = TRUE), v2 = sum(`Daily Units`, na.rm = TRUE)), by = list(Date,Customer,Brand,Category)]
end <- Sys.time()
BenchmarkResults[54, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 1B 2N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults.csv"))
start <- Sys.time()
data[, .(v1 = sum(`Daily Liters`, na.rm = TRUE), v2 = sum(`Daily Units`, na.rm = TRUE)), by = list(Date,Customer,Brand,Category,`Beverage Flavor`)]
end <- Sys.time()
BenchmarkResults[55, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 1B 3N 1D 0G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults.csv"))
start <- Sys.time()
data[, .(v1 = sum(`Daily Liters`, na.rm = TRUE), v2 = sum(`Daily Units`, na.rm = TRUE), v3 = sum(`Daily Margin`, na.rm = TRUE)), by = Date]
end <- Sys.time()
BenchmarkResults[56, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 1B 3N 1D 1G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults.csv"))
start <- Sys.time()
data[, .(v1 = sum(`Daily Liters`, na.rm = TRUE), v2 = sum(`Daily Units`, na.rm = TRUE), v3 = sum(`Daily Margin`, na.rm = TRUE)), by = list(Date,Customer)]
end <- Sys.time()
BenchmarkResults[57, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 1B 3N 1D 2G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults.csv"))
start <- Sys.time()
data[, .(v1 = sum(`Daily Liters`, na.rm = TRUE), v2 = sum(`Daily Units`, na.rm = TRUE), v3 = sum(`Daily Margin`, na.rm = TRUE)), by = list(Date,Customer,Brand)]
end <- Sys.time()
BenchmarkResults[58, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 1B 3N 1D 3G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults.csv"))
start <- Sys.time()
data[, .(v1 = sum(`Daily Liters`, na.rm = TRUE), v2 = sum(`Daily Units`, na.rm = TRUE), v3 = sum(`Daily Margin`, na.rm = TRUE)), by = list(Date,Customer,Brand,Category)]
end <- Sys.time()
BenchmarkResults[59, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 1B 3N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults.csv"))
start <- Sys.time()
data[, .(v1 = sum(`Daily Liters`, na.rm = TRUE), v2 = sum(`Daily Units`, na.rm = TRUE), v3 = sum(`Daily Margin`, na.rm = TRUE)), by = list(Date,Customer,Brand,Category,`Beverage Flavor`)]
end <- Sys.time()
BenchmarkResults[60, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults.csv"))
rm(list = c("BenchmarkResults","data","end","start"))
gc()


BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults.csv"))
BenchmarkResults[61, TimeInSeconds := BenchmarkResults[1:60, sum(TimeInSeconds)]]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults.csv"))



