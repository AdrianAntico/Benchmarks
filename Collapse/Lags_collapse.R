# Path to data storage
Path <- "C:/Users/Bizon/Documents/GitHub/rappwd/"

# Create results table
BenchmarkResults <- data.table::data.table(
  Framework = 'collapse',
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

data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsCollapse_Lags.csv"))
rm(BenchmarkResults)

library(data.table)
library(collapse)

# Aggregation 1M

# Sum 1 Numeric Variable:

## 1M 1N 1D 0G
data <- fread(paste0(Path, "FakeBevData1M.csv"))
setorderv(data, cols = c("Customer","Brand","Category","Date","Beverage Flavor"), c(1,1,1,1,1))
set_collapse(nthreads = data.table::getDTthreads(), mask = "all", na.rm = anyNA(num_vars(data)), stable.algo = FALSE, sort = FALSE)
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsCollapse_Lags.csv"))
setorderv(data, "Date", 1)
start <- Sys.time()
cbind(data, data |> fselect(`Daily Liters`) |> flag(1:5))
end <- Sys.time()
BenchmarkResults[1, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsCollapse_Lags.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 1M 1N 1D 1G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsCollapse_Lags.csv"))
start <- Sys.time()
cbind(data, L(data, 1:5, `Daily Liters` ~ Customer))
end <- Sys.time()
BenchmarkResults[2, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsCollapse_Lags.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 1M 1N 1D 2G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsCollapse_Lags.csv"))
start <- Sys.time()
cbind(data, L(data, 1:5, `Daily Liters` ~ Customer + Brand))
end <- Sys.time()
BenchmarkResults[3, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsCollapse_Lags.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 1M 1N 1D 3G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsCollapse_Lags.csv"))
start <- Sys.time()
cbind(data, L(data, 1:5, `Daily Liters` ~ Customer + Brand + Category))
end <- Sys.time()
BenchmarkResults[4, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsCollapse_Lags.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 1M 1N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsCollapse_Lags.csv"))
start <- Sys.time()
cbind(data, L(data, 1:5, `Daily Liters` ~ Customer + Brand + Category + `Beverage Flavor`))
end <- Sys.time()
BenchmarkResults[5, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsCollapse_Lags.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 1M 2N 1D 0G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsCollapse_Lags.csv"))
start <- Sys.time()
cbind(data, data |> fselect(`Daily Liters`,`Daily Units`) |> flag(1:5))
end <- Sys.time()
BenchmarkResults[6, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsCollapse_Lags.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 1M 2N 1D 1G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsCollapse_Lags.csv"))
start <- Sys.time()
cbind(data, L(data, 1:5, `Daily Liters` + `Daily Units` ~ Customer))
end <- Sys.time()
BenchmarkResults[7, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsCollapse_Lags.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 1M 2N 1D 2G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsCollapse_Lags.csv"))
start <- Sys.time()
cbind(data, L(data, 1:5, `Daily Liters` + `Daily Units` ~ Customer + Brand))
end <- Sys.time()
BenchmarkResults[8, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsCollapse_Lags.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 1M 2N 1D 3G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsCollapse_Lags.csv"))
start <- Sys.time()
cbind(data, L(data, 1:5, `Daily Liters` + `Daily Units` ~ Customer + Brand + Category))
end <- Sys.time()
BenchmarkResults[9, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsCollapse_Lags.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 1M 2N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsCollapse_Lags.csv"))
start <- Sys.time()
cbind(data, L(data, 1:5, `Daily Liters` + `Daily Units` ~ Customer + Brand + Category + `Beverage Flavor`))
end <- Sys.time()
BenchmarkResults[10, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsCollapse_Lags.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 1M 3N 1D 0G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsCollapse_Lags.csv"))
start <- Sys.time()
cbind(data, data |> fselect(`Daily Liters`,`Daily Units`,`Daily Margin`) |> flag(1:5))
end <- Sys.time()
BenchmarkResults[11, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsCollapse_Lags.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 1M 3N 1D 1G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsCollapse_Lags.csv"))
start <- Sys.time()
cbind(data, L(data, 1:5, `Daily Liters` + `Daily Units` + `Daily Margin` ~ Customer))
end <- Sys.time()
BenchmarkResults[12, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsCollapse_Lags.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 1M 3N 1D 2G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsCollapse_Lags.csv"))
start <- Sys.time()
cbind(data, L(data, 1:5, `Daily Liters` + `Daily Units` + `Daily Margin` ~ Customer + Brand))
end <- Sys.time()
BenchmarkResults[13, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsCollapse_Lags.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 1M 3N 1D 3G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsCollapse_Lags.csv"))
start <- Sys.time()
cbind(data, L(data, 1:5, `Daily Liters` + `Daily Units` + `Daily Margin` ~ Customer + Brand + Category))
end <- Sys.time()
BenchmarkResults[14, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsCollapse_Lags.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 1M 3N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsCollapse_Lags.csv"))
start <- Sys.time()
cbind(data, L(data, 1:5, `Daily Liters` + `Daily Units` + `Daily Margin` ~ Customer + Brand + Category + `Beverage Flavor`))
end <- Sys.time()
BenchmarkResults[15, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
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

## 10M 1N 1D 0G
data <- fread(paste0(Path, "FakeBevData10M.csv"))
setorderv(data, cols = c("Customer","Brand","Category","Date","Beverage Flavor"), c(1,1,1,1,1))
set_collapse(nthreads = data.table::getDTthreads(), mask = "all", na.rm = anyNA(num_vars(data)), stable.algo = FALSE, sort = FALSE)
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsCollapse_Lags.csv"))
start <- Sys.time()
cbind(data, data |> fselect(`Daily Liters`) |> flag(1:5))
end <- Sys.time()
BenchmarkResults[16, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsCollapse_Lags.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 10M 1N 1D 1G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsCollapse_Lags.csv"))
start <- Sys.time()
cbind(data, L(data, 1:5, `Daily Liters` ~ Customer))
end <- Sys.time()
BenchmarkResults[17, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsCollapse_Lags.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 10M 1N 1D 2G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsCollapse_Lags.csv"))
start <- Sys.time()
cbind(data, L(data, 1:5, `Daily Liters` ~ Customer + Brand))
end <- Sys.time()
BenchmarkResults[18, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsCollapse_Lags.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 10M 1N 1D 3G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsCollapse_Lags.csv"))
start <- Sys.time()
cbind(data, L(data, 1:5, `Daily Liters` ~ Customer + Brand + Category))
end <- Sys.time()
BenchmarkResults[19, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsCollapse_Lags.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 10M 1N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsCollapse_Lags.csv"))
start <- Sys.time()
cbind(data, L(data, 1:5, `Daily Liters` ~ Customer + Brand + Category + `Beverage Flavor`))
end <- Sys.time()
BenchmarkResults[20, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsCollapse_Lags.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 10M 2N 1D 0G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsCollapse_Lags.csv"))
start <- Sys.time()
cbind(data, data |> fselect(`Daily Liters`,`Daily Units`) |> flag(1:5))
end <- Sys.time()
BenchmarkResults[21, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsCollapse_Lags.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 10M 2N 1D 1G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsCollapse_Lags.csv"))
start <- Sys.time()
cbind(data, L(data, 1:5, `Daily Liters` + `Daily Units` ~ Customer))
end <- Sys.time()
BenchmarkResults[22, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsCollapse_Lags.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 10M 2N 1D 2G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsCollapse_Lags.csv"))
start <- Sys.time()
cbind(data, L(data, 1:5, `Daily Liters` + `Daily Units` ~ Customer + Brand))
end <- Sys.time()
BenchmarkResults[23, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsCollapse_Lags.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 10M 2N 1D 3G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsCollapse_Lags.csv"))
start <- Sys.time()
cbind(data, L(data, 1:5, `Daily Liters` + `Daily Units` ~ Customer + Brand + Category))
end <- Sys.time()
BenchmarkResults[24, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsCollapse_Lags.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 10M 2N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsCollapse_Lags.csv"))
start <- Sys.time()
cbind(data, L(data, 1:5, `Daily Liters` + `Daily Units` ~ Customer + Brand + Category + `Beverage Flavor`))
end <- Sys.time()
BenchmarkResults[25, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsCollapse_Lags.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 10M 3N 1D 0G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsCollapse_Lags.csv"))
start <- Sys.time()
cbind(data, data |> fselect(`Daily Liters`,`Daily Units`,`Daily Margin`) |> flag(1:5))
end <- Sys.time()
BenchmarkResults[26, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsCollapse_Lags.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 10M 3N 1D 1G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsCollapse_Lags.csv"))
start <- Sys.time()
cbind(data, L(data, 1:5, `Daily Liters` + `Daily Units` + `Daily Margin` ~ Customer))
end <- Sys.time()
BenchmarkResults[27, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsCollapse_Lags.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 10M 3N 1D 2G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsCollapse_Lags.csv"))
start <- Sys.time()
cbind(data, L(data, 1:5, `Daily Liters` + `Daily Units` + `Daily Margin` ~ Customer + Brand))
end <- Sys.time()
BenchmarkResults[28, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsCollapse_Lags.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 10M 3N 1D 3G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsCollapse_Lags.csv"))
start <- Sys.time()
cbind(data, L(data, 1:5, `Daily Liters` + `Daily Units` + `Daily Margin` ~ Customer + Brand + Category))
end <- Sys.time()
BenchmarkResults[29, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsCollapse_Lags.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 10M 3N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsCollapse_Lags.csv"))
start <- Sys.time()
cbind(data, L(data, 1:5, `Daily Liters` + `Daily Units` + `Daily Margin` ~ Customer + Brand + Category + `Beverage Flavor`))
cbind(data, L(data, 1:5, `Daily Liters` + `Daily Units` + `Daily Margin` ~ Customer + Brand + Category + `Beverage Flavor`))
end <- Sys.time()
BenchmarkResults[30, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
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

## 100M 1N 1D 0G
data <- fread(paste0(Path, "FakeBevData100M.csv"))
setorderv(data, cols = c("Customer","Brand","Category","Date","Beverage Flavor"), c(1,1,1,1,1))
set_collapse(nthreads = data.table::getDTthreads(), mask = "all", na.rm = anyNA(num_vars(data)), stable.algo = FALSE, sort = FALSE)
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsCollapse_Lags.csv"))
start <- Sys.time()
cbind(data, data |> fselect(`Daily Liters`) |> flag(1:5))
end <- Sys.time()
BenchmarkResults[31, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsCollapse_Lags.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 100M 1N 1D 1G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsCollapse_Lags.csv"))
start <- Sys.time()
cbind(data, L(data, 1:5, `Daily Liters` ~ Customer))
end <- Sys.time()
BenchmarkResults[32, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsCollapse_Lags.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 100M 1N 1D 2G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsCollapse_Lags.csv"))
start <- Sys.time()
cbind(data, L(data, 1:5, `Daily Liters` ~ Customer + Brand))
end <- Sys.time()
BenchmarkResults[33, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsCollapse_Lags.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 100M 1N 1D 3G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsCollapse_Lags.csv"))
start <- Sys.time()
cbind(data, L(data, 1:5, `Daily Liters` ~ Customer + Brand + Category))
end <- Sys.time()
BenchmarkResults[34, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsCollapse_Lags.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 100M 1N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsCollapse_Lags.csv"))
start <- Sys.time()
cbind(data, L(data, 1:5, `Daily Liters` ~ Customer + Brand + Category + `Beverage Flavor`))
end <- Sys.time()
BenchmarkResults[35, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsCollapse_Lags.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 100M 2N 1D 0G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsCollapse_Lags.csv"))
start <- Sys.time()
cbind(data, data |> fselect(`Daily Liters`,`Daily Units`) |> flag(1:5))
end <- Sys.time()
BenchmarkResults[36, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsCollapse_Lags.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 100M 2N 1D 1G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsCollapse_Lags.csv"))
start <- Sys.time()
cbind(data, L(data, 1:5, `Daily Liters` + `Daily Units` ~ Customer))
end <- Sys.time()
BenchmarkResults[37, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsCollapse_Lags.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 100M 2N 1D 2G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsCollapse_Lags.csv"))
start <- Sys.time()
cbind(data, L(data, 1:5, `Daily Liters` + `Daily Units` ~ Customer + Brand))
end <- Sys.time()
BenchmarkResults[38, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsCollapse_Lags.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 100M 2N 1D 3G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsCollapse_Lags.csv"))
start <- Sys.time()
cbind(data, L(data, 1:5, `Daily Liters` + `Daily Units` ~ Customer + Brand + Category))
end <- Sys.time()
BenchmarkResults[39, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsCollapse_Lags.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 100M 2N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsCollapse_Lags.csv"))
start <- Sys.time()
cbind(data, L(data, 1:5, `Daily Liters` + `Daily Units` ~ Customer + Brand + Category + `Beverage Flavor`))
end <- Sys.time()
BenchmarkResults[40, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsCollapse_Lags.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 100M 3N 1D 0G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsCollapse_Lags.csv"))
start <- Sys.time()
cbind(data, data |> fselect(`Daily Liters`,`Daily Units`,`Daily Margin`) |> flag(1:5))
end <- Sys.time()
BenchmarkResults[41, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsCollapse_Lags.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 100M 3N 1D 1G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsCollapse_Lags.csv"))
start <- Sys.time()
cbind(data, L(data, 1:5, `Daily Liters` + `Daily Units` + `Daily Margin` ~ Customer))
end <- Sys.time()
BenchmarkResults[42, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsCollapse_Lags.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 100M 3N 1D 2G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsCollapse_Lags.csv"))
start <- Sys.time()
cbind(data, L(data, 1:5, `Daily Liters` + `Daily Units` + `Daily Margin` ~ Customer + Brand))
end <- Sys.time()
BenchmarkResults[43, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsCollapse_Lags.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 100M 3N 1D 3G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsCollapse_Lags.csv"))
start <- Sys.time()
cbind(data, L(data, 1:5, `Daily Liters` + `Daily Units` + `Daily Margin` ~ Customer + Brand + Category))
end <- Sys.time()
BenchmarkResults[44, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsCollapse_Lags.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 100M 3N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsCollapse_Lags.csv"))
start <- Sys.time()
cbind(data, L(data, 1:5, `Daily Liters` + `Daily Units` + `Daily Margin` ~ Customer + Brand + Category + `Beverage Flavor`))
end <- Sys.time()
BenchmarkResults[45, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsCollapse_Lags.csv"))
rm(list = c("BenchmarkResults","data","end","start"))
gc()


BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsCollapse_Lags.csv"))
BenchmarkResults[46, TimeInSeconds := BenchmarkResults[1:45, sum(TimeInSeconds)]]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsCollapse_Lags.csv"))



