# Path to data storage
Path <- "C:/Users/Bizon/Documents/GitHub/rappwd/"

# Create results table
BenchmarkResults <- data.table::data.table(
  Framework = 'collapse',
  Method = 'cast',
  Experiment = c(
    "1M 4N 1D 0G",
    "1M 4N 1D 1G",
    "1M 4N 1D 2G",
    "1M 4N 1D 3G",
    "1M 4N 1D 4G",

    "10M 4N 1D 0G",
    "10M 4N 1D 1G",
    "10M 4N 1D 2G",
    "10M 4N 1D 3G",
    "10M 4N 1D 4G",

    "100M 4N 1D 0G",
    "100M 4N 1D 1G",
    "100M 4N 1D 2G",
    "100M 4N 1D 3G",
    "100M 4N 1D 4G",

    "Total Runtime"),

  TimeInSeconds = c(rep(-0.1, 16))
)

data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsCollapse_Cast.csv"))
rm(BenchmarkResults)

library(data.table)
library(collapse)
# setDTthreads(percent = 100)


# Cast 1M

# Cast Numeric Variable:

## 1M 2N 1D 0G
data <- fread(paste0(Path, "FakeBevData1M.csv"))
set_collapse(nthreads = data.table::getDTthreads(), mask = "all", na.rm = anyNA(num_vars(data)), stable.algo = FALSE, sort = FALSE)
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsCollapse_Cast.csv"))
rts <- c(rep(1.1, 3))
temp <- data.table::melt(data = data, id.vars = c("Date","Customer","Brand","Category","Beverage Flavor"), measure.vars = c("Daily Liters","Daily Units","Daily Margin","Daily Revenue"))
temp <- temp[, lapply(.SD, sum, na.rm = TRUE), .SDcols = c("value"), by = c("Date","Customer","Brand","Category","Beverage Flavor","variable")]
temp <- temp[Customer %chin% paste0("Location ", 1:43)]
for(i in 1:3) {# i = 1
  print(i)
  start <- Sys.time()
  pivot(data = temp, ids = c("Date"), values = c("value"), how = "wider", FUN = "sum", sort = "ids")
  end <- Sys.time()
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[1, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsCollapse_Cast.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 1M 2N 1D 1G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsCollapse_Cast.csv"))
rts <- c(rep(1.1, 3))
temp <- pivot(data = data, ids = c("Date","Customer"), values = c("Daily Liters", "Daily Units"))
for(i in 1:3) {
  print(i)
  start <- Sys.time()
  pivot(data = temp, ids = c("Date","Customer"), values = c("value"), how = "wider", FUN = "sum", sort = "ids")
  end <- Sys.time()
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[2, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsCollapse_Cast.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 1M 2N 1D 2G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsCollapse_Cast.csv"))
rts <- c(rep(1.1, 3))
temp <- pivot(data = data, ids = c("Date","Customer","Brand"), values = c("Daily Liters", "Daily Units"))
for(i in 1:3) {
  print(i)
  start <- Sys.time()
  pivot(data = temp, ids = c("Date","Customer","Brand"), values = c("value"), how = "wider", FUN = "sum", sort = "ids")
  end <- Sys.time()
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[3, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsCollapse_Cast.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 1M 2N 1D 3G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsCollapse_Cast.csv"))
rts <- c(rep(1.1, 3))
temp <- pivot(data = data, ids = c("Date","Customer","Brand","Category"), values = c("Daily Liters", "Daily Units"))
for(i in 1:3) {
  print(i)
  start <- Sys.time()
  pivot(data = temp, ids = c("Date","Customer","Brand","Category"), values = c("value"), how = "wider", FUN = "sum", sort = "ids")
  end <- Sys.time()
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[4, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsCollapse_Cast.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 1M 2N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsCollapse_Cast.csv"))
rts <- c(rep(1.1, 3))
temp <- pivot(data = data, ids = c("Date","Customer","Brand","Category","Beverage Flavor"), values = c("Daily Liters", "Daily Units"))
for(i in 1:3) {
  print(i)
  start <- Sys.time()
  pivot(data = temp, ids = c("Date","Customer","Brand","Category","Beverage Flavor"), values = c("value"), how = "wider", FUN = "sum", sort = "ids")
  end <- Sys.time()
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[5, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsCollapse_Cast.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

###################################################################################################
###################################################################################################
###################################################################################################
###################################################################################################
###################################################################################################

# Cast 10M

# Cast Numeric Variables:

## 10M 2N 1D 0G
data <- fread(paste0(Path, "FakeBevData10M.csv"))
set_collapse(nthreads = data.table::getDTthreads(), mask = "all", na.rm = anyNA(num_vars(data)), stable.algo = FALSE, sort = FALSE)
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsCollapse_Cast.csv"))
rts <- c(rep(1.1, 3))
temp <- data.table::melt(data = data, id.vars = c("Date","Customer","Brand","Category","Beverage Flavor"), measure.vars = c("Daily Liters","Daily Units","Daily Margin","Daily Revenue"))
temp <- temp[, lapply(.SD, sum, na.rm = TRUE), .SDcols = c("value"), by = c("Date","Customer","Brand","Category","Beverage Flavor","variable")]
temp <- temp[Customer %chin% paste0("Location ", 1:482)]
for(i in 1:3) {
  print(i)
  start <- Sys.time()
  pivot(data = temp, ids = c("Date"), values = c("value"), how = "wider", FUN = "sum", sort = "ids")
  end <- Sys.time()
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[6, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsCollapse_Cast.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 10M 2N 1D 1G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsCollapse_Cast.csv"))
rts <- c(rep(1.1, 3))
temp <- pivot(data = data, ids = c("Date","Customer"), values = c("Daily Liters", "Daily Units"))
for(i in 1:3) {
  print(i)
  start <- Sys.time()
  pivot(data = temp, ids = c("Date","Customer"), values = c("value"), how = "wider", FUN = "sum", sort = "ids")
  end <- Sys.time()
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[7, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsCollapse_Cast.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 10M 2N 1D 2G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsCollapse_Cast.csv"))
rts <- c(rep(1.1, 3))
temp <- pivot(data = data, ids = c("Date","Customer","Brand"), values = c("Daily Liters", "Daily Units"))
for(i in 1:3) {
  print(i)
  start <- Sys.time()
  pivot(data = temp, ids = c("Date","Customer","Brand"), values = c("value"), how = "wider", FUN = "sum", sort = "ids")
  end <- Sys.time()
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[8, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsCollapse_Cast.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 10M 2N 1D 3G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsCollapse_Cast.csv"))
rts <- c(rep(1.1, 3))
temp <- pivot(data = data, ids = c("Date","Customer","Brand","Category"), values = c("Daily Liters", "Daily Units"))
for(i in 1:3) {
  print(i)
  start <- Sys.time()
  pivot(data = temp, ids = c("Date","Customer","Brand","Category"), values = c("value"), how = "wider", FUN = "sum", sort = "ids")
  end <- Sys.time()
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[9, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsCollapse_Cast.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 10M 2N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsCollapse_Cast.csv"))
rts <- c(rep(1.1, 3))
temp <- pivot(data = data, ids = c("Date","Customer","Brand","Category","Beverage Flavor"), values = c("Daily Liters", "Daily Units"))
for(i in 1:3) {
  print(i)
  start <- Sys.time()
  pivot(data = temp, ids = c("Date","Customer","Brand","Category","Beverage Flavor"), values = c("value"), how = "wider", FUN = "sum", sort = "ids")
  end <- Sys.time()
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[10, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsCollapse_Cast.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()


###################################################################################################
###################################################################################################
###################################################################################################
###################################################################################################
###################################################################################################

# Cast 100M

# Cast Numeric Variables:

## 100M 2N 1D 0G
data <- fread(paste0(Path, "FakeBevData100M.csv"))
set_collapse(nthreads = data.table::getDTthreads(), mask = "all", na.rm = anyNA(num_vars(data)), stable.algo = FALSE, sort = FALSE)
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsCollapse_Cast.csv"))
rts <- c(rep(1.1, 3))
temp <- data.table::melt(data = data, id.vars = c("Date","Customer","Brand","Category","Beverage Flavor"), measure.vars = c("Daily Liters","Daily Units","Daily Margin","Daily Revenue"))
temp <- temp[, lapply(.SD, sum, na.rm = TRUE), .SDcols = c("value"), by = c("Date","Customer","Brand","Category","Beverage Flavor","variable")]
temp <- temp[Customer %chin% paste0("Location ", 1:4881)]
for(i in 1:3) {
  print(i)
  start <- Sys.time()
  pivot(data = temp, ids = c("Date"), values = c("value"), how = "wider", FUN = "sum", sort = "ids")
  end <- Sys.time()
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[11, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsCollapse_Cast.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 100M 2N 1D 1G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsCollapse_Cast.csv"))
rts <- c(rep(1.1, 3))
temp <- pivot(data = data, ids = c("Date","Customer"), values = c("Daily Liters", "Daily Units"))
for(i in 1:3) {
  print(i)
  start <- Sys.time()
  pivot(data = temp, ids = c("Date","Customer"), values = c("value"), how = "wider", FUN = "sum", sort = "ids")
  end <- Sys.time()
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[12, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsCollapse_Cast.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 100M 2N 1D 2G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsCollapse_Cast.csv"))
rts <- c(rep(1.1, 3))
temp <- pivot(data = data, ids = c("Date","Customer","Brand"), values = c("Daily Liters", "Daily Units"))
for(i in 1:3) {
  print(i)
  start <- Sys.time()
  pivot(data = temp, ids = c("Date","Customer","Brand"), values = c("value"), how = "wider", FUN = "sum", sort = "ids")
  end <- Sys.time()
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[13, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsCollapse_Cast.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 100M 2N 1D 3G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsCollapse_Cast.csv"))
rts <- c(rep(1.1, 3))
temp <- pivot(data = data, ids = c("Date","Customer","Brand","Category"), values = c("Daily Liters", "Daily Units"))
for(i in 1:3) {
  print(i)
  start <- Sys.time()
  pivot(data = temp, ids = c("Date","Customer","Brand","Category"), values = c("value"), how = "wider", FUN = "sum", sort = "ids")
  end <- Sys.time()
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[14, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsCollapse_Cast.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 100M 2N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsCollapse_Cast.csv"))
rts <- c(rep(1.1, 3))
temp <- pivot(data = data, ids = c("Date","Customer","Brand","Category","Beverage Flavor"), values = c("Daily Liters", "Daily Units"))
for(i in 1:3) {
  print(i)
  start <- Sys.time()
  pivot(data = temp, ids = c("Date","Customer","Brand","Category","Beverage Flavor"), values = c("value"), how = "wider", FUN = "sum", sort = "ids")
  end <- Sys.time()
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[15, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsCollapse_Cast.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()


BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsCollapse_Cast.csv"))
BenchmarkResults[16, TimeInSeconds := BenchmarkResults[1:15, sum(TimeInSeconds)]]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsCollapse_Cast.csv"))



