# Path to data storage
Path <- "C:/Users/Bizon/Documents/GitHub/rappwd/"

# Create results table
BenchmarkResults <- data.table::data.table(
  Framework = 'collapse',
  Method = 'inner join',
  Experiment = c(
    "1M 1N 1D 4G",
    "1M 2N 1D 4G",
    "1M 3N 1D 4G",

    "10M 1N 1D 4G",
    "10M 2N 1D 4G",
    "10M 3N 1D 4G",

    "100M 1N 1D 4G",
    "100M 2N 1D 4G",
    "100M 3N 1D 4G",

    "Total Runtime"),

  TimeInSeconds = c(rep(-0.1, 3))
)

data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsCollapse_InnerJoin.csv"))
rm(BenchmarkResults)

library(data.table)
library(collapse)
# setDTthreads(percent = 100)


# Left Join 1M

# Left Join Numeric Variable:

## 1M 1N 1D 4G
data <- fread(paste0(Path, "FakeBevData1M.csv"))
set_collapse(nthreads = data.table::getDTthreads(), mask = "all", na.rm = anyNA(num_vars(data)), stable.algo = FALSE, sort = FALSE)
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsCollapse_InnerJoin.csv"))
temp1 <- data[, .SD, .SDcols = c("Date","Customer","Brand","Category","Beverage Flavor","Daily Liters")]
temp2 <- data[, .SD, .SDcols = c("Date","Customer","Brand","Category","Beverage Flavor","Daily Units")]
rts <- c(rep(1.1, 3))
for(i in 1:3) {
  print(i)
  start <- Sys.time()
  x = join(temp1, temp2, on=c("Date","Customer","Brand","Category","Beverage Flavor"), how="inner", verbose=0)
  end <- Sys.time()
  rm(x)
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[1, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsCollapse_InnerJoin.csv"))
rm(list = c("BenchmarkResults","end","start","temp1","temp2","rts"))
gc()

## 1M 2N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsCollapse_InnerJoin.csv"))
temp1 <- data[, .SD, .SDcols = c("Date","Customer","Brand","Category","Beverage Flavor","Daily Liters")]
temp2 <- data[, .SD, .SDcols = c("Date","Customer","Brand","Category","Beverage Flavor","Daily Units", "Daily Margin")]
rts <- c(rep(1.1, 3))
for(i in 1:3) {
  print(i)
  start <- Sys.time()
  x = join(temp1, temp2, on=c("Date","Customer","Brand","Category","Beverage Flavor"), how="inner", verbose=0)
  end <- Sys.time()
  rm(x)
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[2, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsCollapse_InnerJoin.csv"))
rm(list = c("BenchmarkResults","end","start","temp1","temp2","rts"))
gc()


## 1M 3N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsCollapse_InnerJoin.csv"))
temp1 <- data[, .SD, .SDcols = c("Date","Customer","Brand","Category","Beverage Flavor","Daily Liters")]
temp2 <- data[, .SD, .SDcols = c("Date","Customer","Brand","Category","Beverage Flavor","Daily Units", "Daily Margin", "Daily Revenue")]
rts <- c(rep(1.1, 3))
for(i in 1:3) {
  print(i)
  start <- Sys.time()
  x = join(temp1, temp2, on=c("Date","Customer","Brand","Category","Beverage Flavor"), how="inner", verbose=0)
  end <- Sys.time()
  rm(x)
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[3, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsCollapse_InnerJoin.csv"))
rm(list = c("BenchmarkResults","end","start","temp1","temp2","rts"))
gc()


###################################################################################################
###################################################################################################
###################################################################################################
###################################################################################################
###################################################################################################

# Aggregation 10M

# Left Join Numeric Variables:

## 10M 2N 1D 0G
data <- fread(paste0(Path, "FakeBevData10M.csv"))
set_collapse(nthreads = data.table::getDTthreads(), mask = "all", na.rm = anyNA(num_vars(data)), stable.algo = FALSE, sort = FALSE)
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsCollapse_InnerJoin.csv"))
temp1 <- data[, .SD, .SDcols = c("Date","Customer","Brand","Category","Beverage Flavor","Daily Liters")]
temp2 <- data[, .SD, .SDcols = c("Date","Customer","Brand","Category","Beverage Flavor","Daily Units")]
rts <- c(rep(1.1, 3))
for(i in 1:3) {
  print(i)
  start <- Sys.time()
  x = join(temp1, temp2, on=c("Date","Customer","Brand","Category","Beverage Flavor"), how="inner", verbose=0)
  end <- Sys.time()
  rm(x)
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[4, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsCollapse_InnerJoin.csv"))
rm(list = c("BenchmarkResults","end","start","temp1","temp2","rts"))
gc()

## 1M 2N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsCollapse_InnerJoin.csv"))
temp1 <- data[, .SD, .SDcols = c("Date","Customer","Brand","Category","Beverage Flavor","Daily Liters")]
temp2 <- data[, .SD, .SDcols = c("Date","Customer","Brand","Category","Beverage Flavor","Daily Units", "Daily Margin")]
rts <- c(rep(1.1, 3))
for(i in 1:3) {
  print(i)
  start <- Sys.time()
  x = join(temp1, temp2, on=c("Date","Customer","Brand","Category","Beverage Flavor"), how="inner", verbose=0)
  end <- Sys.time()
  rm(x)
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[5, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsCollapse_InnerJoin.csv"))
rm(list = c("BenchmarkResults","end","start","temp1","temp2","rts"))
gc()


## 1M 3N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsCollapse_InnerJoin.csv"))
temp1 <- data[, .SD, .SDcols = c("Date","Customer","Brand","Category","Beverage Flavor","Daily Liters")]
temp2 <- data[, .SD, .SDcols = c("Date","Customer","Brand","Category","Beverage Flavor","Daily Units", "Daily Margin", "Daily Revenue")]
rts <- c(rep(1.1, 3))
for(i in 1:3) {
  print(i)
  start <- Sys.time()
  x = join(temp1, temp2, on=c("Date","Customer","Brand","Category","Beverage Flavor"), how="inner", verbose=0)
  end <- Sys.time()
  rm(x)
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[6, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsCollapse_InnerJoin.csv"))
rm(list = c("BenchmarkResults","end","start","temp1","temp2","rts"))
gc()


###################################################################################################
###################################################################################################
###################################################################################################
###################################################################################################
###################################################################################################

# Aggregation 100M

# Left Join Numeric Variables:

## 100M 2N 1D 0G
data <- fread(paste0(Path, "FakeBevData100M.csv"))
set_collapse(nthreads = data.table::getDTthreads(), mask = "all", na.rm = anyNA(num_vars(data)), stable.algo = FALSE, sort = FALSE)
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsCollapse_InnerJoin.csv"))
temp1 <- data[, .SD, .SDcols = c("Date","Customer","Brand","Category","Beverage Flavor","Daily Liters")]
temp2 <- data[, .SD, .SDcols = c("Date","Customer","Brand","Category","Beverage Flavor","Daily Units")]
rts <- c(rep(1.1, 3))
for(i in 1:3) {
  print(i)
  start <- Sys.time()
  x = join(temp1, temp2, on=c("Date","Customer","Brand","Category","Beverage Flavor"), how="inner", verbose=0)
  end <- Sys.time()
  rm(x)
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[7, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsCollapse_InnerJoin.csv"))
rm(list = c("BenchmarkResults","end","start","temp1","temp2","rts"))
gc()

## 1M 2N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsCollapse_InnerJoin.csv"))
temp1 <- data[, .SD, .SDcols = c("Date","Customer","Brand","Category","Beverage Flavor","Daily Liters")]
temp2 <- data[, .SD, .SDcols = c("Date","Customer","Brand","Category","Beverage Flavor","Daily Units", "Daily Margin")]
rts <- c(rep(1.1, 3))
for(i in 1:3) {
  print(i)
  start <- Sys.time()
  x = join(temp1, temp2, on=c("Date","Customer","Brand","Category","Beverage Flavor"), how="inner", verbose=0)
  end <- Sys.time()
  rm(x)
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[8, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsCollapse_InnerJoin.csv"))
rm(list = c("BenchmarkResults","end","start","temp1","temp2","rts"))
gc()


## 1M 3N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsCollapse_InnerJoin.csv"))
temp1 <- data[, .SD, .SDcols = c("Date","Customer","Brand","Category","Beverage Flavor","Daily Liters")]
temp2 <- data[, .SD, .SDcols = c("Date","Customer","Brand","Category","Beverage Flavor","Daily Units", "Daily Margin", "Daily Revenue")]
rts <- c(rep(1.1, 3))
for(i in 1:3) {
  print(i)
  start <- Sys.time()
  x = join(temp1, temp2, on=c("Date","Customer","Brand","Category","Beverage Flavor"), how="inner", verbose=0)
  end <- Sys.time()
  rm(x)
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[9, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsCollapse_InnerJoin.csv"))
rm(list = c("BenchmarkResults","end","start","temp1","temp2","rts"))
gc()



BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsCollapse_InnerJoin.csv"))
BenchmarkResults[10, TimeInSeconds := BenchmarkResults[1:9, sum(TimeInSeconds)]]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsCollapse_InnerJoin.csv"))



