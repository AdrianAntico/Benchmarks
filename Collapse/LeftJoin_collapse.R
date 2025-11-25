# Path to data storage
Path <- "C:/Users/Bizon/Documents/GitHub/rappwd/"

# Create results table
BenchmarkResults <- data.table::data.table(
  Framework = 'collapse',
  Method = 'left join',
  Experiment = c(
    "1M 3N 1D 4G",
    "10M 3N 1D 4G",
    "100M 3N 1D 4G"
  ),

  TimeInSeconds = c(rep(-0.1, 3))
)

data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsCollapse_LeftJoin.csv"))
rm(BenchmarkResults)

library(data.table)
library(collapse)
# setDTthreads(percent = 100)


# Left Join 1M

# Left Join Numeric Variable:
data <- fread(paste0(Path, "FakeBevData1M.csv"))
set_collapse(nthreads = data.table::getDTthreads(), mask = "all", na.rm = anyNA(num_vars(data)), stable.algo = FALSE, sort = FALSE)


## 1M 3N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsCollapse_LeftJoin.csv"))
temp1 <- data[, .SD, .SDcols = c("Date","Customer","Brand","Category","Beverage Flavor","Daily Liters")]
temp2 <- data[, .SD, .SDcols = c("Date","Customer","Brand","Category","Beverage Flavor","Daily Units", "Daily Margin", "Daily Revenue")]
temp2 <- temp2[Brand != "Zingers"]
rts <- c(rep(1.1, 3))
for(i in 1:3) {
  print(i)
  start <- Sys.time()
  x = join(temp1, temp2, on=c("Date","Customer","Brand","Category","Beverage Flavor"), how="left", verbose=0)
  end <- Sys.time()
  rm(x)
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[1, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsCollapse_LeftJoin.csv"))
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

## 1M 3N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsCollapse_LeftJoin.csv"))
temp1 <- data[, .SD, .SDcols = c("Date","Customer","Brand","Category","Beverage Flavor","Daily Liters")]
temp2 <- data[, .SD, .SDcols = c("Date","Customer","Brand","Category","Beverage Flavor","Daily Units", "Daily Margin", "Daily Revenue")]
temp2 <- temp2[Brand != "Zingers"]
rts <- c(rep(1.1, 3))
for(i in 1:3) {
  print(i)
  start <- Sys.time()
  x = join(temp1, temp2, on=c("Date","Customer","Brand","Category","Beverage Flavor"), how="left", verbose=0)
  end <- Sys.time()
  rm(x)
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[2, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsCollapse_LeftJoin.csv"))
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


## 1M 3N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsCollapse_LeftJoin.csv"))
temp1 <- data[, .SD, .SDcols = c("Date","Customer","Brand","Category","Beverage Flavor","Daily Liters")]
temp2 <- data[, .SD, .SDcols = c("Date","Customer","Brand","Category","Beverage Flavor","Daily Units", "Daily Margin", "Daily Revenue")]
temp2 <- temp2[Brand != "Zingers"]
rts <- c(rep(1.1, 3))
for(i in 1:3) {
  print(i)
  start <- Sys.time()
  x = join(temp1, temp2, on=c("Date","Customer","Brand","Category","Beverage Flavor"), how="left", verbose=0)
  end <- Sys.time()
  rm(x)
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[3, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsCollapse_LeftJoin.csv"))
rm(list = c("BenchmarkResults","end","start","temp1","temp2","rts"))
gc()
