# Path to data storage
Path <- "C:/Users/Bizon/Documents/GitHub/rappwd/"

# Create results table
BenchmarkResults <- data.table::data.table(
  Framework = 'data.table',
  Method = 'union',
  Experiment = c(
    "1M 3N 1D 4G",
    "10M 3N 1D 4G",
    "100M 3N 1D 4G"
  ),

  TimeInSeconds = c(rep(-0.1, 3))
)

data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Union.csv"))
rm(BenchmarkResults)

library(data.table)
# setDTthreads(percent = 100)


# Melt 1M

# Melt Numeric Variable:
data <- fread(paste0(Path, "FakeBevData1M.csv"))


## 1M 3N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Union.csv"))
temp <- data[, .SD, .SDcols = c("Date","Customer","Brand","Category","Beverage Flavor","Daily Liters","Daily Units","Daily Margin")]
rts <- c(rep(1.1, 3))
for(i in 1:3) {
  print(i)
  start <- Sys.time()
  rbindlist(list(temp, temp))
  end <- Sys.time()
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[1, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Union.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()



###################################################################################################
###################################################################################################
###################################################################################################
###################################################################################################
###################################################################################################

# Aggregation 10M

# Melt Numeric Variables:
data <- fread(paste0(Path, "FakeBevData10M.csv"))

## 10M 3N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Union.csv"))
temp <- data[, .SD, .SDcols = c("Date","Customer","Brand","Category","Beverage Flavor","Daily Liters","Daily Units","Daily Margin")]
rts <- c(rep(1.1, 3))
for(i in 1:3) {
  print(i)
  start <- Sys.time()
  rbindlist(list(temp, temp))
  end <- Sys.time()
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[2, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Union.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()


###################################################################################################
###################################################################################################
###################################################################################################
###################################################################################################
###################################################################################################

# Aggregation 100M

# Melt Numeric Variables:
data <- fread(paste0(Path, "FakeBevData100M.csv"))


## 100M 3N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Union.csv"))
temp <- data[, .SD, .SDcols = c("Date","Customer","Brand","Category","Beverage Flavor","Daily Liters","Daily Units","Daily Margin")]
rts <- c(rep(1.1, 3))
for(i in 1:3) {
  print(i)
  start <- Sys.time()
  rbindlist(list(temp, temp))
  end <- Sys.time()
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[3, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Union.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()
