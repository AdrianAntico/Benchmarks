# Path to data storage
Path <- "C:/Users/Bizon/Documents/GitHub/rappwd/"

# Create results table
BenchmarkResults <- data.table::data.table(
  Framework = 'data.table',
  Method = 'melt',
  Experiment = c(
    "1M 4N 1D 4G",
    "10M 4N 1D 4G",
    "100M 4N 1D 4G"
  ),

  TimeInSeconds = c(rep(-0.1, 3))
)

data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Melt.csv"))
rm(BenchmarkResults)

library(data.table)
# setDTthreads(percent = 100)


# Melt 1M

# Melt Numeric Variable:


## 1M 4N 1D 4G
data <- fread(paste0(Path, "FakeBevData1M.csv"))
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Melt.csv"))
rts <- c(rep(1.1, 3))
for(i in 1:3) {
  start <- Sys.time()
  data.table::melt(data = data, id.vars = c("Date","Customer","Brand","Category","Beverage Flavor"), measure.vars = c("Daily Liters", "Daily Units", "Daily Margin","Daily Revenue"))
  end <- Sys.time()
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[1, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Melt.csv"))
rm(list = c("BenchmarkResults","data","end","start"))
gc()

###################################################################################################
###################################################################################################
###################################################################################################
###################################################################################################
###################################################################################################

# Aggregation 10M

# Melt Numeric Variables:


## 10M 4N 1D 4G
data <- fread(paste0(Path, "FakeBevData10M.csv"))
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Melt.csv"))
rts <- c(rep(1.1, 3))
for(i in 1:3) {
  start <- Sys.time()
  data.table::melt(data = data, id.vars = c("Date","Customer","Brand","Category","Beverage Flavor"), measure.vars = c("Daily Liters", "Daily Units", "Daily Margin","Daily Revenue"))
  end <- Sys.time()
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[2, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Melt.csv"))
rm(list = c("BenchmarkResults","data","end","start"))
gc()

###################################################################################################
###################################################################################################
###################################################################################################
###################################################################################################
###################################################################################################

# Aggregation 100M

# Melt Numeric Variables:



## 100M 4N 1D 4G
data <- fread(paste0(Path, "FakeBevData100M.csv"))
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Melt.csv"))
rts <- c(rep(1.1, 3))
for(i in 1:3) {
  start <- Sys.time()
  data.table::melt(data = data, id.vars = c("Date","Customer","Brand","Category","Beverage Flavor"), measure.vars = c("Daily Liters", "Daily Units", "Daily Margin","Daily Revenue"))
  end <- Sys.time()
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[3, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Melt.csv"))
rm(list = c("BenchmarkResults","data","end","start"))
gc()
