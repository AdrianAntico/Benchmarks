# Path to data storage
Path <- "C:/Users/Bizon/Documents/GitHub/rappwd/"

# Create results table
BenchmarkResults <- data.table::data.table(
  Framework = 'data.table',
  Method = 'union',
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

    "Total Runtime"),

  TimeInSeconds = c(rep(-0.1, 46))
)

data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Union.csv"))
rm(BenchmarkResults)

library(data.table)
# setDTthreads(percent = 100)


# Melt 1M

# Melt Numeric Variable:

## 1M 1N 1D 0G
data <- fread(paste0(Path, "FakeBevData1M.csv"))
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Union.csv"))
temp <- data[, .SD, .SDcols = c("Date","Daily Liters")]
rts <- c(rep(1.1, 3))
for(i in 1:3) {
  start <- Sys.time()
  rbindlist(list(temp, temp))
  end <- Sys.time()
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[1, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Union.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 1M 1N 1D 1G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Union.csv"))
temp <- data[, .SD, .SDcols = c("Date","Customer","Daily Liters")]
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

## 1M 1N 1D 2G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Union.csv"))
temp <- data[, .SD, .SDcols = c("Date","Customer","Brand","Daily Liters")]
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

## 1M 1N 1D 3G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Union.csv"))
temp <- data[, .SD, .SDcols = c("Date","Customer","Brand","Category","Daily Liters")]
rts <- c(rep(1.1, 3))
for(i in 1:3) {
  print(i)
  start <- Sys.time()
  rbindlist(list(temp, temp))
  end <- Sys.time()
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[4, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Union.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 1M 1N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Union.csv"))
temp <- data[, .SD, .SDcols = c("Date","Customer","Brand","Category","Beverage Flavor","Daily Liters")]
rts <- c(rep(1.1, 3))
for(i in 1:3) {
  print(i)
  start <- Sys.time()
  rbindlist(list(temp, temp))
  end <- Sys.time()
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[5, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Union.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()


## 1M 2N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Union.csv"))
temp <- data[, .SD, .SDcols = c("Date","Daily Liters","Daily Units")]
rts <- c(rep(1.1, 3))
for(i in 1:3) {
  print(i)
  start <- Sys.time()
  rbindlist(list(temp, temp))
  end <- Sys.time()
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[6, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Union.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()


## 1M 2N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Union.csv"))
temp <- data[, .SD, .SDcols = c("Date","Customer","Daily Liters","Daily Units")]
rts <- c(rep(1.1, 3))
for(i in 1:3) {
  print(i)
  start <- Sys.time()
  rbindlist(list(temp, temp))
  end <- Sys.time()
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[7, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Union.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()


## 1M 2N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Union.csv"))
temp <- data[, .SD, .SDcols = c("Date","Customer","Brand","Daily Liters","Daily Units")]
rts <- c(rep(1.1, 3))
for(i in 1:3) {
  print(i)
  start <- Sys.time()
  rbindlist(list(temp, temp))
  end <- Sys.time()
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[8, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Union.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()


## 1M 2N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Union.csv"))
temp <- data[, .SD, .SDcols = c("Date","Customer","Brand","Category","Daily Liters","Daily Units")]
rts <- c(rep(1.1, 3))
for(i in 1:3) {
  print(i)
  start <- Sys.time()
  rbindlist(list(temp, temp))
  end <- Sys.time()
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[9, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Union.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()


## 1M 2N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Union.csv"))
temp <- data[, .SD, .SDcols = c("Date","Customer","Brand","Category","Beverage Flavor","Daily Liters","Daily Units")]
rts <- c(rep(1.1, 3))
for(i in 1:3) {
  print(i)
  start <- Sys.time()
  rbindlist(list(temp, temp))
  end <- Sys.time()
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[10, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Union.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()


## 1M 3N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Union.csv"))
temp <- data[, .SD, .SDcols = c("Date","Daily Liters","Daily Units","Daily Margin")]
rts <- c(rep(1.1, 3))
for(i in 1:3) {
  print(i)
  start <- Sys.time()
  rbindlist(list(temp, temp))
  end <- Sys.time()
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[11, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Union.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()


## 1M 3N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Union.csv"))
temp <- data[, .SD, .SDcols = c("Date","Customer","Daily Liters","Daily Units","Daily Margin")]
rts <- c(rep(1.1, 3))
for(i in 1:3) {
  print(i)
  start <- Sys.time()
  rbindlist(list(temp, temp))
  end <- Sys.time()
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[12, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Union.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()


## 1M 3N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Union.csv"))
temp <- data[, .SD, .SDcols = c("Date","Customer","Brand","Daily Liters","Daily Units","Daily Margin")]
rts <- c(rep(1.1, 3))
for(i in 1:3) {
  print(i)
  start <- Sys.time()
  rbindlist(list(temp, temp))
  end <- Sys.time()
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[13, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Union.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()


## 1M 3N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Union.csv"))
temp <- data[, .SD, .SDcols = c("Date","Customer","Brand","Category","Daily Liters","Daily Units","Daily Margin")]
rts <- c(rep(1.1, 3))
for(i in 1:3) {
  print(i)
  start <- Sys.time()
  rbindlist(list(temp, temp))
  end <- Sys.time()
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[14, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Union.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()


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
BenchmarkResults[15, TimeInSeconds := median(rts)]
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

## 10M 2N 1D 0G
data <- fread(paste0(Path, "FakeBevData10M.csv"))
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Union.csv"))
temp <- data[, .SD, .SDcols = c("Date","Daily Liters")]
rts <- c(rep(1.1, 3))
for(i in 1:3) {
  start <- Sys.time()
  rbindlist(list(temp, temp))
  end <- Sys.time()
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[16, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Union.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 10M 1N 1D 1G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Union.csv"))
temp <- data[, .SD, .SDcols = c("Date","Customer","Daily Liters")]
rts <- c(rep(1.1, 3))
for(i in 1:3) {
  print(i)
  start <- Sys.time()
  rbindlist(list(temp, temp))
  end <- Sys.time()
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[17, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Union.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 10M 1N 1D 2G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Union.csv"))
temp <- data[, .SD, .SDcols = c("Date","Customer","Brand","Daily Liters")]
rts <- c(rep(1.1, 3))
for(i in 1:3) {
  print(i)
  start <- Sys.time()
  rbindlist(list(temp, temp))
  end <- Sys.time()
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[18, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Union.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 10M 1N 1D 3G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Union.csv"))
temp <- data[, .SD, .SDcols = c("Date","Customer","Brand","Category","Daily Liters")]
rts <- c(rep(1.1, 3))
for(i in 1:3) {
  print(i)
  start <- Sys.time()
  rbindlist(list(temp, temp))
  end <- Sys.time()
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[19, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Union.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 10M 1N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Union.csv"))
temp <- data[, .SD, .SDcols = c("Date","Customer","Brand","Category","Beverage Flavor","Daily Liters")]
rts <- c(rep(1.1, 3))
for(i in 1:3) {
  print(i)
  start <- Sys.time()
  rbindlist(list(temp, temp))
  end <- Sys.time()
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[20, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Union.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()


## 10M 2N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Union.csv"))
temp <- data[, .SD, .SDcols = c("Date","Daily Liters","Daily Units")]
rts <- c(rep(1.1, 3))
for(i in 1:3) {
  print(i)
  start <- Sys.time()
  rbindlist(list(temp, temp))
  end <- Sys.time()
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[21, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Union.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()


## 10M 2N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Union.csv"))
temp <- data[, .SD, .SDcols = c("Date","Customer","Daily Liters","Daily Units")]
rts <- c(rep(1.1, 3))
for(i in 1:3) {
  print(i)
  start <- Sys.time()
  rbindlist(list(temp, temp))
  end <- Sys.time()
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[22, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Union.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()


## 10M 2N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Union.csv"))
temp <- data[, .SD, .SDcols = c("Date","Customer","Brand","Daily Liters","Daily Units")]
rts <- c(rep(1.1, 3))
for(i in 1:3) {
  print(i)
  start <- Sys.time()
  rbindlist(list(temp, temp))
  end <- Sys.time()
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[23, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Union.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()


## 10M 2N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Union.csv"))
temp <- data[, .SD, .SDcols = c("Date","Customer","Brand","Category","Daily Liters","Daily Units")]
rts <- c(rep(1.1, 3))
for(i in 1:3) {
  print(i)
  start <- Sys.time()
  rbindlist(list(temp, temp))
  end <- Sys.time()
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[24, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Union.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()


## 10M 2N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Union.csv"))
temp <- data[, .SD, .SDcols = c("Date","Customer","Brand","Category","Beverage Flavor","Daily Liters","Daily Units")]
rts <- c(rep(1.1, 3))
for(i in 1:3) {
  print(i)
  start <- Sys.time()
  rbindlist(list(temp, temp))
  end <- Sys.time()
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[25, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Union.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()


## 10M 3N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Union.csv"))
temp <- data[, .SD, .SDcols = c("Date","Daily Liters","Daily Units","Daily Margin")]
rts <- c(rep(1.1, 3))
for(i in 1:3) {
  print(i)
  start <- Sys.time()
  rbindlist(list(temp, temp))
  end <- Sys.time()
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[26, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Union.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()


## 10M 3N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Union.csv"))
temp <- data[, .SD, .SDcols = c("Date","Customer","Daily Liters","Daily Units","Daily Margin")]
rts <- c(rep(1.1, 3))
for(i in 1:3) {
  print(i)
  start <- Sys.time()
  rbindlist(list(temp, temp))
  end <- Sys.time()
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[27, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Union.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()


## 10M 3N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Union.csv"))
temp <- data[, .SD, .SDcols = c("Date","Customer","Brand","Daily Liters","Daily Units","Daily Margin")]
rts <- c(rep(1.1, 3))
for(i in 1:3) {
  print(i)
  start <- Sys.time()
  rbindlist(list(temp, temp))
  end <- Sys.time()
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[28, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Union.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()


## 10M 3N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Union.csv"))
temp <- data[, .SD, .SDcols = c("Date","Customer","Brand","Category","Daily Liters","Daily Units","Daily Margin")]
rts <- c(rep(1.1, 3))
for(i in 1:3) {
  print(i)
  start <- Sys.time()
  rbindlist(list(temp, temp))
  end <- Sys.time()
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[29, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Union.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()


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
BenchmarkResults[30, TimeInSeconds := median(rts)]
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

## 100M 2N 1D 0G
data <- fread(paste0(Path, "FakeBevData100M.csv"))
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Union.csv"))
temp <- data[, .SD, .SDcols = c("Date","Daily Liters")]
rts <- c(rep(1.1, 3))
for(i in 1:3) {
  start <- Sys.time()
  rbindlist(list(temp, temp))
  end <- Sys.time()
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[31, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Union.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 100M 1N 1D 1G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Union.csv"))
temp <- data[, .SD, .SDcols = c("Date","Customer","Daily Liters")]
rts <- c(rep(1.1, 3))
for(i in 1:3) {
  print(i)
  start <- Sys.time()
  rbindlist(list(temp, temp))
  end <- Sys.time()
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[32, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Union.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 100M 1N 1D 2G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Union.csv"))
temp <- data[, .SD, .SDcols = c("Date","Customer","Brand","Daily Liters")]
rts <- c(rep(1.1, 3))
for(i in 1:3) {
  print(i)
  start <- Sys.time()
  rbindlist(list(temp, temp))
  end <- Sys.time()
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[33, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Union.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 100M 1N 1D 3G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Union.csv"))
temp <- data[, .SD, .SDcols = c("Date","Customer","Brand","Category","Daily Liters")]
rts <- c(rep(1.1, 3))
for(i in 1:3) {
  print(i)
  start <- Sys.time()
  rbindlist(list(temp, temp))
  end <- Sys.time()
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[34, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Union.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 100M 1N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Union.csv"))
temp <- data[, .SD, .SDcols = c("Date","Customer","Brand","Category","Beverage Flavor","Daily Liters")]
rts <- c(rep(1.1, 3))
for(i in 1:3) {
  print(i)
  start <- Sys.time()
  rbindlist(list(temp, temp))
  end <- Sys.time()
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[35, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Union.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()


## 100M 2N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Union.csv"))
temp <- data[, .SD, .SDcols = c("Date","Daily Liters","Daily Units")]
rts <- c(rep(1.1, 3))
for(i in 1:3) {
  print(i)
  start <- Sys.time()
  rbindlist(list(temp, temp))
  end <- Sys.time()
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[36, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Union.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()


## 100M 2N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Union.csv"))
temp <- data[, .SD, .SDcols = c("Date","Customer","Daily Liters","Daily Units")]
rts <- c(rep(1.1, 3))
for(i in 1:3) {
  print(i)
  start <- Sys.time()
  rbindlist(list(temp, temp))
  end <- Sys.time()
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[37, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Union.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()


## 100M 2N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Union.csv"))
temp <- data[, .SD, .SDcols = c("Date","Customer","Brand","Daily Liters","Daily Units")]
rts <- c(rep(1.1, 3))
for(i in 1:3) {
  print(i)
  start <- Sys.time()
  rbindlist(list(temp, temp))
  end <- Sys.time()
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[38, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Union.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()


## 100M 2N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Union.csv"))
temp <- data[, .SD, .SDcols = c("Date","Customer","Brand","Category","Daily Liters","Daily Units")]
rts <- c(rep(1.1, 3))
for(i in 1:3) {
  print(i)
  start <- Sys.time()
  rbindlist(list(temp, temp))
  end <- Sys.time()
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[39, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Union.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()


## 100M 2N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Union.csv"))
temp <- data[, .SD, .SDcols = c("Date","Customer","Brand","Category","Beverage Flavor","Daily Liters","Daily Units")]
rts <- c(rep(1.1, 3))
for(i in 1:3) {
  print(i)
  start <- Sys.time()
  rbindlist(list(temp, temp))
  end <- Sys.time()
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[40, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Union.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()


## 100M 3N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Union.csv"))
temp <- data[, .SD, .SDcols = c("Date","Daily Liters","Daily Units","Daily Margin")]
rts <- c(rep(1.1, 3))
for(i in 1:3) {
  print(i)
  start <- Sys.time()
  rbindlist(list(temp, temp))
  end <- Sys.time()
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[41, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Union.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()


## 100M 3N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Union.csv"))
temp <- data[, .SD, .SDcols = c("Date","Customer","Daily Liters","Daily Units","Daily Margin")]
rts <- c(rep(1.1, 3))
for(i in 1:3) {
  print(i)
  start <- Sys.time()
  rbindlist(list(temp, temp))
  end <- Sys.time()
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[42, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Union.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()


## 100M 3N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Union.csv"))
temp <- data[, .SD, .SDcols = c("Date","Customer","Brand","Daily Liters","Daily Units","Daily Margin")]
rts <- c(rep(1.1, 3))
for(i in 1:3) {
  print(i)
  start <- Sys.time()
  rbindlist(list(temp, temp))
  end <- Sys.time()
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[43, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Union.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()


## 100M 3N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Union.csv"))
temp <- data[, .SD, .SDcols = c("Date","Customer","Brand","Category","Daily Liters","Daily Units","Daily Margin")]
rts <- c(rep(1.1, 3))
for(i in 1:3) {
  print(i)
  start <- Sys.time()
  rbindlist(list(temp, temp))
  end <- Sys.time()
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[44, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Union.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()


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
BenchmarkResults[45, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Union.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()



BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Union.csv"))
BenchmarkResults[16, TimeInSeconds := BenchmarkResults[1:15, sum(TimeInSeconds)]]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Union.csv"))



