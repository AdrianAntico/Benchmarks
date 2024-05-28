# Path to data storage
Path <- "C:/Users/Bizon/Documents/GitHub/rappwd/"

# Create results table
BenchmarkResults <- data.table::data.table(
  Framework = 'data.table',
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

data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Cast.csv"))
rm(BenchmarkResults)

library(data.table)
# setDTthreads(percent = 100)


# Melt 1M

# Melt Numeric Variable:

## 1M 2N 1D 0G
data <- fread(paste0(Path, "FakeBevData1M.csv"))
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Cast.csv"))
rts <- c(rep(1.1, 10))
temp <- data.table::melt(data = data, id.vars = c("Date","Customer","Brand","Category","Beverage Flavor"), measure.vars = c("Daily Liters","Daily Units","Daily Margin","Daily Revenue"))
temp <- temp[, lapply(.SD, sum, na.rm = TRUE), .SDcols = c("value"), by = c("Date","Customer","Brand","Category","Beverage Flavor","variable")]
temp <- temp[Customer %chin% paste0("Location ", 1:43)]
for(i in 1:10) {
  start <- Sys.time()
  data.table::dcast.data.table(data = temp, formula = Date ~ variable, value.var = "value", fun.aggregate = sum, fill = 0)
  end <- Sys.time()
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[1, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Cast.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 1M 2N 1D 1G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Cast.csv"))
rts <- c(rep(1.1, 10))
for(i in 1:10) {
  print(i)
  start <- Sys.time()
  data.table::dcast.data.table(data = temp, formula = Date + Customer ~ variable, value.var = "value", fun.aggregate = sum, fill = 0)
  end <- Sys.time()
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[2, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Cast.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 1M 2N 1D 2G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Cast.csv"))
rts <- c(rep(1.1, 10))
for(i in 1:10) {
  print(i)
  start <- Sys.time()
  data.table::dcast.data.table(data = temp, formula = Date + Customer + Brand ~ variable, value.var = "value", fun.aggregate = sum, fill = 0)
  end <- Sys.time()
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[3, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Cast.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 1M 2N 1D 3G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Cast.csv"))
rts <- c(rep(1.1, 10))
for(i in 1:10) {
  print(i)
  start <- Sys.time()
  data.table::dcast.data.table(data = temp, formula = Date + Customer + Brand + Category ~ variable, value.var = "value", fun.aggregate = sum, fill = 0)
  end <- Sys.time()
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[4, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Cast.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 1M 2N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Cast.csv"))
rts <- c(rep(1.1, 10))
for(i in 1:10) {
  print(i)
  start <- Sys.time()
  data.table::dcast.data.table(data = temp, formula = Date + Customer + Brand + Category + `Beverage Flavor` ~ variable, value.var = "value", fun.aggregate = sum, fill = 0)
  end <- Sys.time()
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[5, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Cast.csv"))
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
temp <- data.table::melt(data = data, id.vars = c("Date","Customer","Brand","Category","Beverage Flavor"), measure.vars = c("Daily Liters","Daily Units","Daily Margin","Daily Revenue"))
temp <- temp[, lapply(.SD, sum, na.rm = TRUE), .SDcols = c("value"), by = c("Date","Customer","Brand","Category","Beverage Flavor","variable")]
temp <- temp[Customer %chin% paste0("Location ", 1:482)]
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Cast.csv"))
rts <- c(rep(1.1, 10))
for(i in 1:10) {
  print(i)
  start <- Sys.time()
  data.table::dcast.data.table(data = temp, formula = Date ~ variable, value.var = "value", fun.aggregate = sum, fill = 0)
  end <- Sys.time()
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[6, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Cast.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 10M 2N 1D 1G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Cast.csv"))
rts <- c(rep(1.1, 10))
for(i in 1:10) {
  print(i)
  start <- Sys.time()
  data.table::dcast.data.table(data = temp, formula = Date + Customer ~ variable, value.var = "value", fun.aggregate = sum, fill = 0)
  end <- Sys.time()
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[7, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Cast.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 10M 2N 1D 2G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Cast.csv"))
rts <- c(rep(1.1, 10))
for(i in 1:10) {
  print(i)
  start <- Sys.time()
  data.table::dcast.data.table(data = temp, formula = Date + Customer + Brand ~ variable, value.var = "value", fun.aggregate = sum, fill = 0)
  end <- Sys.time()
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[8, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Cast.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 10M 2N 1D 3G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Cast.csv"))
rts <- c(rep(1.1, 10))
for(i in 1:10) {
  print(i)
  start <- Sys.time()
  data.table::dcast.data.table(data = temp, formula = Date + Customer + Brand + Category ~ variable, value.var = "value", fun.aggregate = sum, fill = 0)
  end <- Sys.time()
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[9, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Cast.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 10M 2N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Cast.csv"))
rts <- c(rep(1.1, 10))
for(i in 1:10) {
  print(i)
  start <- Sys.time()
  data.table::dcast.data.table(data = temp, formula = Date + Customer + Brand + Category + `Beverage Flavor` ~ variable, value.var = "value", fun.aggregate = sum, fill = 0)
  end <- Sys.time()
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[10, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Cast.csv"))
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
temp <- data.table::melt(data = data, id.vars = c("Date","Customer","Brand","Category","Beverage Flavor"), measure.vars = c("Daily Liters","Daily Units","Daily Margin","Daily Revenue"))
temp <- temp[, lapply(.SD, sum, na.rm = TRUE), .SDcols = c("value"), by = c("Date","Customer","Brand","Category","Beverage Flavor","variable")]
temp <- temp[Customer %chin% paste0("Location ", 1:4881)]
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Cast.csv"))
rts <- c(rep(1.1, 10))
for(i in 1:10) {
  print(i)
  start <- Sys.time()
  data.table::dcast.data.table(data = temp, formula = Date ~ variable, value.var = "value", fun.aggregate = sum, fill = 0)
  end <- Sys.time()
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[11, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Cast.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 100M 2N 1D 1G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Cast.csv"))
rts <- c(rep(1.1, 10))
for(i in 1:10) {
  print(i)
  start <- Sys.time()
  data.table::dcast.data.table(data = temp, formula = Date + Customer ~ variable, value.var = "value", fun.aggregate = sum, fill = 0)
  end <- Sys.time()
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[12, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Cast.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 100M 2N 1D 2G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Cast.csv"))
rts <- c(rep(1.1, 10))
for(i in 1:10) {
  print(i)
  start <- Sys.time()
  data.table::dcast.data.table(data = temp, formula = Date + Customer + Brand ~ variable, value.var = "value", fun.aggregate = sum, fill = 0)
  end <- Sys.time()
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[13, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Cast.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 100M 2N 1D 3G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Cast.csv"))
rts <- c(rep(1.1, 10))
for(i in 1:10) {
  print(i)
  start <- Sys.time()
  data.table::dcast.data.table(data = temp, formula = Date + Customer + Brand + Category ~ variable, value.var = "value", fun.aggregate = sum, fill = 0)
  end <- Sys.time()
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[14, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Cast.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 100M 2N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Cast.csv"))
rts <- c(rep(1.1, 10))
for(i in 1:10) {
  print(i)
  start <- Sys.time()
  data.table::dcast.data.table(data = temp, formula = Date + Customer + Brand + Category + `Beverage Flavor` ~ variable, value.var = "value", fun.aggregate = sum, fill = 0)
  end <- Sys.time()
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[15, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Cast.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()


BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Cast.csv"))
BenchmarkResults[16, TimeInSeconds := BenchmarkResults[1:15, sum(TimeInSeconds)]]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Cast.csv"))



