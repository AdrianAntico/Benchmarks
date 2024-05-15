# Path to data storage
Path <- "C:/Users/Bizon/Documents/GitHub/rappwd/"

# Create results table
BenchmarkResults <- data.table::data.table(
  Framework = 'data.table',
  Method = 'melt',
  Experiment = c(
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
    "1M 4N 1D 0G",
    "1M 4N 1D 1G",
    "1M 4N 1D 2G",
    "1M 4N 1D 3G",
    "1M 4N 1D 4G",

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
    "10M 4N 1D 0G",
    "10M 4N 1D 1G",
    "10M 4N 1D 2G",
    "10M 4N 1D 3G",
    "10M 4N 1D 4G",

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
    "100M 4N 1D 0G",
    "100M 4N 1D 1G",
    "100M 4N 1D 2G",
    "100M 4N 1D 3G",
    "100M 4N 1D 4G",

    "Total Runtime"),

  TimeInSeconds = c(rep(-0.1, 46))
)

data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Melt.csv"))
rm(BenchmarkResults)

library(data.table)
# setDTthreads(percent = 100)


# Melt 1M

# Melt Numeric Variable:

## 1M 2N 1D 0G
data <- fread(paste0(Path, "FakeBevData1M.csv"))
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Melt.csv"))
start <- Sys.time()
data.table::melt.data.table(data = data, id.vars = "Date", measure.vars = c("Daily Liters", "Daily Units"))
end <- Sys.time()
BenchmarkResults[1, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Melt.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 1M 2N 1D 1G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Melt.csv"))
start <- Sys.time()
data.table::melt.data.table(data = data, id.vars = c("Date","Customer"), measure.vars = c("Daily Liters", "Daily Units"))
end <- Sys.time()
BenchmarkResults[2, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Melt.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 1M 2N 1D 2G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Melt.csv"))
start <- Sys.time()
data.table::melt.data.table(data = data, id.vars = c("Date","Customer","Brand"), measure.vars = c("Daily Liters", "Daily Units"))
end <- Sys.time()
BenchmarkResults[3, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Melt.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 1M 2N 1D 3G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Melt.csv"))
start <- Sys.time()
data.table::melt.data.table(data = data, id.vars = c("Date","Customer","Brand","Category"), measure.vars = c("Daily Liters", "Daily Units"))
end <- Sys.time()
BenchmarkResults[4, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Melt.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 1M 2N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Melt.csv"))
start <- Sys.time()
data.table::melt.data.table(data = data, id.vars = c("Date","Customer","Brand","Category","Beverage Flavor"), measure.vars = c("Daily Liters", "Daily Units"))
end <- Sys.time()
BenchmarkResults[5, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Melt.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 1M 3N 1D 0G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Melt.csv"))
start <- Sys.time()
data.table::melt.data.table(data = data, id.vars = "Date", measure.vars = c("Daily Liters", "Daily Units", "Daily Margin"))
end <- Sys.time()
BenchmarkResults[6, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Melt.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 1M 3N 1D 1G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Melt.csv"))
start <- Sys.time()
data.table::melt.data.table(data = data, id.vars = c("Date","Customer"), measure.vars = c("Daily Liters", "Daily Units", "Daily Margin"))
end <- Sys.time()
BenchmarkResults[7, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Melt.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 1M 3N 1D 2G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Melt.csv"))
start <- Sys.time()
data.table::melt.data.table(data = data, id.vars = c("Date","Customer","Brand"), measure.vars = c("Daily Liters", "Daily Units", "Daily Margin"))
end <- Sys.time()
BenchmarkResults[8, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Melt.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 1M 3N 1D 3G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Melt.csv"))
start <- Sys.time()
data.table::melt.data.table(data = data, id.vars = c("Date","Customer","Brand","Category"), measure.vars = c("Daily Liters", "Daily Units", "Daily Margin"))
end <- Sys.time()
BenchmarkResults[9, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Melt.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 1M 3N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Melt.csv"))
start <- Sys.time()
data.table::melt.data.table(data = data, id.vars = c("Date","Customer","Brand","Category","Beverage Flavor"), measure.vars = c("Daily Liters", "Daily Units", "Daily Margin"))
end <- Sys.time()
BenchmarkResults[10, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Melt.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 1M 4N 1D 0G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Melt.csv"))
start <- Sys.time()
data.table::melt.data.table(data = data, id.vars = "Date", measure.vars = c("Daily Liters", "Daily Units", "Daily Margin","Daily Revenue"))
end <- Sys.time()
BenchmarkResults[11, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Melt.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 1M 4N 1D 1G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Melt.csv"))
start <- Sys.time()
data.table::melt.data.table(data = data, id.vars = c("Date","Customer"), measure.vars = c("Daily Liters", "Daily Units", "Daily Margin","Daily Revenue"))
end <- Sys.time()
BenchmarkResults[12, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Melt.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 1M 4N 1D 2G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Melt.csv"))
start <- Sys.time()
data.table::melt.data.table(data = data, id.vars = c("Date","Customer","Brand"), measure.vars = c("Daily Liters", "Daily Units", "Daily Margin","Daily Revenue"))
end <- Sys.time()
BenchmarkResults[13, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Melt.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 1M 4N 1D 3G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Melt.csv"))
start <- Sys.time()
data.table::melt.data.table(data = data, id.vars = c("Date","Customer","Brand","Category"), measure.vars = c("Daily Liters", "Daily Units", "Daily Margin","Daily Revenue"))
end <- Sys.time()
BenchmarkResults[14, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Melt.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 1M 4N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Melt.csv"))
start <- Sys.time()
data.table::melt.data.table(data = data, id.vars = c("Date","Customer","Brand","Category","Beverage Flavor"), measure.vars = c("Daily Liters", "Daily Units", "Daily Margin","Daily Revenue"))
end <- Sys.time()
BenchmarkResults[15, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
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

## 10M 2N 1D 0G
data <- fread(paste0(Path, "FakeBevData10M.csv"))
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Melt.csv"))
start <- Sys.time()
data.table::melt.data.table(data = data, id.vars = "Date", measure.vars = c("Daily Liters", "Daily Units"))
end <- Sys.time()
BenchmarkResults[16, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Melt.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 10M 2N 1D 1G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Melt.csv"))
start <- Sys.time()
data.table::melt.data.table(data = data, id.vars = c("Date","Customer"), measure.vars = c("Daily Liters", "Daily Units"))
end <- Sys.time()
BenchmarkResults[17, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Melt.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 10M 2N 1D 2G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Melt.csv"))
start <- Sys.time()
data.table::melt.data.table(data = data, id.vars = c("Date","Customer","Brand"), measure.vars = c("Daily Liters", "Daily Units"))
end <- Sys.time()
BenchmarkResults[18, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Melt.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 10M 2N 1D 3G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Melt.csv"))
start <- Sys.time()
data.table::melt.data.table(data = data, id.vars = c("Date","Customer","Brand","Category"), measure.vars = c("Daily Liters", "Daily Units"))
end <- Sys.time()
BenchmarkResults[19, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Melt.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 10M 2N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Melt.csv"))
start <- Sys.time()
data.table::melt.data.table(data = data, id.vars = c("Date","Customer","Brand","Category","Beverage Flavor"), measure.vars = c("Daily Liters", "Daily Units"))
end <- Sys.time()
BenchmarkResults[20, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Melt.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 10M 3N 1D 0G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Melt.csv"))
start <- Sys.time()
data.table::melt.data.table(data = data, id.vars = "Date", measure.vars = c("Daily Liters", "Daily Units", "Daily Margin"))
end <- Sys.time()
BenchmarkResults[21, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Melt.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 10M 3N 1D 1G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Melt.csv"))
start <- Sys.time()
data.table::melt.data.table(data = data, id.vars = c("Date","Customer"), measure.vars = c("Daily Liters", "Daily Units", "Daily Margin"))
end <- Sys.time()
BenchmarkResults[22, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Melt.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 10M 3N 1D 2G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Melt.csv"))
start <- Sys.time()
data.table::melt.data.table(data = data, id.vars = c("Date","Customer","Brand"), measure.vars = c("Daily Liters", "Daily Units", "Daily Margin"))
end <- Sys.time()
BenchmarkResults[23, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Melt.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 10M 3N 1D 3G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Melt.csv"))
start <- Sys.time()
data.table::melt.data.table(data = data, id.vars = c("Date","Customer","Brand","Category"), measure.vars = c("Daily Liters", "Daily Units", "Daily Margin"))
end <- Sys.time()
BenchmarkResults[24, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Melt.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 10M 3N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Melt.csv"))
start <- Sys.time()
data.table::melt.data.table(data = data, id.vars = c("Date","Customer","Brand","Category","Beverage Flavor"), measure.vars = c("Daily Liters", "Daily Units", "Daily Margin"))
end <- Sys.time()
BenchmarkResults[25, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Melt.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 10M 4N 1D 0G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Melt.csv"))
start <- Sys.time()
data.table::melt.data.table(data = data, id.vars = "Date", measure.vars = c("Daily Liters", "Daily Units", "Daily Margin","Daily Revenue"))
end <- Sys.time()
BenchmarkResults[26, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Melt.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 10M 4N 1D 1G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Melt.csv"))
start <- Sys.time()
data.table::melt.data.table(data = data, id.vars = c("Date","Customer"), measure.vars = c("Daily Liters", "Daily Units", "Daily Margin","Daily Revenue"))
end <- Sys.time()
BenchmarkResults[27, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Melt.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 10M 4N 1D 2G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Melt.csv"))
start <- Sys.time()
data.table::melt.data.table(data = data, id.vars = c("Date","Customer","Brand"), measure.vars = c("Daily Liters", "Daily Units", "Daily Margin","Daily Revenue"))
end <- Sys.time()
BenchmarkResults[28, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Melt.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 10M 4N 1D 3G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Melt.csv"))
start <- Sys.time()
data.table::melt.data.table(data = data, id.vars = c("Date","Customer","Brand","Category"), measure.vars = c("Daily Liters", "Daily Units", "Daily Margin","Daily Revenue"))
end <- Sys.time()
BenchmarkResults[29, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Melt.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 10M 4N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Melt.csv"))
start <- Sys.time()
data.table::melt.data.table(data = data, id.vars = c("Date","Customer","Brand","Category","Beverage Flavor"), measure.vars = c("Daily Liters", "Daily Units", "Daily Margin","Daily Revenue"))
end <- Sys.time()
BenchmarkResults[30, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
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

## 100M 2N 1D 0G
data <- fread(paste0(Path, "FakeBevData100M.csv"))
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Melt.csv"))
start <- Sys.time()
data.table::melt.data.table(data = data, id.vars = "Date", measure.vars = c("Daily Liters", "Daily Units"))
end <- Sys.time()
BenchmarkResults[31, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Melt.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 100M 2N 1D 1G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Melt.csv"))
start <- Sys.time()
data.table::melt.data.table(data = data, id.vars = c("Date","Customer"), measure.vars = c("Daily Liters", "Daily Units"))
end <- Sys.time()
BenchmarkResults[32, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Melt.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 100M 2N 1D 2G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Melt.csv"))
start <- Sys.time()
data.table::melt.data.table(data = data, id.vars = c("Date","Customer","Brand"), measure.vars = c("Daily Liters", "Daily Units"))
end <- Sys.time()
BenchmarkResults[33, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Melt.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 100M 2N 1D 3G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Melt.csv"))
start <- Sys.time()
data.table::melt.data.table(data = data, id.vars = c("Date","Customer","Brand","Category"), measure.vars = c("Daily Liters", "Daily Units"))
end <- Sys.time()
BenchmarkResults[34, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Melt.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 100M 2N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Melt.csv"))
start <- Sys.time()
data.table::melt.data.table(data = data, id.vars = c("Date","Customer","Brand","Category","Beverage Flavor"), measure.vars = c("Daily Liters", "Daily Units"))
end <- Sys.time()
BenchmarkResults[35, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Melt.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 100M 3N 1D 0G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Melt.csv"))
start <- Sys.time()
data.table::melt.data.table(data = data, id.vars = "Date", measure.vars = c("Daily Liters", "Daily Units", "Daily Margin"))
end <- Sys.time()
BenchmarkResults[36, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Melt.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 100M 3N 1D 1G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Melt.csv"))
start <- Sys.time()
data.table::melt.data.table(data = data, id.vars = c("Date","Customer"), measure.vars = c("Daily Liters", "Daily Units", "Daily Margin"))
end <- Sys.time()
BenchmarkResults[37, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Melt.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 100M 3N 1D 2G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Melt.csv"))
start <- Sys.time()
data.table::melt.data.table(data = data, id.vars = c("Date","Customer","Brand"), measure.vars = c("Daily Liters", "Daily Units", "Daily Margin"))
end <- Sys.time()
BenchmarkResults[38, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Melt.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 100M 3N 1D 3G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Melt.csv"))
start <- Sys.time()
data.table::melt.data.table(data = data, id.vars = c("Date","Customer","Brand","Category"), measure.vars = c("Daily Liters", "Daily Units", "Daily Margin"))
end <- Sys.time()
BenchmarkResults[39, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Melt.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 100M 3N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Melt.csv"))
start <- Sys.time()
data.table::melt.data.table(data = data, id.vars = c("Date","Customer","Brand","Category","Beverage Flavor"), measure.vars = c("Daily Liters", "Daily Units", "Daily Margin"))
end <- Sys.time()
BenchmarkResults[40, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Melt.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 100M 4N 1D 0G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Melt.csv"))
start <- Sys.time()
data.table::melt.data.table(data = data, id.vars = "Date", measure.vars = c("Daily Liters", "Daily Units", "Daily Margin","Daily Revenue"))
end <- Sys.time()
BenchmarkResults[41, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Melt.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 100M 4N 1D 1G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Melt.csv"))
start <- Sys.time()
data.table::melt.data.table(data = data, id.vars = c("Date","Customer"), measure.vars = c("Daily Liters", "Daily Units", "Daily Margin","Daily Revenue"))
end <- Sys.time()
BenchmarkResults[42, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Melt.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 100M 4N 1D 2G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Melt.csv"))
start <- Sys.time()
data.table::melt.data.table(data = data, id.vars = c("Date","Customer","Brand"), measure.vars = c("Daily Liters", "Daily Units", "Daily Margin","Daily Revenue"))
end <- Sys.time()
BenchmarkResults[43, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Melt.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 100M 4N 1D 3G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Melt.csv"))
start <- Sys.time()
data.table::melt.data.table(data = data, id.vars = c("Date","Customer","Brand","Category"), measure.vars = c("Daily Liters", "Daily Units", "Daily Margin","Daily Revenue"))
end <- Sys.time()
BenchmarkResults[44, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Melt.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 100M 4N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Melt.csv"))
start <- Sys.time()
data.table::melt.data.table(data = data, id.vars = c("Date","Customer","Brand","Category","Beverage Flavor"), measure.vars = c("Daily Liters", "Daily Units", "Daily Margin","Daily Revenue"))
end <- Sys.time()
BenchmarkResults[45, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Melt.csv"))
rm(list = c("BenchmarkResults","data","end","start"))
gc()


BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResults_Melt.csv"))
BenchmarkResults[46, TimeInSeconds := BenchmarkResults[1:45, sum(TimeInSeconds)]]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResults_Melt.csv"))



