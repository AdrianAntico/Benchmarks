# Path to data storage
Path <- "C:/Users/Bizon/Documents/GitHub/rappwd/"

# Create results table
BenchmarkResults <- data.table::data.table(
  Framework = 'duckdb',
  Method = 'left join',
  Experiment = c(
    "1M 3N 1D 4G",
    "10M 3N 1D 4G",
    "100M 3N 1D 4G"
  ),

  TimeInSeconds = c(rep(-0.1, 3))
)

data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_LeftJoin.csv"))
rm(BenchmarkResults)

library(data.table)
library(duckdb)
library(DBI)
data <- data.table::fread(paste0(Path, "FakeBevData1M.csv"))
data.table::setnames(data, c("Beverage Flavor", "Daily Liters", "Daily Margin", "Daily Revenue", "Daily Units"), c("BeverageFlavor", "DailyLiters", "DailyMargin", "DailyRevenue", "DailyUnits"))
temp1 <- data[, .SD, .SDcols = c("Date","Customer","Brand","Category","BeverageFlavor","DailyLiters")]
temp2 <- data[, .SD, .SDcols = c("Date","Customer","Brand","Category","BeverageFlavor","DailyUnits")]
temp2 <- temp2[Brand != "Zingers"]
con = dbConnect(duckdb::duckdb())
ncores = parallel::detectCores()
invisible(dbExecute(con, sprintf("PRAGMA THREADS=%d", ncores)))
invisible(dbExecute(con, sprintf("SET THREADS=%d", ncores)))
dbWriteTable(con, "bmdata1M", temp1, overwrite = TRUE)
dbWriteTable(con, "bmdata1M_2", temp2, overwrite = TRUE)
rm(temp1, temp2)

# Query the schema of the table
rm(ncores)




data <- data.table::fread(paste0(Path, "FakeBevData1M.csv"))
data.table::setnames(data, c("Beverage Flavor", "Daily Liters", "Daily Margin", "Daily Revenue", "Daily Units"), c("BeverageFlavor", "DailyLiters", "DailyMargin", "DailyRevenue", "DailyUnits"))
temp2 <- data[, .SD, .SDcols = c("Date","Customer","Brand","Category","BeverageFlavor","DailyUnits","DailyMargin","DailyRevenue")]
temp2 <- temp2[Brand != "Zingers"]
dbWriteTable(con, "bmdata1M_2", temp2, overwrite = TRUE)
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_LeftJoin.csv"))
rts <- c(rep(1.1, 3))
for(i in 1:3) {
  print(i)
  start <- Sys.time()
  dbExecute(con, "CREATE TABLE ans AS
      SELECT * FROM bmdata1M
      LEFT JOIN
        bmdata1M_2
      USING (Date,Customer,Brand,Category,BeverageFlavor)")
  print(c(
    nr <- dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt,
    nc <- ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
  end <- Sys.time()
  invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
invisible(dbExecute(con, "DROP TABLE IF EXISTS bmdata1M_2"))
invisible(dbExecute(con, "DROP TABLE IF EXISTS bmdata1M"))
BenchmarkResults[1, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_LeftJoin.csv"))
rm(list = c("BenchmarkResults","end","start"))


###################################################################################################
###################################################################################################
###################################################################################################
###################################################################################################
###################################################################################################

# Aggregation 10M

# Left Join Numeric Variables:

## 10M 2N 1D 0G
data <- data.table::fread(paste0(Path, "FakeBevData10M.csv"))
data.table::setnames(data, c("Beverage Flavor", "Daily Liters", "Daily Margin", "Daily Revenue", "Daily Units"), c("BeverageFlavor", "DailyLiters", "DailyMargin", "DailyRevenue", "DailyUnits"))
temp1 <- data[, .SD, .SDcols = c("Date","Customer","Brand","Category","BeverageFlavor","DailyLiters")]
temp2 <- data[, .SD, .SDcols = c("Date","Customer","Brand","Category","BeverageFlavor","DailyUnits")]
temp2 <- temp2[Brand != "Zingers"]
con = dbConnect(duckdb::duckdb())
ncores = parallel::detectCores()
invisible(dbExecute(con, sprintf("PRAGMA THREADS=%d", ncores)))
invisible(dbExecute(con, sprintf("SET THREADS=%d", ncores)))
dbWriteTable(con, "bmdata1M", temp1, overwrite = TRUE)
dbWriteTable(con, "bmdata1M_2", temp2, overwrite = TRUE)
rm(temp1, temp2)

# Query the schema of the table
rm(ncores)




data <- data.table::fread(paste0(Path, "FakeBevData10M.csv"))
data.table::setnames(data, c("Beverage Flavor", "Daily Liters", "Daily Margin", "Daily Revenue", "Daily Units"), c("BeverageFlavor", "DailyLiters", "DailyMargin", "DailyRevenue", "DailyUnits"))
temp2 <- data[, .SD, .SDcols = c("Date","Customer","Brand","Category","BeverageFlavor","DailyUnits","DailyMargin","DailyRevenue")]
temp2 <- temp2[Brand != "Zingers"]
dbWriteTable(con, "bmdata1M_2", temp2, overwrite = TRUE)
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_LeftJoin.csv"))
rts <- c(rep(1.1, 3))
for(i in 1:3) {
  print(i)
  start <- Sys.time()
  dbExecute(con, "CREATE TABLE ans AS
      SELECT * FROM bmdata1M
      LEFT JOIN
        bmdata1M_2
      USING (Date,Customer,Brand,Category,BeverageFlavor)")
  print(c(
    nr <- dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt,
    nc <- ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
  end <- Sys.time()
  invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
invisible(dbExecute(con, "DROP TABLE IF EXISTS bmdata1M_2"))
invisible(dbExecute(con, "DROP TABLE IF EXISTS bmdata1M"))
BenchmarkResults[2, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_LeftJoin.csv"))
rm(list = c("BenchmarkResults","end","start"))


###################################################################################################
###################################################################################################
###################################################################################################
###################################################################################################
###################################################################################################

# Aggregation 100M

# Left Join Numeric Variables:

## 10M 2N 1D 0G
data <- data.table::fread(paste0(Path, "FakeBevData10M.csv"))
data.table::setnames(data, c("Beverage Flavor", "Daily Liters", "Daily Margin", "Daily Revenue", "Daily Units"), c("BeverageFlavor", "DailyLiters", "DailyMargin", "DailyRevenue", "DailyUnits"))
temp1 <- data[, .SD, .SDcols = c("Date","Customer","Brand","Category","BeverageFlavor","DailyLiters")]
temp2 <- data[, .SD, .SDcols = c("Date","Customer","Brand","Category","BeverageFlavor","DailyUnits")]
temp2 <- temp2[Brand != "Zingers"]
con = dbConnect(duckdb::duckdb())
ncores = parallel::detectCores()
invisible(dbExecute(con, sprintf("PRAGMA THREADS=%d", ncores)))
invisible(dbExecute(con, sprintf("SET THREADS=%d", ncores)))
dbWriteTable(con, "bmdata1M", temp1, overwrite = TRUE)
dbWriteTable(con, "bmdata1M_2", temp2, overwrite = TRUE)
rm(temp1, temp2)

# Query the schema of the table
rm(ncores)




data <- data.table::fread(paste0(Path, "FakeBevData100M.csv"))
data.table::setnames(data, c("Beverage Flavor", "Daily Liters", "Daily Margin", "Daily Revenue", "Daily Units"), c("BeverageFlavor", "DailyLiters", "DailyMargin", "DailyRevenue", "DailyUnits"))
temp2 <- data[, .SD, .SDcols = c("Date","Customer","Brand","Category","BeverageFlavor","DailyUnits","DailyMargin","DailyRevenue")]
temp2 <- temp2[Brand != "Zingers"]
dbWriteTable(con, "bmdata1M_2", temp2, overwrite = TRUE)
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_LeftJoin.csv"))
rts <- c(rep(1.1, 3))
for(i in 1:3) {
  print(i)
  start <- Sys.time()
  dbExecute(con, "CREATE TABLE ans AS
      SELECT * FROM bmdata1M
      LEFT JOIN
        bmdata1M_2
      USING (Date,Customer,Brand,Category,BeverageFlavor)")
  print(c(
    nr <- dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt,
    nc <- ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
  end <- Sys.time()
  invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
invisible(dbExecute(con, "DROP TABLE IF EXISTS bmdata1M_2"))
invisible(dbExecute(con, "DROP TABLE IF EXISTS bmdata1M"))
BenchmarkResults[3, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_LeftJoin.csv"))
rm(list = c("BenchmarkResults","end","start"))
