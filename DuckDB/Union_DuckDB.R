# Path to data storage
Path <- "C:/Users/Bizon/Documents/GitHub/rappwd/"

# Create results table
BenchmarkResults <- data.table::data.table(
  Framework = 'duckdb',
  Method = 'UNION',
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

data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Union.csv"))
rm(BenchmarkResults)

library(data.table)
library(duckdb)
library(DBI)
data <- data.table::fread(paste0(Path, "FakeBevData1M.csv"))
data.table::setnames(data, c("Beverage Flavor", "Daily Liters", "Daily Margin", "Daily Revenue", "Daily Units"), c("BeverageFlavor", "DailyLiters", "DailyMargin", "DailyRevenue", "DailyUnits"))
data2 <- data[, DailyLiters := DailyLiters + 1]
con = dbConnect(duckdb::duckdb())
ncores = parallel::detectCores()
invisible(dbExecute(con, sprintf("PRAGMA THREADS=%d", ncores)))
invisible(dbExecute(con, sprintf("SET THREADS=%d", ncores)))
dbWriteTable(con, "bmdata1M", data, overwrite = TRUE)
dbWriteTable(con, "bmdata1M_2", data2, overwrite = TRUE)
rm(data, data2)

# Query the schema of the table
rm(ncores)



# Melt 1M

# Melt Numeric Variable:

## 1M 2N 1D 0G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Union.csv"))
rts <- c(rep(1.1, 10))
dbExecute(con, "CREATE TABLE temp1 AS SELECT Date, DailyLiters FROM bmdata1M")
dbExecute(con, "CREATE TABLE temp2 AS SELECT Date, DailyLiters FROM bmdata1M_2")
for(i in 1:10) {
  print(i)
  start <- Sys.time()
  dbExecute(con, "CREATE TABLE ans AS
      SELECT * FROM temp1
      UNION ALL
      SELECT * FROM temp2")
  print(c(
    nr <- dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt,
    nc <- ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
  end <- Sys.time()
  invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
invisible(dbExecute(con, "DROP TABLE IF EXISTS temp1"))
invisible(dbExecute(con, "DROP TABLE IF EXISTS temp2"))
BenchmarkResults[1, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Union.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 1M 2N 1D 1G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Union.csv"))
rts <- c(rep(1.1, 10))
dbExecute(con, "CREATE TABLE temp1 AS SELECT Date, Customer, DailyLiters FROM bmdata1M")
dbExecute(con, "CREATE TABLE temp2 AS SELECT Date, Customer, DailyLiters FROM bmdata1M_2")
for(i in 1:10) {
  print(i)
  start <- Sys.time()
  dbExecute(con, "CREATE TABLE ans AS
      SELECT * FROM temp1
      UNION ALL
      SELECT * FROM temp2")
  print(c(
    nr <- dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt,
    nc <- ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
  end <- Sys.time()
  invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
invisible(dbExecute(con, "DROP TABLE IF EXISTS temp1"))
invisible(dbExecute(con, "DROP TABLE IF EXISTS temp2"))
BenchmarkResults[2, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Union.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 1M 2N 1D 2G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Union.csv"))
rts <- c(rep(1.1, 10))
dbExecute(con, "CREATE TABLE temp1 AS SELECT Date, Customer, Brand, DailyLiters FROM bmdata1M")
dbExecute(con, "CREATE TABLE temp2 AS SELECT Date, Customer, Brand, DailyLiters FROM bmdata1M_2")
for(i in 1:10) {
  print(i)
  start <- Sys.time()
  dbExecute(con, "CREATE TABLE ans AS
      SELECT * FROM temp1
      UNION ALL
      SELECT * FROM temp2")
  print(c(
    nr <- dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt,
    nc <- ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
  end <- Sys.time()
  invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
invisible(dbExecute(con, "DROP TABLE IF EXISTS temp1"))
invisible(dbExecute(con, "DROP TABLE IF EXISTS temp2"))
BenchmarkResults[3, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Union.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 1M 2N 1D 3G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Union.csv"))
rts <- c(rep(1.1, 10))
dbExecute(con, "CREATE TABLE temp1 AS SELECT Date, Customer, Brand, Category, DailyLiters FROM bmdata1M")
dbExecute(con, "CREATE TABLE temp2 AS SELECT Date, Customer, Brand, Category, DailyLiters FROM bmdata1M_2")
for(i in 1:10) {
  print(i)
  start <- Sys.time()
  dbExecute(con, "CREATE TABLE ans AS
      SELECT * FROM temp1
      UNION ALL
      SELECT * FROM temp2")
  print(c(
    nr <- dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt,
    nc <- ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
  end <- Sys.time()
  invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
invisible(dbExecute(con, "DROP TABLE IF EXISTS temp1"))
invisible(dbExecute(con, "DROP TABLE IF EXISTS temp2"))
BenchmarkResults[4, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Union.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 1M 2N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Union.csv"))
rts <- c(rep(1.1, 10))
dbExecute(con, "CREATE TABLE temp1 AS SELECT Date, Customer, Brand, Category, BeverageFlavor, DailyLiters FROM bmdata1M")
dbExecute(con, "CREATE TABLE temp2 AS SELECT Date, Customer, Brand, Category, BeverageFlavor, DailyLiters FROM bmdata1M_2")
for(i in 1:10) {
  print(i)
  start <- Sys.time()
  dbExecute(con, "CREATE TABLE ans AS
      SELECT * FROM temp1
      UNION ALL
      SELECT * FROM temp2")
  print(c(
    nr <- dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt,
    nc <- ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
  end <- Sys.time()
  invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
invisible(dbExecute(con, "DROP TABLE IF EXISTS temp1"))
invisible(dbExecute(con, "DROP TABLE IF EXISTS temp2"))
BenchmarkResults[5, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Union.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()


## 1M 2N 1D 0G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Union.csv"))
rts <- c(rep(1.1, 10))
dbExecute(con, "CREATE TABLE temp1 AS SELECT Date, DailyLiters, DailyUnits FROM bmdata1M")
dbExecute(con, "CREATE TABLE temp2 AS SELECT Date, DailyLiters, DailyUnits FROM bmdata1M_2")
for(i in 1:10) {
  print(i)
  start <- Sys.time()
  dbExecute(con, "CREATE TABLE ans AS
      SELECT * FROM temp1
      UNION ALL
      SELECT * FROM temp2")
  print(c(
    nr <- dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt,
    nc <- ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
  end <- Sys.time()
  invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
invisible(dbExecute(con, "DROP TABLE IF EXISTS temp1"))
invisible(dbExecute(con, "DROP TABLE IF EXISTS temp2"))
BenchmarkResults[6, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Union.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 1M 2N 1D 1G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Union.csv"))
rts <- c(rep(1.1, 10))
dbExecute(con, "CREATE TABLE temp1 AS SELECT Date, Customer, DailyLiters, DailyUnits FROM bmdata1M")
dbExecute(con, "CREATE TABLE temp2 AS SELECT Date, Customer, DailyLiters, DailyUnits FROM bmdata1M_2")
for(i in 1:10) {
  print(i)
  start <- Sys.time()
  dbExecute(con, "CREATE TABLE ans AS
      SELECT * FROM temp1
      UNION ALL
      SELECT * FROM temp2")
  print(c(
    nr <- dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt,
    nc <- ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
  end <- Sys.time()
  invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
invisible(dbExecute(con, "DROP TABLE IF EXISTS temp1"))
invisible(dbExecute(con, "DROP TABLE IF EXISTS temp2"))
BenchmarkResults[7, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Union.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 1M 2N 1D 2G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Union.csv"))
rts <- c(rep(1.1, 10))
dbExecute(con, "CREATE TABLE temp1 AS SELECT Date, Customer, Brand, DailyLiters, DailyUnits FROM bmdata1M")
dbExecute(con, "CREATE TABLE temp2 AS SELECT Date, Customer, Brand, DailyLiters, DailyUnits FROM bmdata1M_2")
for(i in 1:10) {
  print(i)
  start <- Sys.time()
  dbExecute(con, "CREATE TABLE ans AS
      SELECT * FROM temp1
      UNION ALL
      SELECT * FROM temp2")
  print(c(
    nr <- dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt,
    nc <- ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
  end <- Sys.time()
  invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
invisible(dbExecute(con, "DROP TABLE IF EXISTS temp1"))
invisible(dbExecute(con, "DROP TABLE IF EXISTS temp2"))
BenchmarkResults[8, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Union.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 1M 2N 1D 3G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Union.csv"))
rts <- c(rep(1.1, 10))
dbExecute(con, "CREATE TABLE temp1 AS SELECT Date, Customer, Brand, Category, DailyLiters, DailyUnits FROM bmdata1M")
dbExecute(con, "CREATE TABLE temp2 AS SELECT Date, Customer, Brand, Category, DailyLiters, DailyUnits FROM bmdata1M_2")
for(i in 1:10) {
  print(i)
  start <- Sys.time()
  dbExecute(con, "CREATE TABLE ans AS
      SELECT * FROM temp1
      UNION ALL
      SELECT * FROM temp2")
  print(c(
    nr <- dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt,
    nc <- ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
  end <- Sys.time()
  invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
invisible(dbExecute(con, "DROP TABLE IF EXISTS temp1"))
invisible(dbExecute(con, "DROP TABLE IF EXISTS temp2"))
BenchmarkResults[9, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Union.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 1M 2N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Union.csv"))
rts <- c(rep(1.1, 10))
dbExecute(con, "CREATE TABLE temp1 AS SELECT Date, Customer, Brand, Category, BeverageFlavor, DailyLiters, DailyUnits FROM bmdata1M")
dbExecute(con, "CREATE TABLE temp2 AS SELECT Date, Customer, Brand, Category, BeverageFlavor, DailyLiters, DailyUnits FROM bmdata1M_2")
for(i in 1:10) {
  print(i)
  start <- Sys.time()
  dbExecute(con, "CREATE TABLE ans AS
      SELECT * FROM temp1
      UNION ALL
      SELECT * FROM temp2")
  print(c(
    nr <- dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt,
    nc <- ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
  end <- Sys.time()
  invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
invisible(dbExecute(con, "DROP TABLE IF EXISTS temp1"))
invisible(dbExecute(con, "DROP TABLE IF EXISTS temp2"))
BenchmarkResults[10, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Union.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()


## 1M 2N 1D 0G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Union.csv"))
rts <- c(rep(1.1, 10))
dbExecute(con, "CREATE TABLE temp1 AS SELECT Date, DailyLiters, DailyUnits, DailyMargin FROM bmdata1M")
dbExecute(con, "CREATE TABLE temp2 AS SELECT Date, DailyLiters, DailyUnits, DailyMargin FROM bmdata1M_2")
for(i in 1:10) {
  print(i)
  start <- Sys.time()
  dbExecute(con, "CREATE TABLE ans AS
      SELECT * FROM temp1
      UNION ALL
      SELECT * FROM temp2")
  print(c(
    nr <- dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt,
    nc <- ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
  end <- Sys.time()
  invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
invisible(dbExecute(con, "DROP TABLE IF EXISTS temp1"))
invisible(dbExecute(con, "DROP TABLE IF EXISTS temp2"))
BenchmarkResults[11, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Union.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 1M 2N 1D 1G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Union.csv"))
rts <- c(rep(1.1, 10))
dbExecute(con, "CREATE TABLE temp1 AS SELECT Date, Customer, DailyLiters, DailyUnits, DailyMargin FROM bmdata1M")
dbExecute(con, "CREATE TABLE temp2 AS SELECT Date, Customer, DailyLiters, DailyUnits, DailyMargin FROM bmdata1M_2")
for(i in 1:10) {
  print(i)
  start <- Sys.time()
  dbExecute(con, "CREATE TABLE ans AS
      SELECT * FROM temp1
      UNION ALL
      SELECT * FROM temp2")
  print(c(
    nr <- dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt,
    nc <- ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
  end <- Sys.time()
  invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
invisible(dbExecute(con, "DROP TABLE IF EXISTS temp1"))
invisible(dbExecute(con, "DROP TABLE IF EXISTS temp2"))
BenchmarkResults[12, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Union.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 1M 2N 1D 2G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Union.csv"))
rts <- c(rep(1.1, 10))
dbExecute(con, "CREATE TABLE temp1 AS SELECT Date, Customer, Brand, DailyLiters, DailyUnits, DailyMargin FROM bmdata1M")
dbExecute(con, "CREATE TABLE temp2 AS SELECT Date, Customer, Brand, DailyLiters, DailyUnits, DailyMargin FROM bmdata1M_2")
for(i in 1:10) {
  print(i)
  start <- Sys.time()
  dbExecute(con, "CREATE TABLE ans AS
      SELECT * FROM temp1
      UNION ALL
      SELECT * FROM temp2")
  print(c(
    nr <- dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt,
    nc <- ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
  end <- Sys.time()
  invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
invisible(dbExecute(con, "DROP TABLE IF EXISTS temp1"))
invisible(dbExecute(con, "DROP TABLE IF EXISTS temp2"))
BenchmarkResults[13, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Union.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 1M 2N 1D 3G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Union.csv"))
rts <- c(rep(1.1, 10))
dbExecute(con, "CREATE TABLE temp1 AS SELECT Date, Customer, Brand, Category, DailyLiters, DailyUnits, DailyMargin FROM bmdata1M")
dbExecute(con, "CREATE TABLE temp2 AS SELECT Date, Customer, Brand, Category, DailyLiters, DailyUnits, DailyMargin FROM bmdata1M_2")
for(i in 1:10) {
  print(i)
  start <- Sys.time()
  dbExecute(con, "CREATE TABLE ans AS
      SELECT * FROM temp1
      UNION ALL
      SELECT * FROM temp2")
  print(c(
    nr <- dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt,
    nc <- ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
  end <- Sys.time()
  invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
invisible(dbExecute(con, "DROP TABLE IF EXISTS temp1"))
invisible(dbExecute(con, "DROP TABLE IF EXISTS temp2"))
BenchmarkResults[14, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Union.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 1M 2N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Union.csv"))
rts <- c(rep(1.1, 10))
dbExecute(con, "CREATE TABLE temp1 AS SELECT Date, Customer, Brand, Category, BeverageFlavor, DailyLiters, DailyUnits, DailyMargin FROM bmdata1M")
dbExecute(con, "CREATE TABLE temp2 AS SELECT Date, Customer, Brand, Category, BeverageFlavor, DailyLiters, DailyUnits, DailyMargin FROM bmdata1M_2")
for(i in 1:10) {
  print(i)
  start <- Sys.time()
  dbExecute(con, "CREATE TABLE ans AS
      SELECT * FROM temp1
      UNION ALL
      SELECT * FROM temp2")
  print(c(
    nr <- dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt,
    nc <- ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
  end <- Sys.time()
  invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
invisible(dbExecute(con, "DROP TABLE IF EXISTS temp1"))
invisible(dbExecute(con, "DROP TABLE IF EXISTS temp2"))
BenchmarkResults[15, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Union.csv"))
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
data.table::setnames(data, c("Beverage Flavor", "Daily Liters", "Daily Margin", "Daily Revenue", "Daily Units"), c("BeverageFlavor", "DailyLiters", "DailyMargin", "DailyRevenue", "DailyUnits"))
data2 <- data[, DailyLiters := DailyLiters + 1]
con = dbConnect(duckdb::duckdb())
ncores = parallel::detectCores()
invisible(dbExecute(con, sprintf("PRAGMA THREADS=%d", ncores)))
invisible(dbExecute(con, sprintf("SET THREADS=%d", ncores)))
dbWriteTable(con, "bmdata10M", data, overwrite = TRUE)
dbWriteTable(con, "bmdata10M_2", data2, overwrite = TRUE)
rm(data, data2)

BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Union.csv"))
rts <- c(rep(1.1, 10))
dbExecute(con, "CREATE TABLE temp1 AS SELECT Date, DailyLiters FROM bmdata10M")
dbExecute(con, "CREATE TABLE temp2 AS SELECT Date, DailyLiters FROM bmdata10M_2")
for(i in 1:10) {
  print(i)
  start <- Sys.time()
  dbExecute(con, "CREATE TABLE ans AS
      SELECT * FROM temp1
      UNION ALL
      SELECT * FROM temp2")
  print(c(
    nr <- dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt,
    nc <- ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
  end <- Sys.time()
  invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
invisible(dbExecute(con, "DROP TABLE IF EXISTS temp1"))
invisible(dbExecute(con, "DROP TABLE IF EXISTS temp2"))
BenchmarkResults[16, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Union.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 10M 2N 1D 1G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Union.csv"))
rts <- c(rep(1.1, 10))
dbExecute(con, "CREATE TABLE temp1 AS SELECT Date, Customer, DailyLiters FROM bmdata10M")
dbExecute(con, "CREATE TABLE temp2 AS SELECT Date, Customer, DailyLiters FROM bmdata10M_2")
for(i in 1:10) {
  print(i)
  start <- Sys.time()
  dbExecute(con, "CREATE TABLE ans AS
      SELECT * FROM temp1
      UNION ALL
      SELECT * FROM temp2")
  print(c(
    nr <- dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt,
    nc <- ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
  end <- Sys.time()
  invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
invisible(dbExecute(con, "DROP TABLE IF EXISTS temp1"))
invisible(dbExecute(con, "DROP TABLE IF EXISTS temp2"))
BenchmarkResults[17, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Union.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 10M 2N 1D 2G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Union.csv"))
rts <- c(rep(1.1, 10))
dbExecute(con, "CREATE TABLE temp1 AS SELECT Date, Customer, Brand, DailyLiters FROM bmdata10M")
dbExecute(con, "CREATE TABLE temp2 AS SELECT Date, Customer, Brand, DailyLiters FROM bmdata10M_2")
for(i in 1:10) {
  print(i)
  start <- Sys.time()
  dbExecute(con, "CREATE TABLE ans AS
      SELECT * FROM temp1
      UNION ALL
      SELECT * FROM temp2")
  print(c(
    nr <- dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt,
    nc <- ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
  end <- Sys.time()
  invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
invisible(dbExecute(con, "DROP TABLE IF EXISTS temp1"))
invisible(dbExecute(con, "DROP TABLE IF EXISTS temp2"))
BenchmarkResults[18, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Union.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 10M 2N 1D 3G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Union.csv"))
rts <- c(rep(1.1, 10))
dbExecute(con, "CREATE TABLE temp1 AS SELECT Date, Customer, Brand, Category, DailyLiters FROM bmdata10M")
dbExecute(con, "CREATE TABLE temp2 AS SELECT Date, Customer, Brand, Category, DailyLiters FROM bmdata10M_2")
for(i in 1:10) {
  print(i)
  start <- Sys.time()
  dbExecute(con, "CREATE TABLE ans AS
      SELECT * FROM temp1
      UNION ALL
      SELECT * FROM temp2")
  print(c(
    nr <- dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt,
    nc <- ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
  end <- Sys.time()
  invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
invisible(dbExecute(con, "DROP TABLE IF EXISTS temp1"))
invisible(dbExecute(con, "DROP TABLE IF EXISTS temp2"))
BenchmarkResults[19, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Union.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 10M 2N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Union.csv"))
rts <- c(rep(1.1, 10))
dbExecute(con, "CREATE TABLE temp1 AS SELECT Date, Customer, Brand, Category, BeverageFlavor, DailyLiters FROM bmdata10M")
dbExecute(con, "CREATE TABLE temp2 AS SELECT Date, Customer, Brand, Category, BeverageFlavor, DailyLiters FROM bmdata10M_2")
for(i in 1:10) {
  print(i)
  start <- Sys.time()
  dbExecute(con, "CREATE TABLE ans AS
      SELECT * FROM temp1
      UNION ALL
      SELECT * FROM temp2")
  print(c(
    nr <- dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt,
    nc <- ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
  end <- Sys.time()
  invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
invisible(dbExecute(con, "DROP TABLE IF EXISTS temp1"))
invisible(dbExecute(con, "DROP TABLE IF EXISTS temp2"))
BenchmarkResults[20, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Union.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()


## 10M 2N 1D 0G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Union.csv"))
rts <- c(rep(1.1, 10))
dbExecute(con, "CREATE TABLE temp1 AS SELECT Date, DailyLiters, DailyUnits FROM bmdata10M")
dbExecute(con, "CREATE TABLE temp2 AS SELECT Date, DailyLiters, DailyUnits FROM bmdata10M_2")
for(i in 1:10) {
  print(i)
  start <- Sys.time()
  dbExecute(con, "CREATE TABLE ans AS
      SELECT * FROM temp1
      UNION ALL
      SELECT * FROM temp2")
  print(c(
    nr <- dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt,
    nc <- ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
  end <- Sys.time()
  invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
invisible(dbExecute(con, "DROP TABLE IF EXISTS temp1"))
invisible(dbExecute(con, "DROP TABLE IF EXISTS temp2"))
BenchmarkResults[21, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Union.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 10M 2N 1D 1G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Union.csv"))
rts <- c(rep(1.1, 10))
dbExecute(con, "CREATE TABLE temp1 AS SELECT Date, Customer, DailyLiters, DailyUnits FROM bmdata10M")
dbExecute(con, "CREATE TABLE temp2 AS SELECT Date, Customer, DailyLiters, DailyUnits FROM bmdata10M_2")
for(i in 1:10) {
  print(i)
  start <- Sys.time()
  dbExecute(con, "CREATE TABLE ans AS
      SELECT * FROM temp1
      UNION ALL
      SELECT * FROM temp2")
  print(c(
    nr <- dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt,
    nc <- ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
  end <- Sys.time()
  invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
invisible(dbExecute(con, "DROP TABLE IF EXISTS temp1"))
invisible(dbExecute(con, "DROP TABLE IF EXISTS temp2"))
BenchmarkResults[22, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Union.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 10M 2N 1D 2G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Union.csv"))
rts <- c(rep(1.1, 10))
dbExecute(con, "CREATE TABLE temp1 AS SELECT Date, Customer, Brand, DailyLiters, DailyUnits FROM bmdata10M")
dbExecute(con, "CREATE TABLE temp2 AS SELECT Date, Customer, Brand, DailyLiters, DailyUnits FROM bmdata10M_2")
for(i in 1:10) {
  print(i)
  start <- Sys.time()
  dbExecute(con, "CREATE TABLE ans AS
      SELECT * FROM temp1
      UNION ALL
      SELECT * FROM temp2")
  print(c(
    nr <- dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt,
    nc <- ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
  end <- Sys.time()
  invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
invisible(dbExecute(con, "DROP TABLE IF EXISTS temp1"))
invisible(dbExecute(con, "DROP TABLE IF EXISTS temp2"))
BenchmarkResults[23, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Union.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 10M 2N 1D 3G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Union.csv"))
rts <- c(rep(1.1, 10))
dbExecute(con, "CREATE TABLE temp1 AS SELECT Date, Customer, Brand, Category, DailyLiters, DailyUnits FROM bmdata10M")
dbExecute(con, "CREATE TABLE temp2 AS SELECT Date, Customer, Brand, Category, DailyLiters, DailyUnits FROM bmdata10M_2")
for(i in 1:10) {
  print(i)
  start <- Sys.time()
  dbExecute(con, "CREATE TABLE ans AS
      SELECT * FROM temp1
      UNION ALL
      SELECT * FROM temp2")
  print(c(
    nr <- dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt,
    nc <- ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
  end <- Sys.time()
  invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
invisible(dbExecute(con, "DROP TABLE IF EXISTS temp1"))
invisible(dbExecute(con, "DROP TABLE IF EXISTS temp2"))
BenchmarkResults[24, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Union.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 10M 2N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Union.csv"))
rts <- c(rep(1.1, 10))
dbExecute(con, "CREATE TABLE temp1 AS SELECT Date, Customer, Brand, Category, BeverageFlavor, DailyLiters, DailyUnits FROM bmdata10M")
dbExecute(con, "CREATE TABLE temp2 AS SELECT Date, Customer, Brand, Category, BeverageFlavor, DailyLiters, DailyUnits FROM bmdata10M_2")
for(i in 1:10) {
  print(i)
  start <- Sys.time()
  dbExecute(con, "CREATE TABLE ans AS
      SELECT * FROM temp1
      UNION ALL
      SELECT * FROM temp2")
  print(c(
    nr <- dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt,
    nc <- ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
  end <- Sys.time()
  invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
invisible(dbExecute(con, "DROP TABLE IF EXISTS temp1"))
invisible(dbExecute(con, "DROP TABLE IF EXISTS temp2"))
BenchmarkResults[25, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Union.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()


## 10M 2N 1D 0G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Union.csv"))
rts <- c(rep(1.1, 10))
dbExecute(con, "CREATE TABLE temp1 AS SELECT Date, DailyLiters, DailyUnits, DailyMargin FROM bmdata10M")
dbExecute(con, "CREATE TABLE temp2 AS SELECT Date, DailyLiters, DailyUnits, DailyMargin FROM bmdata10M_2")
for(i in 1:10) {
  print(i)
  start <- Sys.time()
  dbExecute(con, "CREATE TABLE ans AS
      SELECT * FROM temp1
      UNION ALL
      SELECT * FROM temp2")
  print(c(
    nr <- dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt,
    nc <- ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
  end <- Sys.time()
  invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
invisible(dbExecute(con, "DROP TABLE IF EXISTS temp1"))
invisible(dbExecute(con, "DROP TABLE IF EXISTS temp2"))
BenchmarkResults[26, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Union.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 10M 2N 1D 1G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Union.csv"))
rts <- c(rep(1.1, 10))
dbExecute(con, "CREATE TABLE temp1 AS SELECT Date, Customer, DailyLiters, DailyUnits, DailyMargin FROM bmdata10M")
dbExecute(con, "CREATE TABLE temp2 AS SELECT Date, Customer, DailyLiters, DailyUnits, DailyMargin FROM bmdata10M_2")
for(i in 1:10) {
  print(i)
  start <- Sys.time()
  dbExecute(con, "CREATE TABLE ans AS
      SELECT * FROM temp1
      UNION ALL
      SELECT * FROM temp2")
  print(c(
    nr <- dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt,
    nc <- ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
  end <- Sys.time()
  invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
invisible(dbExecute(con, "DROP TABLE IF EXISTS temp1"))
invisible(dbExecute(con, "DROP TABLE IF EXISTS temp2"))
BenchmarkResults[27, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Union.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 10M 2N 1D 2G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Union.csv"))
rts <- c(rep(1.1, 10))
dbExecute(con, "CREATE TABLE temp1 AS SELECT Date, Customer, Brand, DailyLiters, DailyUnits, DailyMargin FROM bmdata10M")
dbExecute(con, "CREATE TABLE temp2 AS SELECT Date, Customer, Brand, DailyLiters, DailyUnits, DailyMargin FROM bmdata10M_2")
for(i in 1:10) {
  print(i)
  start <- Sys.time()
  dbExecute(con, "CREATE TABLE ans AS
      SELECT * FROM temp1
      UNION ALL
      SELECT * FROM temp2")
  print(c(
    nr <- dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt,
    nc <- ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
  end <- Sys.time()
  invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
invisible(dbExecute(con, "DROP TABLE IF EXISTS temp1"))
invisible(dbExecute(con, "DROP TABLE IF EXISTS temp2"))
BenchmarkResults[28, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Union.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 10M 2N 1D 3G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Union.csv"))
rts <- c(rep(1.1, 10))
dbExecute(con, "CREATE TABLE temp1 AS SELECT Date, Customer, Brand, Category, DailyLiters, DailyUnits, DailyMargin FROM bmdata10M")
dbExecute(con, "CREATE TABLE temp2 AS SELECT Date, Customer, Brand, Category, DailyLiters, DailyUnits, DailyMargin FROM bmdata10M_2")
for(i in 1:10) {
  print(i)
  start <- Sys.time()
  dbExecute(con, "CREATE TABLE ans AS
      SELECT * FROM temp1
      UNION ALL
      SELECT * FROM temp2")
  print(c(
    nr <- dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt,
    nc <- ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
  end <- Sys.time()
  invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
invisible(dbExecute(con, "DROP TABLE IF EXISTS temp1"))
invisible(dbExecute(con, "DROP TABLE IF EXISTS temp2"))
BenchmarkResults[29, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Union.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 10M 2N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Union.csv"))
rts <- c(rep(1.1, 10))
dbExecute(con, "CREATE TABLE temp1 AS SELECT Date, Customer, Brand, Category, BeverageFlavor, DailyLiters, DailyUnits, DailyMargin FROM bmdata10M")
dbExecute(con, "CREATE TABLE temp2 AS SELECT Date, Customer, Brand, Category, BeverageFlavor, DailyLiters, DailyUnits, DailyMargin FROM bmdata10M_2")
for(i in 1:10) {
  print(i)
  start <- Sys.time()
  dbExecute(con, "CREATE TABLE ans AS
      SELECT * FROM temp1
      UNION ALL
      SELECT * FROM temp2")
  print(c(
    nr <- dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt,
    nc <- ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
  end <- Sys.time()
  invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
invisible(dbExecute(con, "DROP TABLE IF EXISTS temp1"))
invisible(dbExecute(con, "DROP TABLE IF EXISTS temp2"))
BenchmarkResults[30, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Union.csv"))
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
data.table::setnames(data, c("Beverage Flavor", "Daily Liters", "Daily Margin", "Daily Revenue", "Daily Units"), c("BeverageFlavor", "DailyLiters", "DailyMargin", "DailyRevenue", "DailyUnits"))
data2 <- data[, DailyLiters := DailyLiters + 1]
con = dbConnect(duckdb::duckdb())
ncores = parallel::detectCores()
invisible(dbExecute(con, sprintf("PRAGMA THREADS=%d", ncores)))
invisible(dbExecute(con, sprintf("SET THREADS=%d", ncores)))
dbWriteTable(con, "bmdata100M", data, overwrite = TRUE)
dbWriteTable(con, "bmdata100M_2", data2, overwrite = TRUE)
rm(data, data2)

BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Union.csv"))
rts <- c(rep(1.1, 10))
dbExecute(con, "CREATE TABLE temp1 AS SELECT Date, DailyLiters FROM bmdata100M")
dbExecute(con, "CREATE TABLE temp2 AS SELECT Date, DailyLiters FROM bmdata100M_2")
for(i in 1:10) {
  print(i)
  start <- Sys.time()
  dbExecute(con, "CREATE TABLE ans AS
      SELECT * FROM temp1
      UNION ALL
      SELECT * FROM temp2")
  print(c(
    nr <- dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt,
    nc <- ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
  end <- Sys.time()
  invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
invisible(dbExecute(con, "DROP TABLE IF EXISTS temp1"))
invisible(dbExecute(con, "DROP TABLE IF EXISTS temp2"))
BenchmarkResults[31, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Union.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 100M 2N 1D 1G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Union.csv"))
rts <- c(rep(1.1, 10))
dbExecute(con, "CREATE TABLE temp1 AS SELECT Date, Customer, DailyLiters FROM bmdata100M")
dbExecute(con, "CREATE TABLE temp2 AS SELECT Date, Customer, DailyLiters FROM bmdata100M_2")
for(i in 1:10) {
  print(i)
  start <- Sys.time()
  dbExecute(con, "CREATE TABLE ans AS
      SELECT * FROM temp1
      UNION ALL
      SELECT * FROM temp2")
  print(c(
    nr <- dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt,
    nc <- ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
  end <- Sys.time()
  invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
invisible(dbExecute(con, "DROP TABLE IF EXISTS temp1"))
invisible(dbExecute(con, "DROP TABLE IF EXISTS temp2"))
BenchmarkResults[32, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Union.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 100M 2N 1D 2G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Union.csv"))
rts <- c(rep(1.1, 10))
dbExecute(con, "CREATE TABLE temp1 AS SELECT Date, Customer, Brand, DailyLiters FROM bmdata100M")
dbExecute(con, "CREATE TABLE temp2 AS SELECT Date, Customer, Brand, DailyLiters FROM bmdata100M_2")
for(i in 1:10) {
  print(i)
  start <- Sys.time()
  dbExecute(con, "CREATE TABLE ans AS
      SELECT * FROM temp1
      UNION ALL
      SELECT * FROM temp2")
  print(c(
    nr <- dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt,
    nc <- ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
  end <- Sys.time()
  invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
invisible(dbExecute(con, "DROP TABLE IF EXISTS temp1"))
invisible(dbExecute(con, "DROP TABLE IF EXISTS temp2"))
BenchmarkResults[33, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Union.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 100M 2N 1D 3G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Union.csv"))
rts <- c(rep(1.1, 10))
dbExecute(con, "CREATE TABLE temp1 AS SELECT Date, Customer, Brand, Category, DailyLiters FROM bmdata100M")
dbExecute(con, "CREATE TABLE temp2 AS SELECT Date, Customer, Brand, Category, DailyLiters FROM bmdata100M_2")
for(i in 1:10) {
  print(i)
  start <- Sys.time()
  dbExecute(con, "CREATE TABLE ans AS
      SELECT * FROM temp1
      UNION ALL
      SELECT * FROM temp2")
  print(c(
    nr <- dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt,
    nc <- ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
  end <- Sys.time()
  invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
invisible(dbExecute(con, "DROP TABLE IF EXISTS temp1"))
invisible(dbExecute(con, "DROP TABLE IF EXISTS temp2"))
BenchmarkResults[34, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Union.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 100M 2N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Union.csv"))
rts <- c(rep(1.1, 10))
dbExecute(con, "CREATE TABLE temp1 AS SELECT Date, Customer, Brand, Category, BeverageFlavor, DailyLiters FROM bmdata100M")
dbExecute(con, "CREATE TABLE temp2 AS SELECT Date, Customer, Brand, Category, BeverageFlavor, DailyLiters FROM bmdata100M_2")
for(i in 1:10) {
  print(i)
  start <- Sys.time()
  dbExecute(con, "CREATE TABLE ans AS
      SELECT * FROM temp1
      UNION ALL
      SELECT * FROM temp2")
  print(c(
    nr <- dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt,
    nc <- ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
  end <- Sys.time()
  invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
invisible(dbExecute(con, "DROP TABLE IF EXISTS temp1"))
invisible(dbExecute(con, "DROP TABLE IF EXISTS temp2"))
BenchmarkResults[3, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Union.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()


## 100M 2N 1D 0G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Union.csv"))
rts <- c(rep(1.1, 10))
dbExecute(con, "CREATE TABLE temp1 AS SELECT Date, DailyLiters, DailyUnits FROM bmdata100M")
dbExecute(con, "CREATE TABLE temp2 AS SELECT Date, DailyLiters, DailyUnits FROM bmdata100M_2")
for(i in 1:10) {
  print(i)
  start <- Sys.time()
  dbExecute(con, "CREATE TABLE ans AS
      SELECT * FROM temp1
      UNION ALL
      SELECT * FROM temp2")
  print(c(
    nr <- dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt,
    nc <- ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
  end <- Sys.time()
  invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
invisible(dbExecute(con, "DROP TABLE IF EXISTS temp1"))
invisible(dbExecute(con, "DROP TABLE IF EXISTS temp2"))
BenchmarkResults[36, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Union.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 100M 2N 1D 1G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Union.csv"))
rts <- c(rep(1.1, 10))
dbExecute(con, "CREATE TABLE temp1 AS SELECT Date, Customer, DailyLiters, DailyUnits FROM bmdata100M")
dbExecute(con, "CREATE TABLE temp2 AS SELECT Date, Customer, DailyLiters, DailyUnits FROM bmdata100M_2")
for(i in 1:10) {
  print(i)
  start <- Sys.time()
  dbExecute(con, "CREATE TABLE ans AS
      SELECT * FROM temp1
      UNION ALL
      SELECT * FROM temp2")
  print(c(
    nr <- dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt,
    nc <- ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
  end <- Sys.time()
  invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
invisible(dbExecute(con, "DROP TABLE IF EXISTS temp1"))
invisible(dbExecute(con, "DROP TABLE IF EXISTS temp2"))
BenchmarkResults[37, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Union.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 100M 2N 1D 2G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Union.csv"))
rts <- c(rep(1.1, 10))
dbExecute(con, "CREATE TABLE temp1 AS SELECT Date, Customer, Brand, DailyLiters, DailyUnits FROM bmdata100M")
dbExecute(con, "CREATE TABLE temp2 AS SELECT Date, Customer, Brand, DailyLiters, DailyUnits FROM bmdata100M_2")
for(i in 1:10) {
  print(i)
  start <- Sys.time()
  dbExecute(con, "CREATE TABLE ans AS
      SELECT * FROM temp1
      UNION ALL
      SELECT * FROM temp2")
  print(c(
    nr <- dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt,
    nc <- ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
  end <- Sys.time()
  invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
invisible(dbExecute(con, "DROP TABLE IF EXISTS temp1"))
invisible(dbExecute(con, "DROP TABLE IF EXISTS temp2"))
BenchmarkResults[38, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Union.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 100M 2N 1D 3G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Union.csv"))
rts <- c(rep(1.1, 10))
dbExecute(con, "CREATE TABLE temp1 AS SELECT Date, Customer, Brand, Category, DailyLiters, DailyUnits FROM bmdata100M")
dbExecute(con, "CREATE TABLE temp2 AS SELECT Date, Customer, Brand, Category, DailyLiters, DailyUnits FROM bmdata100M_2")
for(i in 1:10) {
  print(i)
  start <- Sys.time()
  dbExecute(con, "CREATE TABLE ans AS
      SELECT * FROM temp1
      UNION ALL
      SELECT * FROM temp2")
  print(c(
    nr <- dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt,
    nc <- ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
  end <- Sys.time()
  invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
invisible(dbExecute(con, "DROP TABLE IF EXISTS temp1"))
invisible(dbExecute(con, "DROP TABLE IF EXISTS temp2"))
BenchmarkResults[39, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Union.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 100M 2N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Union.csv"))
rts <- c(rep(1.1, 10))
dbExecute(con, "CREATE TABLE temp1 AS SELECT Date, Customer, Brand, Category, BeverageFlavor, DailyLiters, DailyUnits FROM bmdata100M")
dbExecute(con, "CREATE TABLE temp2 AS SELECT Date, Customer, Brand, Category, BeverageFlavor, DailyLiters, DailyUnits FROM bmdata100M_2")
for(i in 1:10) {
  print(i)
  start <- Sys.time()
  dbExecute(con, "CREATE TABLE ans AS
      SELECT * FROM temp1
      UNION ALL
      SELECT * FROM temp2")
  print(c(
    nr <- dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt,
    nc <- ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
  end <- Sys.time()
  invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
invisible(dbExecute(con, "DROP TABLE IF EXISTS temp1"))
invisible(dbExecute(con, "DROP TABLE IF EXISTS temp2"))
BenchmarkResults[40, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Union.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()


## 100M 2N 1D 0G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Union.csv"))
rts <- c(rep(1.1, 10))
dbExecute(con, "CREATE TABLE temp1 AS SELECT Date, DailyLiters, DailyUnits, DailyMargin FROM bmdata100M")
dbExecute(con, "CREATE TABLE temp2 AS SELECT Date, DailyLiters, DailyUnits, DailyMargin FROM bmdata100M_2")
for(i in 1:10) {
  print(i)
  start <- Sys.time()
  dbExecute(con, "CREATE TABLE ans AS
      SELECT * FROM temp1
      UNION ALL
      SELECT * FROM temp2")
  print(c(
    nr <- dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt,
    nc <- ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
  end <- Sys.time()
  invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
invisible(dbExecute(con, "DROP TABLE IF EXISTS temp1"))
invisible(dbExecute(con, "DROP TABLE IF EXISTS temp2"))
BenchmarkResults[41, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Union.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 100M 2N 1D 1G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Union.csv"))
rts <- c(rep(1.1, 10))
dbExecute(con, "CREATE TABLE temp1 AS SELECT Date, Customer, DailyLiters, DailyUnits, DailyMargin FROM bmdata100M")
dbExecute(con, "CREATE TABLE temp2 AS SELECT Date, Customer, DailyLiters, DailyUnits, DailyMargin FROM bmdata100M_2")
for(i in 1:10) {
  print(i)
  start <- Sys.time()
  dbExecute(con, "CREATE TABLE ans AS
      SELECT * FROM temp1
      UNION ALL
      SELECT * FROM temp2")
  print(c(
    nr <- dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt,
    nc <- ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
  end <- Sys.time()
  invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
invisible(dbExecute(con, "DROP TABLE IF EXISTS temp1"))
invisible(dbExecute(con, "DROP TABLE IF EXISTS temp2"))
BenchmarkResults[42, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Union.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 100M 2N 1D 2G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Union.csv"))
rts <- c(rep(1.1, 10))
dbExecute(con, "CREATE TABLE temp1 AS SELECT Date, Customer, Brand, DailyLiters, DailyUnits, DailyMargin FROM bmdata100M")
dbExecute(con, "CREATE TABLE temp2 AS SELECT Date, Customer, Brand, DailyLiters, DailyUnits, DailyMargin FROM bmdata100M_2")
for(i in 1:10) {
  print(i)
  start <- Sys.time()
  dbExecute(con, "CREATE TABLE ans AS
      SELECT * FROM temp1
      UNION ALL
      SELECT * FROM temp2")
  print(c(
    nr <- dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt,
    nc <- ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
  end <- Sys.time()
  invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
invisible(dbExecute(con, "DROP TABLE IF EXISTS temp1"))
invisible(dbExecute(con, "DROP TABLE IF EXISTS temp2"))
BenchmarkResults[43, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Union.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 100M 2N 1D 3G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Union.csv"))
rts <- c(rep(1.1, 10))
dbExecute(con, "CREATE TABLE temp1 AS SELECT Date, Customer, Brand, Category, DailyLiters, DailyUnits, DailyMargin FROM bmdata100M")
dbExecute(con, "CREATE TABLE temp2 AS SELECT Date, Customer, Brand, Category, DailyLiters, DailyUnits, DailyMargin FROM bmdata100M_2")
for(i in 1:10) {
  print(i)
  start <- Sys.time()
  dbExecute(con, "CREATE TABLE ans AS
      SELECT * FROM temp1
      UNION ALL
      SELECT * FROM temp2")
  print(c(
    nr <- dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt,
    nc <- ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
  end <- Sys.time()
  invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
invisible(dbExecute(con, "DROP TABLE IF EXISTS temp1"))
invisible(dbExecute(con, "DROP TABLE IF EXISTS temp2"))
BenchmarkResults[44, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Union.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 100M 2N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Union.csv"))
rts <- c(rep(1.1, 10))
dbExecute(con, "CREATE TABLE temp1 AS SELECT Date, Customer, Brand, Category, BeverageFlavor, DailyLiters, DailyUnits, DailyMargin FROM bmdata100M")
dbExecute(con, "CREATE TABLE temp2 AS SELECT Date, Customer, Brand, Category, BeverageFlavor, DailyLiters, DailyUnits, DailyMargin FROM bmdata100M_2")
for(i in 1:10) {
  print(i)
  start <- Sys.time()
  dbExecute(con, "CREATE TABLE ans AS
      SELECT * FROM temp1
      UNION ALL
      SELECT * FROM temp2")
  print(c(
    nr <- dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt,
    nc <- ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
  end <- Sys.time()
  invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
invisible(dbExecute(con, "DROP TABLE IF EXISTS temp1"))
invisible(dbExecute(con, "DROP TABLE IF EXISTS temp2"))
BenchmarkResults[45, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Union.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()


BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Union.csv"))
BenchmarkResults[16, TimeInSeconds := BenchmarkResults[1:15, sum(TimeInSeconds)]]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Union.csv"))



