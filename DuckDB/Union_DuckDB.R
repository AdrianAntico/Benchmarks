# Path to data storage
Path <- "C:/Users/Bizon/Documents/GitHub/rappwd/"

# Create results table
BenchmarkResults <- data.table::data.table(
  Framework = 'duckdb',
  Method = 'UNION',
  Experiment = c(
    "1M 3N 1D 4G",
    "10M 3N 1D 4G",
    "100M 3N 1D 4G"
  ),

  TimeInSeconds = c(rep(-0.1, 3))
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

## 1M 2N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Union.csv"))
rts <- c(rep(1.1, 3))
dbExecute(con, "CREATE TABLE temp1 AS SELECT Date, Customer, Brand, Category, BeverageFlavor, DailyLiters, DailyUnits, DailyMargin FROM bmdata1M")
dbExecute(con, "CREATE TABLE temp2 AS SELECT Date, Customer, Brand, Category, BeverageFlavor, DailyLiters, DailyUnits, DailyMargin FROM bmdata1M_2")
for(i in 1:3) {
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
BenchmarkResults[1, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Union.csv"))
rm(list = c("BenchmarkResults","end","start"))



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


## 10M 2N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Union.csv"))
rts <- c(rep(1.1, 3))
dbExecute(con, "CREATE TABLE temp1 AS SELECT Date, Customer, Brand, Category, BeverageFlavor, DailyLiters, DailyUnits, DailyMargin FROM bmdata10M")
dbExecute(con, "CREATE TABLE temp2 AS SELECT Date, Customer, Brand, Category, BeverageFlavor, DailyLiters, DailyUnits, DailyMargin FROM bmdata10M_2")
for(i in 1:3) {
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
BenchmarkResults[2, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Union.csv"))
rm(list = c("BenchmarkResults","end","start"))



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


## 100M 2N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Union.csv"))
rts <- c(rep(1.1, 3))
dbExecute(con, "CREATE TABLE temp1 AS SELECT Date, Customer, Brand, Category, BeverageFlavor, DailyLiters, DailyUnits, DailyMargin FROM bmdata100M")
dbExecute(con, "CREATE TABLE temp2 AS SELECT Date, Customer, Brand, Category, BeverageFlavor, DailyLiters, DailyUnits, DailyMargin FROM bmdata100M_2")
for(i in 1:3) {
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
BenchmarkResults[3, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Union.csv"))
rm(list = c("BenchmarkResults","end","start"))
