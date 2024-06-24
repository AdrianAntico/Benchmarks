# Path to data storage
Path <- "C:/Users/Bizon/Documents/GitHub/rappwd/"

# Create results table
BenchmarkResults <- data.table::data.table(
  Framework = 'duckdb',
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

data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Melt.csv"))
rm(BenchmarkResults)

library(data.table)
library(duckdb)
library(DBI)
data <- data.table::fread(paste0(Path, "FakeBevData1M.csv"))
data.table::setnames(data, c("Beverage Flavor", "Daily Liters", "Daily Margin", "Daily Revenue", "Daily Units"), c("BeverageFlavor", "DailyLiters", "DailyMargin", "DailyRevenue", "DailyUnits"))
con = dbConnect(duckdb::duckdb())
ncores = parallel::detectCores()
invisible(dbExecute(con, sprintf("PRAGMA THREADS=%d", ncores)))
invisible(dbExecute(con, sprintf("SET THREADS=%d", ncores)))
table_name <- "bmdata1M"
dbWriteTable(con, "bmdata1M", data, overwrite = TRUE)
rm(data)

# Query the schema of the table
query <- sprintf("PRAGMA table_info(%s)", table_name)
schema_info <- dbGetQuery(con, query)
rm(schema_info, ncores, query, table_name)



# Melt 1M

# Melt Numeric Variable:

## 1M 2N 1D 0G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Melt.csv"))
rts <- c(rep(1.1, 3))
for(i in 1:3) {
  start <- Sys.time()
  dbExecute(con, "CREATE TABLE ans AS
      SELECT DATE, DailyLiters as variable from bmdata1M
      UNION ALL
      SELECT DATE, DailyUnits as variable from bmdata1M")
  print(c(
    nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt,
    nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
  end <- Sys.time()
  invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[1, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Melt.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 1M 2N 1D 1G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Melt.csv"))
rts <- c(rep(1.1, 3))
for(i in 1:3) {
  start <- Sys.time()
  dbExecute(con, "CREATE TABLE ans AS
      SELECT DATE, Customer, DailyLiters as variable from bmdata1M
      UNION ALL
      SELECT DATE, Customer, DailyUnits as variable from bmdata1M")
  print(c(
    nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt,
    nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
  end <- Sys.time()
  invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[2, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Melt.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 1M 2N 1D 2G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Melt.csv"))
rts <- c(rep(1.1, 3))
for(i in 1:3) {
  start <- Sys.time()
  dbExecute(con, "CREATE TABLE ans AS
      SELECT DATE, Customer, Brand, DailyLiters as variable from bmdata1M
      UNION ALL
      SELECT DATE, Customer, Brand, DailyUnits as variable from bmdata1M")
  print(c(
    nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt,
    nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
  end <- Sys.time()
  invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[3, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Melt.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 1M 2N 1D 3G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Melt.csv"))
rts <- c(rep(1.1, 3))
for(i in 1:3) {
  start <- Sys.time()
  dbExecute(con, "CREATE TABLE ans AS
      SELECT DATE, Customer, Brand, Category, DailyLiters as variable from bmdata1M
      UNION ALL
      SELECT DATE, Customer, Brand, Category, DailyUnits as variable from bmdata1M")
  print(c(
    nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt,
    nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
  end <- Sys.time()
  invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[4, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Melt.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 1M 2N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Melt.csv"))
rts <- c(rep(1.1, 3))
for(i in 1:3) {
  start <- Sys.time()
  dbExecute(con, "CREATE TABLE ans AS
      SELECT DATE, Customer, Brand, Category, BeverageFlavor, DailyLiters as variable from bmdata1M
      UNION ALL
      SELECT DATE, Customer, Brand, Category, BeverageFlavor, DailyUnits as variable from bmdata1M")
  print(c(
    nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt,
    nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
  end <- Sys.time()
  invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[5, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Melt.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 1M 3N 1D 0G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Melt.csv"))
rts <- c(rep(1.1, 3))
for(i in 1:3) {
  start <- Sys.time()
  dbExecute(con, "CREATE TABLE ans AS
      SELECT DATE, DailyLiters as variable from bmdata1M
      UNION ALL
      SELECT DATE, DailyUnits as variable from bmdata1M
      UNION ALL
      SELECT DATE, DailyMargin as variable from bmdata1M")
  print(c(
    nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt,
    nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
  end <- Sys.time()
  invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[6, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Melt.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 1M 3N 1D 1G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Melt.csv"))
rts <- c(rep(1.1, 3))
for(i in 1:3) {
  start <- Sys.time()
  dbExecute(con, "CREATE TABLE ans AS
      SELECT DATE, Customer, DailyLiters as variable from bmdata1M
      UNION ALL
      SELECT DATE, Customer, DailyUnits as variable from bmdata1M
      UNION ALL
      SELECT DATE, Customer, DailyMargin as variable from bmdata1M")
  print(c(
    nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt,
    nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
  end <- Sys.time()
  invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[7, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Melt.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 1M 3N 1D 2G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Melt.csv"))
rts <- c(rep(1.1, 3))
for(i in 1:3) {
  start <- Sys.time()
  dbExecute(con, "CREATE TABLE ans AS
      SELECT DATE, Customer, Brand, DailyLiters as variable from bmdata1M
      UNION ALL
      SELECT DATE, Customer, Brand, DailyUnits as variable from bmdata1M
      UNION ALL
      SELECT DATE, Customer, Brand, DailyMargin as variable from bmdata1M")
  print(c(
    nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt,
    nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
  end <- Sys.time()
  invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[8, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Melt.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 1M 3N 1D 3G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Melt.csv"))
rts <- c(rep(1.1, 3))
for(i in 1:3) {
  start <- Sys.time()
  dbExecute(con, "CREATE TABLE ans AS
      SELECT DATE, Customer, Brand, Category, DailyLiters as variable from bmdata1M
      UNION ALL
      SELECT DATE, Customer, Brand, Category, DailyUnits as variable from bmdata1M
      UNION ALL
      SELECT DATE, Customer, Brand, Category, DailyMargin as variable from bmdata1M")
  print(c(
    nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt,
    nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
  end <- Sys.time()
  invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[9, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Melt.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 1M 3N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Melt.csv"))
rts <- c(rep(1.1, 3))
for(i in 1:3) {
  start <- Sys.time()
  dbExecute(con, "CREATE TABLE ans AS
      SELECT DATE, Customer, Brand, Category, BeverageFlavor, DailyLiters as variable from bmdata1M
      UNION ALL
      SELECT DATE, Customer, Brand, Category, BeverageFlavor, DailyUnits as variable from bmdata1M
      UNION ALL
      SELECT DATE, Customer, Brand, Category, BeverageFlavor, DailyMargin as variable from bmdata1M")
  print(c(
    nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt,
    nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
  end <- Sys.time()
  invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[10, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Melt.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 1M 4N 1D 0G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Melt.csv"))
rts <- c(rep(1.1, 3))
for(i in 1:3) {
  start <- Sys.time()
  dbExecute(con, "CREATE TABLE ans AS
      SELECT DATE, DailyLiters as variable from bmdata1M
      UNION ALL
      SELECT DATE, DailyUnits as variable from bmdata1M
      UNION ALL
      SELECT DATE, DailyMargin as variable from bmdata1M
      UNION ALL
      SELECT DATE, DailyRevenue as variable from bmdata1M")
  print(c(
    nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt,
    nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
  end <- Sys.time()
  invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))
  end <- Sys.time()
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[11, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Melt.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 1M 4N 1D 1G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Melt.csv"))
rts <- c(rep(1.1, 3))
for(i in 1:3) {
  start <- Sys.time()
  dbExecute(con, "CREATE TABLE ans AS
      SELECT DATE, Customer, DailyLiters as variable from bmdata1M
      UNION ALL
      SELECT DATE, Customer, DailyUnits as variable from bmdata1M
      UNION ALL
      SELECT DATE, Customer, DailyMargin as variable from bmdata1M
      UNION ALL
      SELECT DATE, Customer, DailyRevenue as variable from bmdata1M")
  print(c(
    nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt,
    nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
  end <- Sys.time()
  invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[12, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Melt.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 1M 4N 1D 2G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Melt.csv"))
rts <- c(rep(1.1, 3))
for(i in 1:3) {
  start <- Sys.time()
  dbExecute(con, "CREATE TABLE ans AS
      SELECT DATE, Customer, Brand, DailyLiters as variable from bmdata1M
      UNION ALL
      SELECT DATE, Customer, Brand, DailyUnits as variable from bmdata1M
      UNION ALL
      SELECT DATE, Customer, Brand, DailyMargin as variable from bmdata1M
      UNION ALL
      SELECT DATE, Customer, Brand, DailyRevenue as variable from bmdata1M")
  print(c(
    nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt,
    nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
  end <- Sys.time()
  invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[13, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Melt.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 1M 4N 1D 3G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Melt.csv"))
rts <- c(rep(1.1, 3))
for(i in 1:3) {
  start <- Sys.time()
  dbExecute(con, "CREATE TABLE ans AS
      SELECT DATE, Customer, Brand, Category, DailyLiters as variable from bmdata1M
      UNION ALL
      SELECT DATE, Customer, Brand, Category, DailyUnits as variable from bmdata1M
      UNION ALL
      SELECT DATE, Customer, Brand, Category, DailyMargin as variable from bmdata1M
      UNION ALL
      SELECT DATE, Customer, Brand, Category, DailyRevenue as variable from bmdata1M")
  print(c(
    nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt,
    nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
  end <- Sys.time()
  invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[14, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Melt.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 1M 4N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Melt.csv"))
rts <- c(rep(1.1, 3))
for(i in 1:3) {
  start <- Sys.time()
  dbExecute(con, "CREATE TABLE ans AS
      SELECT DATE, Customer, Brand, Category, BeverageFlavor, DailyLiters as variable from bmdata1M
      UNION ALL
      SELECT DATE, Customer, Brand, Category, BeverageFlavor, DailyUnits as variable from bmdata1M
      UNION ALL
      SELECT DATE, Customer, Brand, Category, BeverageFlavor, DailyMargin as variable from bmdata1M
      UNION ALL
      SELECT DATE, Customer, Brand, Category, BeverageFlavor, DailyRevenue as variable from bmdata1M")
  print(c(
    nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt,
    nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
  end <- Sys.time()
  invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[15, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Melt.csv"))
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
data.table::setnames(data, c("Beverage Flavor", "Daily Liters", "Daily Margin", "Daily Revenue", "Daily Units"), c("BeverageFlavor", "DailyLiters", "DailyMargin", "DailyRevenue", "DailyUnits"))
con = dbConnect(duckdb::duckdb())
ncores = parallel::detectCores()
invisible(dbExecute(con, sprintf("PRAGMA THREADS=%d", ncores)))
invisible(dbExecute(con, sprintf("SET THREADS=%d", ncores)))
table_name <- "bmdata10M"
dbWriteTable(con, "bmdata10M", data, overwrite = TRUE)
rm(data)
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Melt.csv"))
rts <- c(rep(1.1, 3))
for(i in 1:3) {
  start <- Sys.time()
  dbExecute(con, "CREATE TABLE ans AS
      SELECT DATE, DailyLiters as variable from bmdata10M
      UNION ALL
      SELECT DATE, DailyUnits as variable from bmdata10M")
  print(c(
    nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt,
    nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
  end <- Sys.time()
  invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[16, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Melt.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 10M 2N 1D 1G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Melt.csv"))
rts <- c(rep(1.1, 3))
for(i in 1:3) {
  start <- Sys.time()
  dbExecute(con, "CREATE TABLE ans AS
      SELECT DATE, Customer, DailyLiters as variable from bmdata10M
      UNION ALL
      SELECT DATE, Customer, DailyUnits as variable from bmdata10M")
  print(c(
    nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt,
    nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
  end <- Sys.time()
  invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[17, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Melt.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 10M 2N 1D 2G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Melt.csv"))
rts <- c(rep(1.1, 3))
for(i in 1:3) {
  start <- Sys.time()
  dbExecute(con, "CREATE TABLE ans AS
      SELECT DATE, Customer, Brand, DailyLiters as variable from bmdata10M
      UNION ALL
      SELECT DATE, Customer, Brand, DailyUnits as variable from bmdata10M")
  print(c(
    nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt,
    nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
  end <- Sys.time()
  invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[18, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Melt.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 10M 2N 1D 3G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Melt.csv"))
rts <- c(rep(1.1, 3))
for(i in 1:3) {
  start <- Sys.time()
  dbExecute(con, "CREATE TABLE ans AS
      SELECT DATE, Customer, Brand, Category, DailyLiters as variable from bmdata10M
      UNION ALL
      SELECT DATE, Customer, Brand, Category, DailyUnits as variable from bmdata10M")
  print(c(
    nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt,
    nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
  end <- Sys.time()
  invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[19, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Melt.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 10M 2N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Melt.csv"))
rts <- c(rep(1.1, 3))
for(i in 1:3) {
  start <- Sys.time()
  dbExecute(con, "CREATE TABLE ans AS
      SELECT DATE, Customer, Brand, Category, BeverageFlavor, DailyLiters as variable from bmdata10M
      UNION ALL
      SELECT DATE, Customer, Brand, Category, BeverageFlavor, DailyUnits as variable from bmdata10M")
  print(c(
    nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt,
    nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
  end <- Sys.time()
  invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[20, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Melt.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 10M 3N 1D 0G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Melt.csv"))
rts <- c(rep(1.1, 3))
for(i in 1:3) {
  start <- Sys.time()
  dbExecute(con, "CREATE TABLE ans AS
      SELECT DATE, DailyLiters as variable from bmdata10M
      UNION ALL
      SELECT DATE, DailyUnits as variable from bmdata10M
      UNION ALL
      SELECT DATE, DailyMargin as variable from bmdata10M")
  print(c(
    nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt,
    nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
  end <- Sys.time()
  invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[21, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Melt.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 10M 3N 1D 1G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Melt.csv"))
rts <- c(rep(1.1, 3))
for(i in 1:3) {
  start <- Sys.time()
  dbExecute(con, "CREATE TABLE ans AS
      SELECT DATE, Customer, DailyLiters as variable from bmdata10M
      UNION ALL
      SELECT DATE, Customer, DailyUnits as variable from bmdata10M
      UNION ALL
      SELECT DATE, Customer, DailyMargin as variable from bmdata10M")
  print(c(
    nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt,
    nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
  end <- Sys.time()
  invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[22, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Melt.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 10M 3N 1D 2G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Melt.csv"))
rts <- c(rep(1.1, 3))
for(i in 1:3) {
  start <- Sys.time()
  dbExecute(con, "CREATE TABLE ans AS
      SELECT DATE, Customer, Brand, DailyLiters as variable from bmdata10M
      UNION ALL
      SELECT DATE, Customer, Brand, DailyUnits as variable from bmdata10M
      UNION ALL
      SELECT DATE, Customer, Brand, DailyMargin as variable from bmdata10M")
  print(c(
    nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt,
    nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
  end <- Sys.time()
  invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[23, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Melt.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 10M 3N 1D 3G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Melt.csv"))
rts <- c(rep(1.1, 3))
for(i in 1:3) {
  start <- Sys.time()
  dbExecute(con, "CREATE TABLE ans AS
      SELECT DATE, Customer, Brand, Category, DailyLiters as variable from bmdata10M
      UNION ALL
      SELECT DATE, Customer, Brand, Category, DailyUnits as variable from bmdata10M
      UNION ALL
      SELECT DATE, Customer, Brand, Category, DailyMargin as variable from bmdata10M")
  print(c(
    nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt,
    nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
  end <- Sys.time()
  invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[24, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Melt.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 10M 3N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Melt.csv"))
rts <- c(rep(1.1, 3))
for(i in 1:3) {
  start <- Sys.time()
  dbExecute(con, "CREATE TABLE ans AS
      SELECT DATE, Customer, Brand, Category, BeverageFlavor, DailyLiters as variable from bmdata10M
      UNION ALL
      SELECT DATE, Customer, Brand, Category, BeverageFlavor, DailyUnits as variable from bmdata10M
      UNION ALL
      SELECT DATE, Customer, Brand, Category, BeverageFlavor, DailyMargin as variable from bmdata10M")
  print(c(
    nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt,
    nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
  end <- Sys.time()
  invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[25, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Melt.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 10M 4N 1D 0G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Melt.csv"))
rts <- c(rep(1.1, 3))
for(i in 1:3) {
  start <- Sys.time()
  dbExecute(con, "CREATE TABLE ans AS
      SELECT DATE, DailyLiters as variable from bmdata10M
      UNION ALL
      SELECT DATE, DailyUnits as variable from bmdata10M
      UNION ALL
      SELECT DATE, DailyMargin as variable from bmdata10M
      UNION ALL
      SELECT DATE, DailyRevenue as variable from bmdata10M")
  print(c(
    nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt,
    nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
  end <- Sys.time()
  invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))
  end <- Sys.time()
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[26, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Melt.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 10M 4N 1D 1G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Melt.csv"))
rts <- c(rep(1.1, 3))
for(i in 1:3) {
  start <- Sys.time()
  dbExecute(con, "CREATE TABLE ans AS
      SELECT DATE, Customer, DailyLiters as variable from bmdata10M
      UNION ALL
      SELECT DATE, Customer, DailyUnits as variable from bmdata10M
      UNION ALL
      SELECT DATE, Customer, DailyMargin as variable from bmdata10M
      UNION ALL
      SELECT DATE, Customer, DailyRevenue as variable from bmdata10M")
  print(c(
    nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt,
    nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
  end <- Sys.time()
  invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[27, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Melt.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 10M 4N 1D 2G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Melt.csv"))
rts <- c(rep(1.1, 3))
for(i in 1:3) {
  start <- Sys.time()
  dbExecute(con, "CREATE TABLE ans AS
      SELECT DATE, Customer, Brand, DailyLiters as variable from bmdata10M
      UNION ALL
      SELECT DATE, Customer, Brand, DailyUnits as variable from bmdata10M
      UNION ALL
      SELECT DATE, Customer, Brand, DailyMargin as variable from bmdata10M
      UNION ALL
      SELECT DATE, Customer, Brand, DailyRevenue as variable from bmdata10M")
  print(c(
    nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt,
    nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
  end <- Sys.time()
  invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[28, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Melt.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 10M 4N 1D 3G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Melt.csv"))
rts <- c(rep(1.1, 3))
for(i in 1:3) {
  start <- Sys.time()
  dbExecute(con, "CREATE TABLE ans AS
      SELECT DATE, Customer, Brand, Category, DailyLiters as variable from bmdata10M
      UNION ALL
      SELECT DATE, Customer, Brand, Category, DailyUnits as variable from bmdata10M
      UNION ALL
      SELECT DATE, Customer, Brand, Category, DailyMargin as variable from bmdata10M
      UNION ALL
      SELECT DATE, Customer, Brand, Category, DailyRevenue as variable from bmdata10M")
  print(c(
    nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt,
    nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
  end <- Sys.time()
  invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[29, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Melt.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 10M 4N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Melt.csv"))
rts <- c(rep(1.1, 3))
for(i in 1:3) {
  start <- Sys.time()
  dbExecute(con, "CREATE TABLE ans AS
      SELECT DATE, Customer, Brand, Category, BeverageFlavor, DailyLiters as variable from bmdata10M
      UNION ALL
      SELECT DATE, Customer, Brand, Category, BeverageFlavor, DailyUnits as variable from bmdata10M
      UNION ALL
      SELECT DATE, Customer, Brand, Category, BeverageFlavor, DailyMargin as variable from bmdata10M
      UNION ALL
      SELECT DATE, Customer, Brand, Category, BeverageFlavor, DailyRevenue as variable from bmdata10M")
  print(c(
    nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt,
    nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
  end <- Sys.time()
  invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[30, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Melt.csv"))
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
data.table::setnames(data, c("Beverage Flavor", "Daily Liters", "Daily Margin", "Daily Revenue", "Daily Units"), c("BeverageFlavor", "DailyLiters", "DailyMargin", "DailyRevenue", "DailyUnits"))
con = dbConnect(duckdb::duckdb())
ncores = parallel::detectCores()
invisible(dbExecute(con, sprintf("PRAGMA THREADS=%d", ncores)))
invisible(dbExecute(con, sprintf("SET THREADS=%d", ncores)))
table_name <- "bmdata100M"
dbWriteTable(con, "bmdata100M", data, overwrite = TRUE)
rm(data)
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Melt.csv"))
rts <- c(rep(1.1, 3))
for(i in 1:3) {
  start <- Sys.time()
  dbExecute(con, "CREATE TABLE ans AS
      SELECT DATE, DailyLiters as variable from bmdata100M
      UNION ALL
      SELECT DATE, DailyUnits as variable from bmdata100M")
  print(c(
    nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt,
    nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
  end <- Sys.time()
  invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[31, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Melt.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 100M 2N 1D 1G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Melt.csv"))
rts <- c(rep(1.1, 3))
for(i in 1:3) {
  start <- Sys.time()
  dbExecute(con, "CREATE TABLE ans AS
      SELECT DATE, Customer, DailyLiters as variable from bmdata100M
      UNION ALL
      SELECT DATE, Customer, DailyUnits as variable from bmdata100M")
  print(c(
    nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt,
    nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
  end <- Sys.time()
  invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[32, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Melt.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 100M 2N 1D 2G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Melt.csv"))
rts <- c(rep(1.1, 3))
for(i in 1:3) {
  start <- Sys.time()
  dbExecute(con, "CREATE TABLE ans AS
      SELECT DATE, Customer, Brand, DailyLiters as variable from bmdata100M
      UNION ALL
      SELECT DATE, Customer, Brand, DailyUnits as variable from bmdata100M")
  print(c(
    nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt,
    nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
  end <- Sys.time()
  invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[33, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Melt.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 100M 2N 1D 3G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Melt.csv"))
rts <- c(rep(1.1, 3))
for(i in 1:3) {
  start <- Sys.time()
  dbExecute(con, "CREATE TABLE ans AS
      SELECT DATE, Customer, Brand, Category, DailyLiters as variable from bmdata100M
      UNION ALL
      SELECT DATE, Customer, Brand, Category, DailyUnits as variable from bmdata100M")
  print(c(
    nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt,
    nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
  end <- Sys.time()
  invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[34, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Melt.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 100M 2N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Melt.csv"))
rts <- c(rep(1.1, 3))
for(i in 1:3) {
  start <- Sys.time()
  dbExecute(con, "CREATE TABLE ans AS
      SELECT DATE, Customer, Brand, Category, BeverageFlavor, DailyLiters as variable from bmdata100M
      UNION ALL
      SELECT DATE, Customer, Brand, Category, BeverageFlavor, DailyUnits as variable from bmdata100M")
  print(c(
    nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt,
    nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
  end <- Sys.time()
  invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[35, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Melt.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 100M 3N 1D 0G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Melt.csv"))
rts <- c(rep(1.1, 3))
for(i in 1:3) {
  start <- Sys.time()
  dbExecute(con, "CREATE TABLE ans AS
      SELECT DATE, DailyLiters as variable from bmdata100M
      UNION ALL
      SELECT DATE, DailyUnits as variable from bmdata100M
      UNION ALL
      SELECT DATE, DailyMargin as variable from bmdata100M")
  print(c(
    nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt,
    nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
  end <- Sys.time()
  invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[36, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Melt.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 100M 3N 1D 1G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Melt.csv"))
rts <- c(rep(1.1, 3))
for(i in 1:3) {
  start <- Sys.time()
  dbExecute(con, "CREATE TABLE ans AS
      SELECT DATE, Customer, DailyLiters as variable from bmdata100M
      UNION ALL
      SELECT DATE, Customer, DailyUnits as variable from bmdata100M
      UNION ALL
      SELECT DATE, Customer, DailyMargin as variable from bmdata100M")
  print(c(
    nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt,
    nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
  end <- Sys.time()
  invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[37, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Melt.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 100M 3N 1D 2G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Melt.csv"))
rts <- c(rep(1.1, 3))
for(i in 1:3) {
  start <- Sys.time()
  dbExecute(con, "CREATE TABLE ans AS
      SELECT DATE, Customer, Brand, DailyLiters as variable from bmdata100M
      UNION ALL
      SELECT DATE, Customer, Brand, DailyUnits as variable from bmdata100M
      UNION ALL
      SELECT DATE, Customer, Brand, DailyMargin as variable from bmdata100M")
  print(c(
    nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt,
    nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
  end <- Sys.time()
  invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[38, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Melt.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 100M 3N 1D 3G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Melt.csv"))
rts <- c(rep(1.1, 3))
for(i in 1:3) {
  start <- Sys.time()
  dbExecute(con, "CREATE TABLE ans AS
      SELECT DATE, Customer, Brand, Category, DailyLiters as variable from bmdata100M
      UNION ALL
      SELECT DATE, Customer, Brand, Category, DailyUnits as variable from bmdata100M
      UNION ALL
      SELECT DATE, Customer, Brand, Category, DailyMargin as variable from bmdata100M")
  print(c(
    nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt,
    nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
  end <- Sys.time()
  invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[39, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Melt.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 100M 3N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Melt.csv"))
rts <- c(rep(1.1, 3))
for(i in 1:3) {
  start <- Sys.time()
  dbExecute(con, "CREATE TABLE ans AS
      SELECT DATE, Customer, Brand, Category, BeverageFlavor, DailyLiters as variable from bmdata100M
      UNION ALL
      SELECT DATE, Customer, Brand, Category, BeverageFlavor, DailyUnits as variable from bmdata100M
      UNION ALL
      SELECT DATE, Customer, Brand, Category, BeverageFlavor, DailyMargin as variable from bmdata100M")
  print(c(
    nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt,
    nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
  end <- Sys.time()
  invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[40, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Melt.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 100M 4N 1D 0G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Melt.csv"))
rts <- c(rep(1.1, 3))
for(i in 1:3) {
  start <- Sys.time()
  dbExecute(con, "CREATE TABLE ans AS
      SELECT DATE, DailyLiters as variable from bmdata100M
      UNION ALL
      SELECT DATE, DailyUnits as variable from bmdata100M
      UNION ALL
      SELECT DATE, DailyMargin as variable from bmdata100M
      UNION ALL
      SELECT DATE, DailyRevenue as variable from bmdata100M")
  print(c(
    nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt,
    nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
  end <- Sys.time()
  invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))
  end <- Sys.time()
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[41, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Melt.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 100M 4N 1D 1G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Melt.csv"))
rts <- c(rep(1.1, 3))
for(i in 1:3) {
  start <- Sys.time()
  dbExecute(con, "CREATE TABLE ans AS
      SELECT DATE, Customer, DailyLiters as variable from bmdata100M
      UNION ALL
      SELECT DATE, Customer, DailyUnits as variable from bmdata100M
      UNION ALL
      SELECT DATE, Customer, DailyMargin as variable from bmdata100M
      UNION ALL
      SELECT DATE, Customer, DailyRevenue as variable from bmdata100M")
  print(c(
    nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt,
    nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
  end <- Sys.time()
  invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[42, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Melt.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 100M 4N 1D 2G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Melt.csv"))
rts <- c(rep(1.1, 3))
for(i in 1:3) {
  start <- Sys.time()
  dbExecute(con, "CREATE TABLE ans AS
      SELECT DATE, Customer, Brand, DailyLiters as variable from bmdata100M
      UNION ALL
      SELECT DATE, Customer, Brand, DailyUnits as variable from bmdata100M
      UNION ALL
      SELECT DATE, Customer, Brand, DailyMargin as variable from bmdata100M
      UNION ALL
      SELECT DATE, Customer, Brand, DailyRevenue as variable from bmdata100M")
  print(c(
    nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt,
    nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
  end <- Sys.time()
  invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[43, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Melt.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 100M 4N 1D 3G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Melt.csv"))
rts <- c(rep(1.1, 3))
for(i in 1:3) {
  start <- Sys.time()
  dbExecute(con, "CREATE TABLE ans AS
      SELECT DATE, Customer, Brand, Category, DailyLiters as variable from bmdata100M
      UNION ALL
      SELECT DATE, Customer, Brand, Category, DailyUnits as variable from bmdata100M
      UNION ALL
      SELECT DATE, Customer, Brand, Category, DailyMargin as variable from bmdata100M
      UNION ALL
      SELECT DATE, Customer, Brand, Category, DailyRevenue as variable from bmdata100M")
  print(c(
    nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt,
    nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
  end <- Sys.time()
  invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[44, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Melt.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()

## 100M 4N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Melt.csv"))
rts <- c(rep(1.1, 3))
for(i in 1:3) {
  start <- Sys.time()
  dbExecute(con, "CREATE TABLE ans AS
      SELECT DATE, Customer, Brand, Category, BeverageFlavor, DailyLiters as variable from bmdata100M
      UNION ALL
      SELECT DATE, Customer, Brand, Category, BeverageFlavor, DailyUnits as variable from bmdata100M
      UNION ALL
      SELECT DATE, Customer, Brand, Category, BeverageFlavor, DailyMargin as variable from bmdata100M
      UNION ALL
      SELECT DATE, Customer, Brand, Category, BeverageFlavor, DailyRevenue as variable from bmdata100M")
  print(c(
    nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt,
    nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
  end <- Sys.time()
  invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[45, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Melt.csv"))
rm(list = c("BenchmarkResults","data","end","start"))
gc()


BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Melt.csv"))
BenchmarkResults[46, TimeInSeconds := BenchmarkResults[1:45, sum(TimeInSeconds)]]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Melt.csv"))
