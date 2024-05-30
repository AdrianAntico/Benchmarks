# Path to data storage
Path <- "C:/Users/Bizon/Documents/GitHub/rappwd/"

# Create Results table
BenchmarkResults <- data.table::data.table(
  Framework = 'duckdb',
  Method = 'lags',
  Experiment = c(
    "1M 1N 1D 0G 5L",
    "1M 1N 1D 1G 5L",
    "1M 1N 1D 2G 5L",
    "1M 1N 1D 3G 5L",
    "1M 1N 1D 4G 5L",
    "1M 2N 1D 0G 5L",
    "1M 2N 1D 1G 5L",
    "1M 2N 1D 2G 5L",
    "1M 2N 1D 3G 5L",
    "1M 2N 1D 4G 5L",
    "1M 3N 1D 0G 5L",
    "1M 3N 1D 1G 5L",
    "1M 3N 1D 2G 5L",
    "1M 3N 1D 3G 5L",
    "1M 3N 1D 4G 5L",

    "10M 1N 1D 0G 5L",
    "10M 1N 1D 1G 5L",
    "10M 1N 1D 2G 5L",
    "10M 1N 1D 3G 5L",
    "10M 1N 1D 4G 5L",
    "10M 2N 1D 0G 5L",
    "10M 2N 1D 1G 5L",
    "10M 2N 1D 2G 5L",
    "10M 2N 1D 3G 5L",
    "10M 2N 1D 4G 5L",
    "10M 3N 1D 0G 5L",
    "10M 3N 1D 1G 5L",
    "10M 3N 1D 2G 5L",
    "10M 3N 1D 3G 5L",
    "10M 3N 1D 4G 5L",

    "100M 1N 1D 0G 5L",
    "100M 1N 1D 1G 5L",
    "100M 1N 1D 2G 5L",
    "100M 1N 1D 3G 5L",
    "100M 1N 1D 4G 5L",
    "100M 2N 1D 0G 5L",
    "100M 2N 1D 1G 5L",
    "100M 2N 1D 2G 5L",
    "100M 2N 1D 3G 5L",
    "100M 2N 1D 4G 5L",
    "100M 3N 1D 0G 5L",
    "100M 3N 1D 1G 5L",
    "100M 3N 1D 2G 5L",
    "100M 3N 1D 3G 5L",
    "100M 3N 1D 4G 5L",

    "Total Runtime"),

  TimeInSeconds = c(rep(-0.1, 46))
)

# Save results table
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Lags.csv"))
rm(BenchmarkResults)

# Environment setup
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


# Aggregation 1M

# Sum 1 Numeric Variable:

## 1M 1N 1D 0G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Lags.csv"))
start <- Sys.time()
dbExecute(
  con,
  "CREATE TABLE ans AS
  SELECT Date, Customer, Brand, Category, BeverageFlavor, DailyLiters, DailyUnits, DailyMargin, DailyRevenue,
  LAG(DailyLiters, 1) OVER() AS LagDailyLiters1,
  LAG(DailyLiters, 2) OVER() AS LagDailyLiters2,
  LAG(DailyLiters, 3) OVER() AS LagDailyLiters3,
  LAG(DailyLiters, 4) OVER() AS LagDailyLiters4,
  LAG(DailyLiters, 5) OVER() AS LagDailyLiters5
  FROM bmdata1M")
print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
end <- Sys.time()
dbExecute(con, "DROP TABLE ans")
BenchmarkResults[1, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Lags.csv"))
rm(list = c("BenchmarkResults","end","start"))


## 1M 1N 1D 1G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Lags.csv"))
start <- Sys.time()
dbExecute(
  con,
  "CREATE TABLE ans AS
  SELECT Date, Customer, Brand, Category, BeverageFlavor, DailyLiters, DailyUnits, DailyMargin, DailyRevenue,
  LAG(DailyLiters, 1) OVER(PARTITION BY Customer) AS LagDailyLiters1,
  LAG(DailyLiters, 2) OVER(PARTITION BY Customer) AS LagDailyLiters2,
  LAG(DailyLiters, 3) OVER(PARTITION BY Customer) AS LagDailyLiters3,
  LAG(DailyLiters, 4) OVER(PARTITION BY Customer) AS LagDailyLiters4,
  LAG(DailyLiters, 5) OVER(PARTITION BY Customer) AS LagDailyLiters5
  FROM bmdata1M")
print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
end <- Sys.time()
dbExecute(con, "DROP TABLE ans")
BenchmarkResults[2, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Lags.csv"))
rm(list = c("BenchmarkResults","end","start"))


## 1M 1N 1D 2G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Lags.csv"))
start <- Sys.time()
dbExecute(
  con,
  "CREATE TABLE ans AS
  SELECT Date, Customer, Brand, Category, BeverageFlavor, DailyLiters, DailyUnits, DailyMargin, DailyRevenue,
  LAG(DailyLiters, 1) OVER(PARTITION BY Customer,Brand) AS LagDailyLiters1,
  LAG(DailyLiters, 2) OVER(PARTITION BY Customer,Brand) AS LagDailyLiters2,
  LAG(DailyLiters, 3) OVER(PARTITION BY Customer,Brand) AS LagDailyLiters3,
  LAG(DailyLiters, 4) OVER(PARTITION BY Customer,Brand) AS LagDailyLiters4,
  LAG(DailyLiters, 5) OVER(PARTITION BY Customer,Brand) AS LagDailyLiters5
  FROM bmdata1M")
print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
end <- Sys.time()
dbExecute(con, "DROP TABLE ans")
BenchmarkResults[3, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Lags.csv"))
rm(list = c("BenchmarkResults","end","start"))


## 1M 1N 1D 3G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Lags.csv"))
start <- Sys.time()
dbExecute(
  con,
  "CREATE TABLE ans AS
  SELECT Date, Customer, Brand, Category, BeverageFlavor, DailyLiters, DailyUnits, DailyMargin, DailyRevenue,
  LAG(DailyLiters, 1) OVER(PARTITION BY Customer,Brand,Category) AS LagDailyLiters1,
  LAG(DailyLiters, 2) OVER(PARTITION BY Customer,Brand,Category) AS LagDailyLiters2,
  LAG(DailyLiters, 3) OVER(PARTITION BY Customer,Brand,Category) AS LagDailyLiters3,
  LAG(DailyLiters, 4) OVER(PARTITION BY Customer,Brand,Category) AS LagDailyLiters4,
  LAG(DailyLiters, 5) OVER(PARTITION BY Customer,Brand,Category) AS LagDailyLiters5
  FROM bmdata1M")
print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
end <- Sys.time()
dbExecute(con, "DROP TABLE ans")
BenchmarkResults[4, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Lags.csv"))
rm(list = c("BenchmarkResults","end","start"))


## 1M 1N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Lags.csv"))
start <- Sys.time()
dbExecute(
  con,
  "CREATE TABLE ans AS
  SELECT Date, Customer, Brand, Category, BeverageFlavor, DailyLiters, DailyUnits, DailyMargin, DailyRevenue,
  LAG(DailyLiters, 1) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor) AS LagDailyLiters1,
  LAG(DailyLiters, 2) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor) AS LagDailyLiters2,
  LAG(DailyLiters, 3) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor) AS LagDailyLiters3,
  LAG(DailyLiters, 4) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor) AS LagDailyLiters4,
  LAG(DailyLiters, 5) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor) AS LagDailyLiters5
  FROM bmdata1M")
print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
end <- Sys.time()
dbExecute(con, "DROP TABLE ans")
BenchmarkResults[5, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Lags.csv"))
rm(list = c("BenchmarkResults","end","start"))


## 1M 2N 1D 0G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Lags.csv"))
start <- Sys.time()
dbExecute(
  con,
  "CREATE TABLE ans AS
  SELECT Date, Customer, Brand, Category, BeverageFlavor, DailyLiters, DailyUnits, DailyMargin, DailyRevenue,
  LAG(DailyLiters, 1) OVER() AS LagDailyLiters1,
  LAG(DailyLiters, 2) OVER() AS LagDailyLiters2,
  LAG(DailyLiters, 3) OVER() AS LagDailyLiters3,
  LAG(DailyLiters, 4) OVER() AS LagDailyLiters4,
  LAG(DailyLiters, 5) OVER() AS LagDailyLiters5,
  LAG(DailyUnits, 1) OVER() AS LagDailyUnits1,
  LAG(DailyUnits, 2) OVER() AS LagDailyUnits2,
  LAG(DailyUnits, 3) OVER() AS LagDailyUnits3,
  LAG(DailyUnits, 4) OVER() AS LagDailyUnits4,
  LAG(DailyUnits, 5) OVER() AS LagDailyUnits5
  FROM bmdata1M")
print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
end <- Sys.time()
dbExecute(con, "DROP TABLE ans")
BenchmarkResults[6, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Lags.csv"))
rm(list = c("BenchmarkResults","end","start"))


## 1M 2N 1D 1G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Lags.csv"))
start <- Sys.time()
dbExecute(
  con,
  "CREATE TABLE ans AS
  SELECT Date, Customer, Brand, Category, BeverageFlavor, DailyLiters, DailyUnits, DailyMargin, DailyRevenue,
  LAG(DailyLiters, 1) OVER(PARTITION BY Customer) AS LagDailyLiters1,
  LAG(DailyLiters, 2) OVER(PARTITION BY Customer) AS LagDailyLiters2,
  LAG(DailyLiters, 3) OVER(PARTITION BY Customer) AS LagDailyLiters3,
  LAG(DailyLiters, 4) OVER(PARTITION BY Customer) AS LagDailyLiters4,
  LAG(DailyLiters, 5) OVER(PARTITION BY Customer) AS LagDailyLiters5,
  LAG(DailyUnits, 1) OVER(PARTITION BY Customer) AS LagDailyUnits1,
  LAG(DailyUnits, 2) OVER(PARTITION BY Customer) AS LagDailyUnits2,
  LAG(DailyUnits, 3) OVER(PARTITION BY Customer) AS LagDailyUnits3,
  LAG(DailyUnits, 4) OVER(PARTITION BY Customer) AS LagDailyUnits4,
  LAG(DailyUnits, 5) OVER(PARTITION BY Customer) AS LagDailyUnits5
  FROM bmdata1M")
print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
end <- Sys.time()
dbExecute(con, "DROP TABLE ans")
BenchmarkResults[7, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Lags.csv"))
rm(list = c("BenchmarkResults","end","start"))


## 1M 2N 1D 2G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Lags.csv"))
start <- Sys.time()
dbExecute(
  con,
  "CREATE TABLE ans AS
  SELECT Date, Customer, Brand, Category, BeverageFlavor, DailyLiters, DailyUnits, DailyMargin, DailyRevenue,
  LAG(DailyLiters, 1) OVER(PARTITION BY Customer,Brand) AS LagDailyLiters1,
  LAG(DailyLiters, 2) OVER(PARTITION BY Customer,Brand) AS LagDailyLiters2,
  LAG(DailyLiters, 3) OVER(PARTITION BY Customer,Brand) AS LagDailyLiters3,
  LAG(DailyLiters, 4) OVER(PARTITION BY Customer,Brand) AS LagDailyLiters4,
  LAG(DailyLiters, 5) OVER(PARTITION BY Customer,Brand) AS LagDailyLiters5,
  LAG(DailyUnits, 1) OVER(PARTITION BY Customer,Brand) AS LagDailyUnits1,
  LAG(DailyUnits, 2) OVER(PARTITION BY Customer,Brand) AS LagDailyUnits2,
  LAG(DailyUnits, 3) OVER(PARTITION BY Customer,Brand) AS LagDailyUnits3,
  LAG(DailyUnits, 4) OVER(PARTITION BY Customer,Brand) AS LagDailyUnits4,
  LAG(DailyUnits, 5) OVER(PARTITION BY Customer,Brand) AS LagDailyUnits5
  FROM bmdata1M")
print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
end <- Sys.time()
dbExecute(con, "DROP TABLE ans")
BenchmarkResults[8, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Lags.csv"))
rm(list = c("BenchmarkResults","end","start"))


## 1M 2N 1D 3G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Lags.csv"))
start <- Sys.time()
dbExecute(
  con,
  "CREATE TABLE ans AS
  SELECT Date, Customer, Brand, Category, BeverageFlavor, DailyLiters, DailyUnits, DailyMargin, DailyRevenue,
  LAG(DailyLiters, 1) OVER(PARTITION BY Customer,Brand,Category) AS LagDailyLiters1,
  LAG(DailyLiters, 2) OVER(PARTITION BY Customer,Brand,Category) AS LagDailyLiters2,
  LAG(DailyLiters, 3) OVER(PARTITION BY Customer,Brand,Category) AS LagDailyLiters3,
  LAG(DailyLiters, 4) OVER(PARTITION BY Customer,Brand,Category) AS LagDailyLiters4,
  LAG(DailyLiters, 5) OVER(PARTITION BY Customer,Brand,Category) AS LagDailyLiters5,
  LAG(DailyUnits, 1) OVER(PARTITION BY Customer,Brand,Category) AS LagDailyUnits1,
  LAG(DailyUnits, 2) OVER(PARTITION BY Customer,Brand,Category) AS LagDailyUnits2,
  LAG(DailyUnits, 3) OVER(PARTITION BY Customer,Brand,Category) AS LagDailyUnits3,
  LAG(DailyUnits, 4) OVER(PARTITION BY Customer,Brand,Category) AS LagDailyUnits4,
  LAG(DailyUnits, 5) OVER(PARTITION BY Customer,Brand,Category) AS LagDailyUnits5
  FROM bmdata1M")
print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
end <- Sys.time()
dbExecute(con, "DROP TABLE ans")
BenchmarkResults[9, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Lags.csv"))
rm(list = c("BenchmarkResults","end","start"))


## 1M 2N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Lags.csv"))
start <- Sys.time()
dbExecute(
  con,
  "CREATE TABLE ans AS
  SELECT Date, Customer, Brand, Category, BeverageFlavor, DailyLiters, DailyUnits, DailyMargin, DailyRevenue,
  LAG(DailyLiters, 1) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor) AS LagDailyLiters1,
  LAG(DailyLiters, 2) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor) AS LagDailyLiters2,
  LAG(DailyLiters, 3) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor) AS LagDailyLiters3,
  LAG(DailyLiters, 4) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor) AS LagDailyLiters4,
  LAG(DailyLiters, 5) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor) AS LagDailyLiters5,
  LAG(DailyUnits, 1) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor) AS LagDailyUnits1,
  LAG(DailyUnits, 2) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor) AS LagDailyUnits2,
  LAG(DailyUnits, 3) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor) AS LagDailyUnits3,
  LAG(DailyUnits, 4) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor) AS LagDailyUnits4,
  LAG(DailyUnits, 5) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor) AS LagDailyUnits5
  FROM bmdata1M")
print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
end <- Sys.time()
dbExecute(con, "DROP TABLE ans")
BenchmarkResults[10, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Lags.csv"))
rm(list = c("BenchmarkResults","end","start"))


## 1M 3N 1D 0G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Lags.csv"))
start <- Sys.time()
dbExecute(
  con,
  "CREATE TABLE ans AS
  SELECT Date, Customer, Brand, Category, BeverageFlavor, DailyLiters, DailyUnits, DailyMargin, DailyRevenue,
  LAG(DailyLiters, 1) OVER() AS LagDailyLiters1,
  LAG(DailyLiters, 2) OVER() AS LagDailyLiters2,
  LAG(DailyLiters, 3) OVER() AS LagDailyLiters3,
  LAG(DailyLiters, 4) OVER() AS LagDailyLiters4,
  LAG(DailyLiters, 5) OVER() AS LagDailyLiters5,
  LAG(DailyUnits, 1) OVER() AS LagDailyUnits1,
  LAG(DailyUnits, 2) OVER() AS LagDailyUnits2,
  LAG(DailyUnits, 3) OVER() AS LagDailyUnits3,
  LAG(DailyUnits, 4) OVER() AS LagDailyUnits4,
  LAG(DailyUnits, 5) OVER() AS LagDailyUnits5,
  LAG(DailyMargin, 1) OVER() AS LagDailyMargin1,
  LAG(DailyMargin, 2) OVER() AS LagDailyMargin2,
  LAG(DailyMargin, 3) OVER() AS LagDailyMargin3,
  LAG(DailyMargin, 4) OVER() AS LagDailyMargin4,
  LAG(DailyMargin, 5) OVER() AS LagDailyMargin5
  FROM bmdata1M")
print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
end <- Sys.time()
dbExecute(con, "DROP TABLE ans")
BenchmarkResults[11, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Lags.csv"))
rm(list = c("BenchmarkResults","end","start"))


## 1M 3N 1D 1G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Lags.csv"))
start <- Sys.time()
dbExecute(
  con,
  "CREATE TABLE ans AS
  SELECT Date, Customer, Brand, Category, BeverageFlavor, DailyLiters, DailyUnits, DailyMargin, DailyRevenue,
  LAG(DailyLiters, 1) OVER(PARTITION BY Customer) AS LagDailyLiters1,
  LAG(DailyLiters, 2) OVER(PARTITION BY Customer) AS LagDailyLiters2,
  LAG(DailyLiters, 3) OVER(PARTITION BY Customer) AS LagDailyLiters3,
  LAG(DailyLiters, 4) OVER(PARTITION BY Customer) AS LagDailyLiters4,
  LAG(DailyLiters, 5) OVER(PARTITION BY Customer) AS LagDailyLiters5,
  LAG(DailyUnits, 1) OVER(PARTITION BY Customer) AS LagDailyUnits1,
  LAG(DailyUnits, 2) OVER(PARTITION BY Customer) AS LagDailyUnits2,
  LAG(DailyUnits, 3) OVER(PARTITION BY Customer) AS LagDailyUnits3,
  LAG(DailyUnits, 4) OVER(PARTITION BY Customer) AS LagDailyUnits4,
  LAG(DailyUnits, 5) OVER(PARTITION BY Customer) AS LagDailyUnits5,
  LAG(DailyMargin, 1) OVER(PARTITION BY Customer) AS LagDailyMargin1,
  LAG(DailyMargin, 2) OVER(PARTITION BY Customer) AS LagDailyMargin2,
  LAG(DailyMargin, 3) OVER(PARTITION BY Customer) AS LagDailyMargin3,
  LAG(DailyMargin, 4) OVER(PARTITION BY Customer) AS LagDailyMargin4,
  LAG(DailyMargin, 5) OVER(PARTITION BY Customer) AS LagDailyMargin5
  FROM bmdata1M")
print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
end <- Sys.time()
dbExecute(con, "DROP TABLE ans")
BenchmarkResults[12, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Lags.csv"))
rm(list = c("BenchmarkResults","end","start"))


## 1M 3N 1D 2G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Lags.csv"))
start <- Sys.time()
dbExecute(
  con,
  "CREATE TABLE ans AS
  SELECT Date, Customer, Brand, Category, BeverageFlavor, DailyLiters, DailyUnits, DailyMargin, DailyRevenue,
  LAG(DailyLiters, 1) OVER(PARTITION BY Customer,Brand) AS LagDailyLiters1,
  LAG(DailyLiters, 2) OVER(PARTITION BY Customer,Brand) AS LagDailyLiters2,
  LAG(DailyLiters, 3) OVER(PARTITION BY Customer,Brand) AS LagDailyLiters3,
  LAG(DailyLiters, 4) OVER(PARTITION BY Customer,Brand) AS LagDailyLiters4,
  LAG(DailyLiters, 5) OVER(PARTITION BY Customer,Brand) AS LagDailyLiters5,
  LAG(DailyUnits, 1) OVER(PARTITION BY Customer,Brand) AS LagDailyUnits1,
  LAG(DailyUnits, 2) OVER(PARTITION BY Customer,Brand) AS LagDailyUnits2,
  LAG(DailyUnits, 3) OVER(PARTITION BY Customer,Brand) AS LagDailyUnits3,
  LAG(DailyUnits, 4) OVER(PARTITION BY Customer,Brand) AS LagDailyUnits4,
  LAG(DailyUnits, 5) OVER(PARTITION BY Customer,Brand) AS LagDailyUnits5,
  LAG(DailyMargin, 1) OVER(PARTITION BY Customer,Brand) AS LagDailyMargin1,
  LAG(DailyMargin, 2) OVER(PARTITION BY Customer,Brand) AS LagDailyMargin2,
  LAG(DailyMargin, 3) OVER(PARTITION BY Customer,Brand) AS LagDailyMargin3,
  LAG(DailyMargin, 4) OVER(PARTITION BY Customer,Brand) AS LagDailyMargin4,
  LAG(DailyMargin, 5) OVER(PARTITION BY Customer,Brand) AS LagDailyMargin5
  FROM bmdata1M")
print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
end <- Sys.time()
dbExecute(con, "DROP TABLE ans")
BenchmarkResults[13, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Lags.csv"))
rm(list = c("BenchmarkResults","end","start"))


## 1M 3N 1D 3G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Lags.csv"))
start <- Sys.time()
dbExecute(
  con,
  "CREATE TABLE ans AS
  SELECT Date, Customer, Brand, Category, BeverageFlavor, DailyLiters, DailyUnits, DailyMargin, DailyRevenue,
  LAG(DailyLiters, 1) OVER(PARTITION BY Customer,Brand,Category) AS LagDailyLiters1,
  LAG(DailyLiters, 2) OVER(PARTITION BY Customer,Brand,Category) AS LagDailyLiters2,
  LAG(DailyLiters, 3) OVER(PARTITION BY Customer,Brand,Category) AS LagDailyLiters3,
  LAG(DailyLiters, 4) OVER(PARTITION BY Customer,Brand,Category) AS LagDailyLiters4,
  LAG(DailyLiters, 5) OVER(PARTITION BY Customer,Brand,Category) AS LagDailyLiters5,
  LAG(DailyUnits, 1) OVER(PARTITION BY Customer,Brand,Category) AS LagDailyUnits1,
  LAG(DailyUnits, 2) OVER(PARTITION BY Customer,Brand,Category) AS LagDailyUnits2,
  LAG(DailyUnits, 3) OVER(PARTITION BY Customer,Brand,Category) AS LagDailyUnits3,
  LAG(DailyUnits, 4) OVER(PARTITION BY Customer,Brand,Category) AS LagDailyUnits4,
  LAG(DailyUnits, 5) OVER(PARTITION BY Customer,Brand,Category) AS LagDailyUnits5,
  LAG(DailyMargin, 1) OVER(PARTITION BY Customer,Brand,Category) AS LagDailyMargin1,
  LAG(DailyMargin, 2) OVER(PARTITION BY Customer,Brand,Category) AS LagDailyMargin2,
  LAG(DailyMargin, 3) OVER(PARTITION BY Customer,Brand,Category) AS LagDailyMargin3,
  LAG(DailyMargin, 4) OVER(PARTITION BY Customer,Brand,Category) AS LagDailyMargin4,
  LAG(DailyMargin, 5) OVER(PARTITION BY Customer,Brand,Category) AS LagDailyMargin5
  FROM bmdata1M")
print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
end <- Sys.time()
dbExecute(con, "DROP TABLE ans")
BenchmarkResults[14, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Lags.csv"))
rm(list = c("BenchmarkResults","end","start"))


## 1M 3N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Lags.csv"))
start <- Sys.time()
dbExecute(
  con,
  "CREATE TABLE ans AS
  SELECT Date, Customer, Brand, Category, BeverageFlavor, DailyLiters, DailyUnits, DailyMargin, DailyRevenue,
  LAG(DailyLiters, 1) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor) AS LagDailyLiters1,
  LAG(DailyLiters, 2) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor) AS LagDailyLiters2,
  LAG(DailyLiters, 3) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor) AS LagDailyLiters3,
  LAG(DailyLiters, 4) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor) AS LagDailyLiters4,
  LAG(DailyLiters, 5) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor) AS LagDailyLiters5,
  LAG(DailyUnits, 1) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor) AS LagDailyUnits1,
  LAG(DailyUnits, 2) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor) AS LagDailyUnits2,
  LAG(DailyUnits, 3) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor) AS LagDailyUnits3,
  LAG(DailyUnits, 4) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor) AS LagDailyUnits4,
  LAG(DailyUnits, 5) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor) AS LagDailyUnits5,
  LAG(DailyMargin, 1) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor) AS LagDailyMargin1,
  LAG(DailyMargin, 2) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor) AS LagDailyMargin2,
  LAG(DailyMargin, 3) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor) AS LagDailyMargin3,
  LAG(DailyMargin, 4) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor) AS LagDailyMargin4,
  LAG(DailyMargin, 5) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor) AS LagDailyMargin5
  FROM bmdata1M")
print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
end <- Sys.time()
dbExecute(con, "DROP TABLE ans")
BenchmarkResults[15, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Lags.csv"))
rm(list = c("BenchmarkResults","end","start"))


###################################################################################################
###################################################################################################
###################################################################################################
###################################################################################################
###################################################################################################

dbDisconnect(con)
data <- data.table::fread(paste0(Path, "FakeBevData10M.csv"))
data.table::setnames(data, c("Beverage Flavor", "Daily Liters", "Daily Margin", "Daily Revenue", "Daily Units"), c("BeverageFlavor", "DailyLiters", "DailyMargin", "DailyRevenue", "DailyUnits"))
con = dbConnect(duckdb::duckdb())
ncores = parallel::detectCores()
invisible(dbExecute(con, sprintf("PRAGMA THREADS=%d", ncores)))
invisible(dbExecute(con, sprintf("SET THREADS=%d", ncores)))
table_name <- "bmdata10M"
dbWriteTable(con, "bmdata10M", data, overwrite = TRUE)
rm(data)

# Aggregation 10M

# Sum 1 Numeric Variable:

## 10M 1N 1D 0G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Lags.csv"))
start <- Sys.time()
dbExecute(
  con,
  "CREATE TABLE ans AS
  SELECT Date, Customer, Brand, Category, BeverageFlavor, DailyLiters, DailyUnits, DailyMargin, DailyRevenue,
  LAG(DailyLiters, 1) OVER() AS LagDailyLiters1,
  LAG(DailyLiters, 2) OVER() AS LagDailyLiters2,
  LAG(DailyLiters, 3) OVER() AS LagDailyLiters3,
  LAG(DailyLiters, 4) OVER() AS LagDailyLiters4,
  LAG(DailyLiters, 5) OVER() AS LagDailyLiters5
  FROM bmdata10M")
print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
end <- Sys.time()
dbExecute(con, "DROP TABLE ans")
BenchmarkResults[16, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Lags.csv"))
rm(list = c("BenchmarkResults","end","start"))


## 1M 1N 1D 1G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Lags.csv"))
start <- Sys.time()
dbExecute(
  con,
  "CREATE TABLE ans AS
  SELECT Date, Customer, Brand, Category, BeverageFlavor, DailyLiters, DailyUnits, DailyMargin, DailyRevenue,
  LAG(DailyLiters, 1) OVER(PARTITION BY Customer) AS LagDailyLiters1,
  LAG(DailyLiters, 2) OVER(PARTITION BY Customer) AS LagDailyLiters2,
  LAG(DailyLiters, 3) OVER(PARTITION BY Customer) AS LagDailyLiters3,
  LAG(DailyLiters, 4) OVER(PARTITION BY Customer) AS LagDailyLiters4,
  LAG(DailyLiters, 5) OVER(PARTITION BY Customer) AS LagDailyLiters5
  FROM bmdata10M")
print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
end <- Sys.time()
dbExecute(con, "DROP TABLE ans")
BenchmarkResults[17, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Lags.csv"))
rm(list = c("BenchmarkResults","end","start"))


## 1M 1N 1D 2G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Lags.csv"))
start <- Sys.time()
dbExecute(
  con,
  "CREATE TABLE ans AS
  SELECT Date, Customer, Brand, Category, BeverageFlavor, DailyLiters, DailyUnits, DailyMargin, DailyRevenue,
  LAG(DailyLiters, 1) OVER(PARTITION BY Customer,Brand) AS LagDailyLiters1,
  LAG(DailyLiters, 2) OVER(PARTITION BY Customer,Brand) AS LagDailyLiters2,
  LAG(DailyLiters, 3) OVER(PARTITION BY Customer,Brand) AS LagDailyLiters3,
  LAG(DailyLiters, 4) OVER(PARTITION BY Customer,Brand) AS LagDailyLiters4,
  LAG(DailyLiters, 5) OVER(PARTITION BY Customer,Brand) AS LagDailyLiters5
  FROM bmdata10M")
print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
end <- Sys.time()
dbExecute(con, "DROP TABLE ans")
BenchmarkResults[18, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Lags.csv"))
rm(list = c("BenchmarkResults","end","start"))


## 1M 1N 1D 3G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Lags.csv"))
start <- Sys.time()
dbExecute(
  con,
  "CREATE TABLE ans AS
  SELECT Date, Customer, Brand, Category, BeverageFlavor, DailyLiters, DailyUnits, DailyMargin, DailyRevenue,
  LAG(DailyLiters, 1) OVER(PARTITION BY Customer,Brand,Category) AS LagDailyLiters1,
  LAG(DailyLiters, 2) OVER(PARTITION BY Customer,Brand,Category) AS LagDailyLiters2,
  LAG(DailyLiters, 3) OVER(PARTITION BY Customer,Brand,Category) AS LagDailyLiters3,
  LAG(DailyLiters, 4) OVER(PARTITION BY Customer,Brand,Category) AS LagDailyLiters4,
  LAG(DailyLiters, 5) OVER(PARTITION BY Customer,Brand,Category) AS LagDailyLiters5
  FROM bmdata10M")
print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
end <- Sys.time()
dbExecute(con, "DROP TABLE ans")
BenchmarkResults[19, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Lags.csv"))
rm(list = c("BenchmarkResults","end","start"))


## 1M 1N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Lags.csv"))
start <- Sys.time()
dbExecute(
  con,
  "CREATE TABLE ans AS
  SELECT Date, Customer, Brand, Category, BeverageFlavor, DailyLiters, DailyUnits, DailyMargin, DailyRevenue,
  LAG(DailyLiters, 1) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor) AS LagDailyLiters1,
  LAG(DailyLiters, 2) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor) AS LagDailyLiters2,
  LAG(DailyLiters, 3) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor) AS LagDailyLiters3,
  LAG(DailyLiters, 4) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor) AS LagDailyLiters4,
  LAG(DailyLiters, 5) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor) AS LagDailyLiters5
  FROM bmdata10M")
print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
end <- Sys.time()
dbExecute(con, "DROP TABLE ans")
BenchmarkResults[20, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Lags.csv"))
rm(list = c("BenchmarkResults","end","start"))


## 1M 2N 1D 0G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Lags.csv"))
start <- Sys.time()
dbExecute(
  con,
  "CREATE TABLE ans AS
  SELECT Date, Customer, Brand, Category, BeverageFlavor, DailyLiters, DailyUnits, DailyMargin, DailyRevenue,
  LAG(DailyLiters, 1) OVER() AS LagDailyLiters1,
  LAG(DailyLiters, 2) OVER() AS LagDailyLiters2,
  LAG(DailyLiters, 3) OVER() AS LagDailyLiters3,
  LAG(DailyLiters, 4) OVER() AS LagDailyLiters4,
  LAG(DailyLiters, 5) OVER() AS LagDailyLiters5,
  LAG(DailyUnits, 1) OVER() AS LagDailyUnits1,
  LAG(DailyUnits, 2) OVER() AS LagDailyUnits2,
  LAG(DailyUnits, 3) OVER() AS LagDailyUnits3,
  LAG(DailyUnits, 4) OVER() AS LagDailyUnits4,
  LAG(DailyUnits, 5) OVER() AS LagDailyUnits5
  FROM bmdata10M")
print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
end <- Sys.time()
dbExecute(con, "DROP TABLE ans")
BenchmarkResults[21, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Lags.csv"))
rm(list = c("BenchmarkResults","end","start"))


## 1M 2N 1D 1G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Lags.csv"))
start <- Sys.time()
dbExecute(
  con,
  "CREATE TABLE ans AS
  SELECT Date, Customer, Brand, Category, BeverageFlavor, DailyLiters, DailyUnits, DailyMargin, DailyRevenue,
  LAG(DailyLiters, 1) OVER(PARTITION BY Customer) AS LagDailyLiters1,
  LAG(DailyLiters, 2) OVER(PARTITION BY Customer) AS LagDailyLiters2,
  LAG(DailyLiters, 3) OVER(PARTITION BY Customer) AS LagDailyLiters3,
  LAG(DailyLiters, 4) OVER(PARTITION BY Customer) AS LagDailyLiters4,
  LAG(DailyLiters, 5) OVER(PARTITION BY Customer) AS LagDailyLiters5,
  LAG(DailyUnits, 1) OVER(PARTITION BY Customer) AS LagDailyUnits1,
  LAG(DailyUnits, 2) OVER(PARTITION BY Customer) AS LagDailyUnits2,
  LAG(DailyUnits, 3) OVER(PARTITION BY Customer) AS LagDailyUnits3,
  LAG(DailyUnits, 4) OVER(PARTITION BY Customer) AS LagDailyUnits4,
  LAG(DailyUnits, 5) OVER(PARTITION BY Customer) AS LagDailyUnits5
  FROM bmdata10M")
print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
end <- Sys.time()
dbExecute(con, "DROP TABLE ans")
BenchmarkResults[22, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Lags.csv"))
rm(list = c("BenchmarkResults","end","start"))


## 1M 2N 1D 2G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Lags.csv"))
start <- Sys.time()
dbExecute(
  con,
  "CREATE TABLE ans AS
  SELECT Date, Customer, Brand, Category, BeverageFlavor, DailyLiters, DailyUnits, DailyMargin, DailyRevenue,
  LAG(DailyLiters, 1) OVER(PARTITION BY Customer,Brand) AS LagDailyLiters1,
  LAG(DailyLiters, 2) OVER(PARTITION BY Customer,Brand) AS LagDailyLiters2,
  LAG(DailyLiters, 3) OVER(PARTITION BY Customer,Brand) AS LagDailyLiters3,
  LAG(DailyLiters, 4) OVER(PARTITION BY Customer,Brand) AS LagDailyLiters4,
  LAG(DailyLiters, 5) OVER(PARTITION BY Customer,Brand) AS LagDailyLiters5,
  LAG(DailyUnits, 1) OVER(PARTITION BY Customer,Brand) AS LagDailyUnits1,
  LAG(DailyUnits, 2) OVER(PARTITION BY Customer,Brand) AS LagDailyUnits2,
  LAG(DailyUnits, 3) OVER(PARTITION BY Customer,Brand) AS LagDailyUnits3,
  LAG(DailyUnits, 4) OVER(PARTITION BY Customer,Brand) AS LagDailyUnits4,
  LAG(DailyUnits, 5) OVER(PARTITION BY Customer,Brand) AS LagDailyUnits5
  FROM bmdata10M")
print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
end <- Sys.time()
dbExecute(con, "DROP TABLE ans")
BenchmarkResults[23, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Lags.csv"))
rm(list = c("BenchmarkResults","end","start"))


## 1M 2N 1D 3G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Lags.csv"))
start <- Sys.time()
dbExecute(
  con,
  "CREATE TABLE ans AS
  SELECT Date, Customer, Brand, Category, BeverageFlavor, DailyLiters, DailyUnits, DailyMargin, DailyRevenue,
  LAG(DailyLiters, 1) OVER(PARTITION BY Customer,Brand,Category) AS LagDailyLiters1,
  LAG(DailyLiters, 2) OVER(PARTITION BY Customer,Brand,Category) AS LagDailyLiters2,
  LAG(DailyLiters, 3) OVER(PARTITION BY Customer,Brand,Category) AS LagDailyLiters3,
  LAG(DailyLiters, 4) OVER(PARTITION BY Customer,Brand,Category) AS LagDailyLiters4,
  LAG(DailyLiters, 5) OVER(PARTITION BY Customer,Brand,Category) AS LagDailyLiters5,
  LAG(DailyUnits, 1) OVER(PARTITION BY Customer,Brand,Category) AS LagDailyUnits1,
  LAG(DailyUnits, 2) OVER(PARTITION BY Customer,Brand,Category) AS LagDailyUnits2,
  LAG(DailyUnits, 3) OVER(PARTITION BY Customer,Brand,Category) AS LagDailyUnits3,
  LAG(DailyUnits, 4) OVER(PARTITION BY Customer,Brand,Category) AS LagDailyUnits4,
  LAG(DailyUnits, 5) OVER(PARTITION BY Customer,Brand,Category) AS LagDailyUnits5
  FROM bmdata10M")
print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
end <- Sys.time()
dbExecute(con, "DROP TABLE ans")
BenchmarkResults[24, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Lags.csv"))
rm(list = c("BenchmarkResults","end","start"))


## 1M 2N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Lags.csv"))
start <- Sys.time()
dbExecute(
  con,
  "CREATE TABLE ans AS
  SELECT Date, Customer, Brand, Category, BeverageFlavor, DailyLiters, DailyUnits, DailyMargin, DailyRevenue,
  LAG(DailyLiters, 1) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor) AS LagDailyLiters1,
  LAG(DailyLiters, 2) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor) AS LagDailyLiters2,
  LAG(DailyLiters, 3) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor) AS LagDailyLiters3,
  LAG(DailyLiters, 4) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor) AS LagDailyLiters4,
  LAG(DailyLiters, 5) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor) AS LagDailyLiters5,
  LAG(DailyUnits, 1) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor) AS LagDailyUnits1,
  LAG(DailyUnits, 2) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor) AS LagDailyUnits2,
  LAG(DailyUnits, 3) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor) AS LagDailyUnits3,
  LAG(DailyUnits, 4) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor) AS LagDailyUnits4,
  LAG(DailyUnits, 5) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor) AS LagDailyUnits5
  FROM bmdata10M")
print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
end <- Sys.time()
dbExecute(con, "DROP TABLE ans")
BenchmarkResults[25, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Lags.csv"))
rm(list = c("BenchmarkResults","end","start"))


## 1M 3N 1D 0G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Lags.csv"))
start <- Sys.time()
dbExecute(
  con,
  "CREATE TABLE ans AS
  SELECT Date, Customer, Brand, Category, BeverageFlavor, DailyLiters, DailyUnits, DailyMargin, DailyRevenue,
  LAG(DailyLiters, 1) OVER() AS LagDailyLiters1,
  LAG(DailyLiters, 2) OVER() AS LagDailyLiters2,
  LAG(DailyLiters, 3) OVER() AS LagDailyLiters3,
  LAG(DailyLiters, 4) OVER() AS LagDailyLiters4,
  LAG(DailyLiters, 5) OVER() AS LagDailyLiters5,
  LAG(DailyUnits, 1) OVER() AS LagDailyUnits1,
  LAG(DailyUnits, 2) OVER() AS LagDailyUnits2,
  LAG(DailyUnits, 3) OVER() AS LagDailyUnits3,
  LAG(DailyUnits, 4) OVER() AS LagDailyUnits4,
  LAG(DailyUnits, 5) OVER() AS LagDailyUnits5,
  LAG(DailyMargin, 1) OVER() AS LagDailyMargin1,
  LAG(DailyMargin, 2) OVER() AS LagDailyMargin2,
  LAG(DailyMargin, 3) OVER() AS LagDailyMargin3,
  LAG(DailyMargin, 4) OVER() AS LagDailyMargin4,
  LAG(DailyMargin, 5) OVER() AS LagDailyMargin5
  FROM bmdata10M")
print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
end <- Sys.time()
dbExecute(con, "DROP TABLE ans")
BenchmarkResults[26, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Lags.csv"))
rm(list = c("BenchmarkResults","end","start"))


## 1M 3N 1D 1G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Lags.csv"))
start <- Sys.time()
dbExecute(
  con,
  "CREATE TABLE ans AS
  SELECT Date, Customer, Brand, Category, BeverageFlavor, DailyLiters, DailyUnits, DailyMargin, DailyRevenue,
  LAG(DailyLiters, 1) OVER(PARTITION BY Customer) AS LagDailyLiters1,
  LAG(DailyLiters, 2) OVER(PARTITION BY Customer) AS LagDailyLiters2,
  LAG(DailyLiters, 3) OVER(PARTITION BY Customer) AS LagDailyLiters3,
  LAG(DailyLiters, 4) OVER(PARTITION BY Customer) AS LagDailyLiters4,
  LAG(DailyLiters, 5) OVER(PARTITION BY Customer) AS LagDailyLiters5,
  LAG(DailyUnits, 1) OVER(PARTITION BY Customer) AS LagDailyUnits1,
  LAG(DailyUnits, 2) OVER(PARTITION BY Customer) AS LagDailyUnits2,
  LAG(DailyUnits, 3) OVER(PARTITION BY Customer) AS LagDailyUnits3,
  LAG(DailyUnits, 4) OVER(PARTITION BY Customer) AS LagDailyUnits4,
  LAG(DailyUnits, 5) OVER(PARTITION BY Customer) AS LagDailyUnits5,
  LAG(DailyMargin, 1) OVER(PARTITION BY Customer) AS LagDailyMargin1,
  LAG(DailyMargin, 2) OVER(PARTITION BY Customer) AS LagDailyMargin2,
  LAG(DailyMargin, 3) OVER(PARTITION BY Customer) AS LagDailyMargin3,
  LAG(DailyMargin, 4) OVER(PARTITION BY Customer) AS LagDailyMargin4,
  LAG(DailyMargin, 5) OVER(PARTITION BY Customer) AS LagDailyMargin5
  FROM bmdata10M")
print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
end <- Sys.time()
dbExecute(con, "DROP TABLE ans")
BenchmarkResults[27, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Lags.csv"))
rm(list = c("BenchmarkResults","end","start"))


## 1M 3N 1D 2G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Lags.csv"))
start <- Sys.time()
dbExecute(
  con,
  "CREATE TABLE ans AS
  SELECT Date, Customer, Brand, Category, BeverageFlavor, DailyLiters, DailyUnits, DailyMargin, DailyRevenue,
  LAG(DailyLiters, 1) OVER(PARTITION BY Customer,Brand) AS LagDailyLiters1,
  LAG(DailyLiters, 2) OVER(PARTITION BY Customer,Brand) AS LagDailyLiters2,
  LAG(DailyLiters, 3) OVER(PARTITION BY Customer,Brand) AS LagDailyLiters3,
  LAG(DailyLiters, 4) OVER(PARTITION BY Customer,Brand) AS LagDailyLiters4,
  LAG(DailyLiters, 5) OVER(PARTITION BY Customer,Brand) AS LagDailyLiters5,
  LAG(DailyUnits, 1) OVER(PARTITION BY Customer,Brand) AS LagDailyUnits1,
  LAG(DailyUnits, 2) OVER(PARTITION BY Customer,Brand) AS LagDailyUnits2,
  LAG(DailyUnits, 3) OVER(PARTITION BY Customer,Brand) AS LagDailyUnits3,
  LAG(DailyUnits, 4) OVER(PARTITION BY Customer,Brand) AS LagDailyUnits4,
  LAG(DailyUnits, 5) OVER(PARTITION BY Customer,Brand) AS LagDailyUnits5,
  LAG(DailyMargin, 1) OVER(PARTITION BY Customer,Brand) AS LagDailyMargin1,
  LAG(DailyMargin, 2) OVER(PARTITION BY Customer,Brand) AS LagDailyMargin2,
  LAG(DailyMargin, 3) OVER(PARTITION BY Customer,Brand) AS LagDailyMargin3,
  LAG(DailyMargin, 4) OVER(PARTITION BY Customer,Brand) AS LagDailyMargin4,
  LAG(DailyMargin, 5) OVER(PARTITION BY Customer,Brand) AS LagDailyMargin5
  FROM bmdata10M")
print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
end <- Sys.time()
dbExecute(con, "DROP TABLE ans")
BenchmarkResults[28, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Lags.csv"))
rm(list = c("BenchmarkResults","end","start"))


## 1M 3N 1D 3G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Lags.csv"))
start <- Sys.time()
dbExecute(
  con,
  "CREATE TABLE ans AS
  SELECT Date, Customer, Brand, Category, BeverageFlavor, DailyLiters, DailyUnits, DailyMargin, DailyRevenue,
  LAG(DailyLiters, 1) OVER(PARTITION BY Customer,Brand,Category) AS LagDailyLiters1,
  LAG(DailyLiters, 2) OVER(PARTITION BY Customer,Brand,Category) AS LagDailyLiters2,
  LAG(DailyLiters, 3) OVER(PARTITION BY Customer,Brand,Category) AS LagDailyLiters3,
  LAG(DailyLiters, 4) OVER(PARTITION BY Customer,Brand,Category) AS LagDailyLiters4,
  LAG(DailyLiters, 5) OVER(PARTITION BY Customer,Brand,Category) AS LagDailyLiters5,
  LAG(DailyUnits, 1) OVER(PARTITION BY Customer,Brand,Category) AS LagDailyUnits1,
  LAG(DailyUnits, 2) OVER(PARTITION BY Customer,Brand,Category) AS LagDailyUnits2,
  LAG(DailyUnits, 3) OVER(PARTITION BY Customer,Brand,Category) AS LagDailyUnits3,
  LAG(DailyUnits, 4) OVER(PARTITION BY Customer,Brand,Category) AS LagDailyUnits4,
  LAG(DailyUnits, 5) OVER(PARTITION BY Customer,Brand,Category) AS LagDailyUnits5,
  LAG(DailyMargin, 1) OVER(PARTITION BY Customer,Brand,Category) AS LagDailyMargin1,
  LAG(DailyMargin, 2) OVER(PARTITION BY Customer,Brand,Category) AS LagDailyMargin2,
  LAG(DailyMargin, 3) OVER(PARTITION BY Customer,Brand,Category) AS LagDailyMargin3,
  LAG(DailyMargin, 4) OVER(PARTITION BY Customer,Brand,Category) AS LagDailyMargin4,
  LAG(DailyMargin, 5) OVER(PARTITION BY Customer,Brand,Category) AS LagDailyMargin5
  FROM bmdata10M")
print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
end <- Sys.time()
dbExecute(con, "DROP TABLE ans")
BenchmarkResults[29, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Lags.csv"))
rm(list = c("BenchmarkResults","end","start"))


## 1M 3N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Lags.csv"))
start <- Sys.time()
dbExecute(
  con,
  "CREATE TABLE ans AS
  SELECT Date, Customer, Brand, Category, BeverageFlavor, DailyLiters, DailyUnits, DailyMargin, DailyRevenue,
  LAG(DailyLiters, 1) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor) AS LagDailyLiters1,
  LAG(DailyLiters, 2) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor) AS LagDailyLiters2,
  LAG(DailyLiters, 3) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor) AS LagDailyLiters3,
  LAG(DailyLiters, 4) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor) AS LagDailyLiters4,
  LAG(DailyLiters, 5) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor) AS LagDailyLiters5,
  LAG(DailyUnits, 1) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor) AS LagDailyUnits1,
  LAG(DailyUnits, 2) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor) AS LagDailyUnits2,
  LAG(DailyUnits, 3) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor) AS LagDailyUnits3,
  LAG(DailyUnits, 4) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor) AS LagDailyUnits4,
  LAG(DailyUnits, 5) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor) AS LagDailyUnits5,
  LAG(DailyMargin, 1) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor) AS LagDailyMargin1,
  LAG(DailyMargin, 2) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor) AS LagDailyMargin2,
  LAG(DailyMargin, 3) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor) AS LagDailyMargin3,
  LAG(DailyMargin, 4) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor) AS LagDailyMargin4,
  LAG(DailyMargin, 5) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor) AS LagDailyMargin5
  FROM bmdata10M")
print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
end <- Sys.time()
dbExecute(con, "DROP TABLE ans")
BenchmarkResults[30, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Lags.csv"))
rm(list = c("BenchmarkResults","end","start"))


###################################################################################################
###################################################################################################
###################################################################################################
###################################################################################################
###################################################################################################

dbDisconnect(con)
data <- data.table::fread(paste0(Path, "FakeBevData100M.csv"))
data.table::setnames(data, c("Beverage Flavor", "Daily Liters", "Daily Margin", "Daily Revenue", "Daily Units"), c("BeverageFlavor", "DailyLiters", "DailyMargin", "DailyRevenue", "DailyUnits"))
con = dbConnect(duckdb::duckdb())
ncores = parallel::detectCores()
invisible(dbExecute(con, sprintf("PRAGMA THREADS=%d", ncores)))
invisible(dbExecute(con, sprintf("SET THREADS=%d", ncores)))
table_name <- "bmdata100M"
dbWriteTable(con, "bmdata100M", data, overwrite = TRUE)
rm(data)

# Aggregation 100M

# Sum 1 Numeric Variable:

## 100M 1N 1D 0G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Lags.csv"))
start <- Sys.time()
dbExecute(
  con,
  "CREATE TABLE ans AS
  SELECT Date, Customer, Brand, Category, BeverageFlavor, DailyLiters, DailyUnits, DailyMargin, DailyRevenue,
  LAG(DailyLiters, 1) OVER() AS LagDailyLiters1,
  LAG(DailyLiters, 2) OVER() AS LagDailyLiters2,
  LAG(DailyLiters, 3) OVER() AS LagDailyLiters3,
  LAG(DailyLiters, 4) OVER() AS LagDailyLiters4,
  LAG(DailyLiters, 5) OVER() AS LagDailyLiters5
  FROM bmdata100M")
print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
end <- Sys.time()
dbExecute(con, "DROP TABLE ans")
BenchmarkResults[31, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Lags.csv"))
rm(list = c("BenchmarkResults","end","start"))


## 1M 1N 1D 1G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Lags.csv"))
start <- Sys.time()
dbExecute(
  con,
  "CREATE TABLE ans AS
  SELECT Date, Customer, Brand, Category, BeverageFlavor, DailyLiters, DailyUnits, DailyMargin, DailyRevenue,
  LAG(DailyLiters, 1) OVER(PARTITION BY Customer) AS LagDailyLiters1,
  LAG(DailyLiters, 2) OVER(PARTITION BY Customer) AS LagDailyLiters2,
  LAG(DailyLiters, 3) OVER(PARTITION BY Customer) AS LagDailyLiters3,
  LAG(DailyLiters, 4) OVER(PARTITION BY Customer) AS LagDailyLiters4,
  LAG(DailyLiters, 5) OVER(PARTITION BY Customer) AS LagDailyLiters5
  FROM bmdata100M")
print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
end <- Sys.time()
dbExecute(con, "DROP TABLE ans")
BenchmarkResults[32, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Lags.csv"))
rm(list = c("BenchmarkResults","end","start"))


## 1M 1N 1D 2G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Lags.csv"))
start <- Sys.time()
dbExecute(
  con,
  "CREATE TABLE ans AS
  SELECT Date, Customer, Brand, Category, BeverageFlavor, DailyLiters, DailyUnits, DailyMargin, DailyRevenue,
  LAG(DailyLiters, 1) OVER(PARTITION BY Customer,Brand) AS LagDailyLiters1,
  LAG(DailyLiters, 2) OVER(PARTITION BY Customer,Brand) AS LagDailyLiters2,
  LAG(DailyLiters, 3) OVER(PARTITION BY Customer,Brand) AS LagDailyLiters3,
  LAG(DailyLiters, 4) OVER(PARTITION BY Customer,Brand) AS LagDailyLiters4,
  LAG(DailyLiters, 5) OVER(PARTITION BY Customer,Brand) AS LagDailyLiters5
  FROM bmdata100M")
print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
end <- Sys.time()
dbExecute(con, "DROP TABLE ans")
BenchmarkResults[33, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Lags.csv"))
rm(list = c("BenchmarkResults","end","start"))


## 1M 1N 1D 3G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Lags.csv"))
start <- Sys.time()
dbExecute(
  con,
  "CREATE TABLE ans AS
  SELECT Date, Customer, Brand, Category, BeverageFlavor, DailyLiters, DailyUnits, DailyMargin, DailyRevenue,
  LAG(DailyLiters, 1) OVER(PARTITION BY Customer,Brand,Category) AS LagDailyLiters1,
  LAG(DailyLiters, 2) OVER(PARTITION BY Customer,Brand,Category) AS LagDailyLiters2,
  LAG(DailyLiters, 3) OVER(PARTITION BY Customer,Brand,Category) AS LagDailyLiters3,
  LAG(DailyLiters, 4) OVER(PARTITION BY Customer,Brand,Category) AS LagDailyLiters4,
  LAG(DailyLiters, 5) OVER(PARTITION BY Customer,Brand,Category) AS LagDailyLiters5
  FROM bmdata100M")
print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
end <- Sys.time()
dbExecute(con, "DROP TABLE ans")
BenchmarkResults[34, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Lags.csv"))
rm(list = c("BenchmarkResults","end","start"))


## 1M 1N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Lags.csv"))
start <- Sys.time()
dbExecute(
  con,
  "CREATE TABLE ans AS
  SELECT Date, Customer, Brand, Category, BeverageFlavor, DailyLiters, DailyUnits, DailyMargin, DailyRevenue,
  LAG(DailyLiters, 1) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor) AS LagDailyLiters1,
  LAG(DailyLiters, 2) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor) AS LagDailyLiters2,
  LAG(DailyLiters, 3) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor) AS LagDailyLiters3,
  LAG(DailyLiters, 4) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor) AS LagDailyLiters4,
  LAG(DailyLiters, 5) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor) AS LagDailyLiters5
  FROM bmdata100M")
print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
end <- Sys.time()
dbExecute(con, "DROP TABLE ans")
BenchmarkResults[35, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Lags.csv"))
rm(list = c("BenchmarkResults","end","start"))


## 1M 2N 1D 0G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Lags.csv"))
start <- Sys.time()
dbExecute(
  con,
  "CREATE TABLE ans AS
  SELECT Date, Customer, Brand, Category, BeverageFlavor, DailyLiters, DailyUnits, DailyMargin, DailyRevenue,
  LAG(DailyLiters, 1) OVER() AS LagDailyLiters1,
  LAG(DailyLiters, 2) OVER() AS LagDailyLiters2,
  LAG(DailyLiters, 3) OVER() AS LagDailyLiters3,
  LAG(DailyLiters, 4) OVER() AS LagDailyLiters4,
  LAG(DailyLiters, 5) OVER() AS LagDailyLiters5,
  LAG(DailyUnits, 1) OVER() AS LagDailyUnits1,
  LAG(DailyUnits, 2) OVER() AS LagDailyUnits2,
  LAG(DailyUnits, 3) OVER() AS LagDailyUnits3,
  LAG(DailyUnits, 4) OVER() AS LagDailyUnits4,
  LAG(DailyUnits, 5) OVER() AS LagDailyUnits5
  FROM bmdata100M")
print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
end <- Sys.time()
dbExecute(con, "DROP TABLE ans")
BenchmarkResults[36, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Lags.csv"))
rm(list = c("BenchmarkResults","end","start"))


## 1M 2N 1D 1G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Lags.csv"))
start <- Sys.time()
dbExecute(
  con,
  "CREATE TABLE ans AS
  SELECT Date, Customer, Brand, Category, BeverageFlavor, DailyLiters, DailyUnits, DailyMargin, DailyRevenue,
  LAG(DailyLiters, 1) OVER(PARTITION BY Customer) AS LagDailyLiters1,
  LAG(DailyLiters, 2) OVER(PARTITION BY Customer) AS LagDailyLiters2,
  LAG(DailyLiters, 3) OVER(PARTITION BY Customer) AS LagDailyLiters3,
  LAG(DailyLiters, 4) OVER(PARTITION BY Customer) AS LagDailyLiters4,
  LAG(DailyLiters, 5) OVER(PARTITION BY Customer) AS LagDailyLiters5,
  LAG(DailyUnits, 1) OVER(PARTITION BY Customer) AS LagDailyUnits1,
  LAG(DailyUnits, 2) OVER(PARTITION BY Customer) AS LagDailyUnits2,
  LAG(DailyUnits, 3) OVER(PARTITION BY Customer) AS LagDailyUnits3,
  LAG(DailyUnits, 4) OVER(PARTITION BY Customer) AS LagDailyUnits4,
  LAG(DailyUnits, 5) OVER(PARTITION BY Customer) AS LagDailyUnits5
  FROM bmdata100M")
print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
end <- Sys.time()
dbExecute(con, "DROP TABLE ans")
BenchmarkResults[37, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Lags.csv"))
rm(list = c("BenchmarkResults","end","start"))


## 1M 2N 1D 2G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Lags.csv"))
start <- Sys.time()
dbExecute(
  con,
  "CREATE TABLE ans AS
  SELECT Date, Customer, Brand, Category, BeverageFlavor, DailyLiters, DailyUnits, DailyMargin, DailyRevenue,
  LAG(DailyLiters, 1) OVER(PARTITION BY Customer,Brand) AS LagDailyLiters1,
  LAG(DailyLiters, 2) OVER(PARTITION BY Customer,Brand) AS LagDailyLiters2,
  LAG(DailyLiters, 3) OVER(PARTITION BY Customer,Brand) AS LagDailyLiters3,
  LAG(DailyLiters, 4) OVER(PARTITION BY Customer,Brand) AS LagDailyLiters4,
  LAG(DailyLiters, 5) OVER(PARTITION BY Customer,Brand) AS LagDailyLiters5,
  LAG(DailyUnits, 1) OVER(PARTITION BY Customer,Brand) AS LagDailyUnits1,
  LAG(DailyUnits, 2) OVER(PARTITION BY Customer,Brand) AS LagDailyUnits2,
  LAG(DailyUnits, 3) OVER(PARTITION BY Customer,Brand) AS LagDailyUnits3,
  LAG(DailyUnits, 4) OVER(PARTITION BY Customer,Brand) AS LagDailyUnits4,
  LAG(DailyUnits, 5) OVER(PARTITION BY Customer,Brand) AS LagDailyUnits5
  FROM bmdata100M")
print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
end <- Sys.time()
dbExecute(con, "DROP TABLE ans")
BenchmarkResults[38, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Lags.csv"))
rm(list = c("BenchmarkResults","end","start"))


## 1M 2N 1D 3G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Lags.csv"))
start <- Sys.time()
dbExecute(
  con,
  "CREATE TABLE ans AS
  SELECT Date, Customer, Brand, Category, BeverageFlavor, DailyLiters, DailyUnits, DailyMargin, DailyRevenue,
  LAG(DailyLiters, 1) OVER(PARTITION BY Customer,Brand,Category) AS LagDailyLiters1,
  LAG(DailyLiters, 2) OVER(PARTITION BY Customer,Brand,Category) AS LagDailyLiters2,
  LAG(DailyLiters, 3) OVER(PARTITION BY Customer,Brand,Category) AS LagDailyLiters3,
  LAG(DailyLiters, 4) OVER(PARTITION BY Customer,Brand,Category) AS LagDailyLiters4,
  LAG(DailyLiters, 5) OVER(PARTITION BY Customer,Brand,Category) AS LagDailyLiters5,
  LAG(DailyUnits, 1) OVER(PARTITION BY Customer,Brand,Category) AS LagDailyUnits1,
  LAG(DailyUnits, 2) OVER(PARTITION BY Customer,Brand,Category) AS LagDailyUnits2,
  LAG(DailyUnits, 3) OVER(PARTITION BY Customer,Brand,Category) AS LagDailyUnits3,
  LAG(DailyUnits, 4) OVER(PARTITION BY Customer,Brand,Category) AS LagDailyUnits4,
  LAG(DailyUnits, 5) OVER(PARTITION BY Customer,Brand,Category) AS LagDailyUnits5
  FROM bmdata100M")
print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
end <- Sys.time()
dbExecute(con, "DROP TABLE ans")
BenchmarkResults[39, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Lags.csv"))
rm(list = c("BenchmarkResults","end","start"))


## 1M 2N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Lags.csv"))
start <- Sys.time()
dbExecute(
  con,
  "CREATE TABLE ans AS
  SELECT Date, Customer, Brand, Category, BeverageFlavor, DailyLiters, DailyUnits, DailyMargin, DailyRevenue,
  LAG(DailyLiters, 1) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor) AS LagDailyLiters1,
  LAG(DailyLiters, 2) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor) AS LagDailyLiters2,
  LAG(DailyLiters, 3) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor) AS LagDailyLiters3,
  LAG(DailyLiters, 4) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor) AS LagDailyLiters4,
  LAG(DailyLiters, 5) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor) AS LagDailyLiters5,
  LAG(DailyUnits, 1) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor) AS LagDailyUnits1,
  LAG(DailyUnits, 2) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor) AS LagDailyUnits2,
  LAG(DailyUnits, 3) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor) AS LagDailyUnits3,
  LAG(DailyUnits, 4) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor) AS LagDailyUnits4,
  LAG(DailyUnits, 5) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor) AS LagDailyUnits5
  FROM bmdata100M")
print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
end <- Sys.time()
dbExecute(con, "DROP TABLE ans")
BenchmarkResults[40, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Lags.csv"))
rm(list = c("BenchmarkResults","end","start"))


## 1M 3N 1D 0G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Lags.csv"))
start <- Sys.time()
dbExecute(
  con,
  "CREATE TABLE ans AS
  SELECT Date, Customer, Brand, Category, BeverageFlavor, DailyLiters, DailyUnits, DailyMargin, DailyRevenue,
  LAG(DailyLiters, 1) OVER() AS LagDailyLiters1,
  LAG(DailyLiters, 2) OVER() AS LagDailyLiters2,
  LAG(DailyLiters, 3) OVER() AS LagDailyLiters3,
  LAG(DailyLiters, 4) OVER() AS LagDailyLiters4,
  LAG(DailyLiters, 5) OVER() AS LagDailyLiters5,
  LAG(DailyUnits, 1) OVER() AS LagDailyUnits1,
  LAG(DailyUnits, 2) OVER() AS LagDailyUnits2,
  LAG(DailyUnits, 3) OVER() AS LagDailyUnits3,
  LAG(DailyUnits, 4) OVER() AS LagDailyUnits4,
  LAG(DailyUnits, 5) OVER() AS LagDailyUnits5,
  LAG(DailyMargin, 1) OVER() AS LagDailyMargin1,
  LAG(DailyMargin, 2) OVER() AS LagDailyMargin2,
  LAG(DailyMargin, 3) OVER() AS LagDailyMargin3,
  LAG(DailyMargin, 4) OVER() AS LagDailyMargin4,
  LAG(DailyMargin, 5) OVER() AS LagDailyMargin5
  FROM bmdata100M")
print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
end <- Sys.time()
dbExecute(con, "DROP TABLE ans")
BenchmarkResults[41, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Lags.csv"))
rm(list = c("BenchmarkResults","end","start"))


## 1M 3N 1D 1G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Lags.csv"))
start <- Sys.time()
dbExecute(
  con,
  "CREATE TABLE ans AS
  SELECT Date, Customer, Brand, Category, BeverageFlavor, DailyLiters, DailyUnits, DailyMargin, DailyRevenue,
  LAG(DailyLiters, 1) OVER(PARTITION BY Customer) AS LagDailyLiters1,
  LAG(DailyLiters, 2) OVER(PARTITION BY Customer) AS LagDailyLiters2,
  LAG(DailyLiters, 3) OVER(PARTITION BY Customer) AS LagDailyLiters3,
  LAG(DailyLiters, 4) OVER(PARTITION BY Customer) AS LagDailyLiters4,
  LAG(DailyLiters, 5) OVER(PARTITION BY Customer) AS LagDailyLiters5,
  LAG(DailyUnits, 1) OVER(PARTITION BY Customer) AS LagDailyUnits1,
  LAG(DailyUnits, 2) OVER(PARTITION BY Customer) AS LagDailyUnits2,
  LAG(DailyUnits, 3) OVER(PARTITION BY Customer) AS LagDailyUnits3,
  LAG(DailyUnits, 4) OVER(PARTITION BY Customer) AS LagDailyUnits4,
  LAG(DailyUnits, 5) OVER(PARTITION BY Customer) AS LagDailyUnits5,
  LAG(DailyMargin, 1) OVER(PARTITION BY Customer) AS LagDailyMargin1,
  LAG(DailyMargin, 2) OVER(PARTITION BY Customer) AS LagDailyMargin2,
  LAG(DailyMargin, 3) OVER(PARTITION BY Customer) AS LagDailyMargin3,
  LAG(DailyMargin, 4) OVER(PARTITION BY Customer) AS LagDailyMargin4,
  LAG(DailyMargin, 5) OVER(PARTITION BY Customer) AS LagDailyMargin5
  FROM bmdata100M")
print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
end <- Sys.time()
dbExecute(con, "DROP TABLE ans")
BenchmarkResults[42, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Lags.csv"))
rm(list = c("BenchmarkResults","end","start"))


## 1M 3N 1D 2G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Lags.csv"))
start <- Sys.time()
dbExecute(
  con,
  "CREATE TABLE ans AS
  SELECT Date, Customer, Brand, Category, BeverageFlavor, DailyLiters, DailyUnits, DailyMargin, DailyRevenue,
  LAG(DailyLiters, 1) OVER(PARTITION BY Customer,Brand) AS LagDailyLiters1,
  LAG(DailyLiters, 2) OVER(PARTITION BY Customer,Brand) AS LagDailyLiters2,
  LAG(DailyLiters, 3) OVER(PARTITION BY Customer,Brand) AS LagDailyLiters3,
  LAG(DailyLiters, 4) OVER(PARTITION BY Customer,Brand) AS LagDailyLiters4,
  LAG(DailyLiters, 5) OVER(PARTITION BY Customer,Brand) AS LagDailyLiters5,
  LAG(DailyUnits, 1) OVER(PARTITION BY Customer,Brand) AS LagDailyUnits1,
  LAG(DailyUnits, 2) OVER(PARTITION BY Customer,Brand) AS LagDailyUnits2,
  LAG(DailyUnits, 3) OVER(PARTITION BY Customer,Brand) AS LagDailyUnits3,
  LAG(DailyUnits, 4) OVER(PARTITION BY Customer,Brand) AS LagDailyUnits4,
  LAG(DailyUnits, 5) OVER(PARTITION BY Customer,Brand) AS LagDailyUnits5,
  LAG(DailyMargin, 1) OVER(PARTITION BY Customer,Brand) AS LagDailyMargin1,
  LAG(DailyMargin, 2) OVER(PARTITION BY Customer,Brand) AS LagDailyMargin2,
  LAG(DailyMargin, 3) OVER(PARTITION BY Customer,Brand) AS LagDailyMargin3,
  LAG(DailyMargin, 4) OVER(PARTITION BY Customer,Brand) AS LagDailyMargin4,
  LAG(DailyMargin, 5) OVER(PARTITION BY Customer,Brand) AS LagDailyMargin5
  FROM bmdata100M")
print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
end <- Sys.time()
dbExecute(con, "DROP TABLE ans")
BenchmarkResults[43, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Lags.csv"))
rm(list = c("BenchmarkResults","end","start"))


## 1M 3N 1D 3G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Lags.csv"))
start <- Sys.time()
dbExecute(
  con,
  "CREATE TABLE ans AS
  SELECT Date, Customer, Brand, Category, BeverageFlavor, DailyLiters, DailyUnits, DailyMargin, DailyRevenue,
  LAG(DailyLiters, 1) OVER(PARTITION BY Customer,Brand,Category) AS LagDailyLiters1,
  LAG(DailyLiters, 2) OVER(PARTITION BY Customer,Brand,Category) AS LagDailyLiters2,
  LAG(DailyLiters, 3) OVER(PARTITION BY Customer,Brand,Category) AS LagDailyLiters3,
  LAG(DailyLiters, 4) OVER(PARTITION BY Customer,Brand,Category) AS LagDailyLiters4,
  LAG(DailyLiters, 5) OVER(PARTITION BY Customer,Brand,Category) AS LagDailyLiters5,
  LAG(DailyUnits, 1) OVER(PARTITION BY Customer,Brand,Category) AS LagDailyUnits1,
  LAG(DailyUnits, 2) OVER(PARTITION BY Customer,Brand,Category) AS LagDailyUnits2,
  LAG(DailyUnits, 3) OVER(PARTITION BY Customer,Brand,Category) AS LagDailyUnits3,
  LAG(DailyUnits, 4) OVER(PARTITION BY Customer,Brand,Category) AS LagDailyUnits4,
  LAG(DailyUnits, 5) OVER(PARTITION BY Customer,Brand,Category) AS LagDailyUnits5,
  LAG(DailyMargin, 1) OVER(PARTITION BY Customer,Brand,Category) AS LagDailyMargin1,
  LAG(DailyMargin, 2) OVER(PARTITION BY Customer,Brand,Category) AS LagDailyMargin2,
  LAG(DailyMargin, 3) OVER(PARTITION BY Customer,Brand,Category) AS LagDailyMargin3,
  LAG(DailyMargin, 4) OVER(PARTITION BY Customer,Brand,Category) AS LagDailyMargin4,
  LAG(DailyMargin, 5) OVER(PARTITION BY Customer,Brand,Category) AS LagDailyMargin5
  FROM bmdata100M")
print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
end <- Sys.time()
dbExecute(con, "DROP TABLE ans")
BenchmarkResults[44, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Lags.csv"))
rm(list = c("BenchmarkResults","end","start"))


## 1M 3N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Lags.csv"))
start <- Sys.time()
dbExecute(
  con,
  "CREATE TABLE ans AS
  SELECT Date, Customer, Brand, Category, BeverageFlavor, DailyLiters, DailyUnits, DailyMargin, DailyRevenue,
  LAG(DailyLiters, 1) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor) AS LagDailyLiters1,
  LAG(DailyLiters, 2) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor) AS LagDailyLiters2,
  LAG(DailyLiters, 3) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor) AS LagDailyLiters3,
  LAG(DailyLiters, 4) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor) AS LagDailyLiters4,
  LAG(DailyLiters, 5) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor) AS LagDailyLiters5,
  LAG(DailyUnits, 1) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor) AS LagDailyUnits1,
  LAG(DailyUnits, 2) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor) AS LagDailyUnits2,
  LAG(DailyUnits, 3) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor) AS LagDailyUnits3,
  LAG(DailyUnits, 4) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor) AS LagDailyUnits4,
  LAG(DailyUnits, 5) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor) AS LagDailyUnits5,
  LAG(DailyMargin, 1) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor) AS LagDailyMargin1,
  LAG(DailyMargin, 2) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor) AS LagDailyMargin2,
  LAG(DailyMargin, 3) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor) AS LagDailyMargin3,
  LAG(DailyMargin, 4) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor) AS LagDailyMargin4,
  LAG(DailyMargin, 5) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor) AS LagDailyMargin5
  FROM bmdata100M")
print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
end <- Sys.time()
dbExecute(con, "DROP TABLE ans")
BenchmarkResults[45, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Lags.csv"))
rm(list = c("BenchmarkResults","end","start"))


BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Lags.csv"))
BenchmarkResults[46, TimeInSeconds := BenchmarkResults[1:45, sum(TimeInSeconds)]]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Lags.csv"))



