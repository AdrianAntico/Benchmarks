# Path to data storage
Path <- "C:/Users/Bizon/Documents/GitHub/rappwd/"

# Create Results table
BenchmarkResults <- data.table::data.table(
  Framework = 'duckdb',
  Method = 'lags',
  Experiment = c(
    "1M 3N 1D 4G 5L",
    "10M 3N 1D 4G 5L",
    "100M 3N 1D 4G 5L"
  ),

  TimeInSeconds = c(rep(-0.1, 3))
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
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Lags.csv"))
start <- Sys.time()
dbExecute(
  con,
  "CREATE TABLE ans AS
  SELECT Date, Customer, Brand, Category, BeverageFlavor, DailyLiters, DailyUnits, DailyMargin, DailyRevenue,
  LAG(DailyLiters, 1) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor ORDER BY Date ASC) AS LagDailyLiters1,
  LAG(DailyLiters, 2) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor ORDER BY Date ASC) AS LagDailyLiters2,
  LAG(DailyLiters, 3) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor ORDER BY Date ASC) AS LagDailyLiters3,
  LAG(DailyLiters, 4) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor ORDER BY Date ASC) AS LagDailyLiters4,
  LAG(DailyLiters, 5) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor ORDER BY Date ASC) AS LagDailyLiters5,
  LAG(DailyUnits, 1) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor ORDER BY Date ASC) AS LagDailyUnits1,
  LAG(DailyUnits, 2) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor ORDER BY Date ASC) AS LagDailyUnits2,
  LAG(DailyUnits, 3) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor ORDER BY Date ASC) AS LagDailyUnits3,
  LAG(DailyUnits, 4) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor ORDER BY Date ASC) AS LagDailyUnits4,
  LAG(DailyUnits, 5) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor ORDER BY Date ASC) AS LagDailyUnits5,
  LAG(DailyMargin, 1) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor ORDER BY Date ASC) AS LagDailyMargin1,
  LAG(DailyMargin, 2) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor ORDER BY Date ASC) AS LagDailyMargin2,
  LAG(DailyMargin, 3) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor ORDER BY Date ASC) AS LagDailyMargin3,
  LAG(DailyMargin, 4) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor ORDER BY Date ASC) AS LagDailyMargin4,
  LAG(DailyMargin, 5) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor ORDER BY Date ASC) AS LagDailyMargin5
  FROM bmdata1M")
print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
end <- Sys.time()
dbExecute(con, "DROP TABLE ans")
BenchmarkResults[1, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
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
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Lags.csv"))
start <- Sys.time()
dbExecute(
  con,
  "CREATE TABLE ans AS
  SELECT Date, Customer, Brand, Category, BeverageFlavor, DailyLiters, DailyUnits, DailyMargin, DailyRevenue,
  LAG(DailyLiters, 1) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor ORDER BY Date ASC) AS LagDailyLiters1,
  LAG(DailyLiters, 2) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor ORDER BY Date ASC) AS LagDailyLiters2,
  LAG(DailyLiters, 3) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor ORDER BY Date ASC) AS LagDailyLiters3,
  LAG(DailyLiters, 4) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor ORDER BY Date ASC) AS LagDailyLiters4,
  LAG(DailyLiters, 5) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor ORDER BY Date ASC) AS LagDailyLiters5,
  LAG(DailyUnits, 1) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor ORDER BY Date ASC) AS LagDailyUnits1,
  LAG(DailyUnits, 2) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor ORDER BY Date ASC) AS LagDailyUnits2,
  LAG(DailyUnits, 3) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor ORDER BY Date ASC) AS LagDailyUnits3,
  LAG(DailyUnits, 4) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor ORDER BY Date ASC) AS LagDailyUnits4,
  LAG(DailyUnits, 5) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor ORDER BY Date ASC) AS LagDailyUnits5,
  LAG(DailyMargin, 1) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor ORDER BY Date ASC) AS LagDailyMargin1,
  LAG(DailyMargin, 2) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor ORDER BY Date ASC) AS LagDailyMargin2,
  LAG(DailyMargin, 3) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor ORDER BY Date ASC) AS LagDailyMargin3,
  LAG(DailyMargin, 4) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor ORDER BY Date ASC) AS LagDailyMargin4,
  LAG(DailyMargin, 5) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor ORDER BY Date ASC) AS LagDailyMargin5
  FROM bmdata10M")
print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
end <- Sys.time()
dbExecute(con, "DROP TABLE ans")
BenchmarkResults[2, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
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

## 1M 3N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Lags.csv"))
start <- Sys.time()
dbExecute(
  con,
  "CREATE TABLE ans AS
  SELECT Date, Customer, Brand, Category, BeverageFlavor, DailyLiters, DailyUnits, DailyMargin, DailyRevenue,
  LAG(DailyLiters, 1) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor ORDER BY Date ASC) AS LagDailyLiters1,
  LAG(DailyLiters, 2) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor ORDER BY Date ASC) AS LagDailyLiters2,
  LAG(DailyLiters, 3) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor ORDER BY Date ASC) AS LagDailyLiters3,
  LAG(DailyLiters, 4) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor ORDER BY Date ASC) AS LagDailyLiters4,
  LAG(DailyLiters, 5) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor ORDER BY Date ASC) AS LagDailyLiters5,
  LAG(DailyUnits, 1) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor ORDER BY Date ASC) AS LagDailyUnits1,
  LAG(DailyUnits, 2) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor ORDER BY Date ASC) AS LagDailyUnits2,
  LAG(DailyUnits, 3) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor ORDER BY Date ASC) AS LagDailyUnits3,
  LAG(DailyUnits, 4) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor ORDER BY Date ASC) AS LagDailyUnits4,
  LAG(DailyUnits, 5) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor ORDER BY Date ASC) AS LagDailyUnits5,
  LAG(DailyMargin, 1) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor ORDER BY Date ASC) AS LagDailyMargin1,
  LAG(DailyMargin, 2) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor ORDER BY Date ASC) AS LagDailyMargin2,
  LAG(DailyMargin, 3) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor ORDER BY Date ASC) AS LagDailyMargin3,
  LAG(DailyMargin, 4) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor ORDER BY Date ASC) AS LagDailyMargin4,
  LAG(DailyMargin, 5) OVER(PARTITION BY Customer,Brand,Category,BeverageFlavor ORDER BY Date ASC) AS LagDailyMargin5
  FROM bmdata100M")
print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
end <- Sys.time()
dbExecute(con, "DROP TABLE ans")
BenchmarkResults[3, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Lags.csv"))
rm(list = c("BenchmarkResults","end","start"))
