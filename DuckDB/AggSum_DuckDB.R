# Path to data storage
Path <- "C:/Users/Bizon/Documents/GitHub/rappwd/"

# Create Results table
BenchmarkResults <- data.table::data.table(
  Framework = 'duckdb',
  Method = 'sum aggregation',
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

# Save results table
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB.csv"))
rm(BenchmarkResults)

# Environment setup
library(duckdb)
library(DBI)
data <- data.table::fread(paste0(Path, "FakeBevData1M.csv"))
data.table::setnames(data, c("Beverage Flavor", "Daily Liters", "Daily Margin", "Daily Revenue", "Daily Units"), c("BeverageFlavor", "DailyLiters", "DailyMargin", "DailyRevenue", "DailyUnits"))
con = dbConnect(duckdb::duckdb(), config = list(threads = 32))
# print(dbGetQuery(con, "PRAGMA threads"))
ncores = parallel::detectCores()
MaxMem = '243GB'
invisible(dbExecute(con, sprintf("PRAGMA THREADS=%d", ncores)))
invisible(dbExecute(con, sprintf("SET THREADS=%d", ncores)))
invisible(dbExecute(con, sprintf("SET memory_limit=%s", paste0("'", MaxMem, "'"))))
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
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB.csv"))
start <- Sys.time()
dbExecute(con, "CREATE TABLE ans AS SELECT Date, sum(DailyLiters) AS v1 FROM bmdata1M GROUP BY Date")
print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
end <- Sys.time()
dbExecute(con, "DROP TABLE ans")
BenchmarkResults[1, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB.csv"))
rm(list = c("BenchmarkResults","end","start"))


## 1M 1N 1D 1G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB.csv"))
start <- Sys.time()
dbExecute(con, "CREATE TABLE ans AS SELECT Date, Customer, sum(DailyLiters) AS v1 FROM bmdata1M GROUP BY Date, Customer")
print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
end <- Sys.time()
dbExecute(con, "DROP TABLE ans")
BenchmarkResults[2, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB.csv"))
rm(list = c("BenchmarkResults","end","start"))


## 1M 1N 1D 2G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB.csv"))
start <- Sys.time()
dbExecute(con, "CREATE TABLE ans AS SELECT Date, Customer, Brand, sum(DailyLiters) AS v1 FROM bmdata1M GROUP BY Date, Customer, Brand")
print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
end <- Sys.time()
dbExecute(con, "DROP TABLE ans")
BenchmarkResults[3, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB.csv"))
rm(list = c("BenchmarkResults","end","start"))


## 1M 1N 1D 3G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB.csv"))
start <- Sys.time()
dbExecute(con, "CREATE TABLE ans AS SELECT Date, Customer, Brand, Category, sum(DailyLiters) AS v1 FROM bmdata1M GROUP BY Date, Customer, Brand, Category")
print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
end <- Sys.time()
dbExecute(con, "DROP TABLE ans")
BenchmarkResults[4, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB.csv"))
rm(list = c("BenchmarkResults","end","start"))


## 1M 1N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB.csv"))
start <- Sys.time()
dbExecute(con, "CREATE TABLE ans AS SELECT Date, Customer, Brand, Category, BeverageFlavor, sum(DailyLiters) AS v1 FROM bmdata1M GROUP BY Date, Customer, Brand, Category, BeverageFlavor")
print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
end <- Sys.time()
dbExecute(con, "DROP TABLE ans")
BenchmarkResults[5, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB.csv"))
rm(list = c("BenchmarkResults","end","start"))


## 1M 2N 1D 0G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB.csv"))
start <- Sys.time()
dbExecute(con, "CREATE TABLE ans AS SELECT Date, sum(DailyLiters) AS v1, sum(DailyUnits) AS v2 FROM bmdata1M GROUP BY Date")
print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
end <- Sys.time()
dbExecute(con, "DROP TABLE ans")
BenchmarkResults[6, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB.csv"))
rm(list = c("BenchmarkResults","end","start"))


## 1M 2N 1D 1G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB.csv"))
start <- Sys.time()
dbExecute(con, "CREATE TABLE ans AS SELECT Date, Customer, sum(DailyLiters) AS v1, sum(DailyUnits) AS v2 FROM bmdata1M GROUP BY Date, Customer")
print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
end <- Sys.time()
dbExecute(con, "DROP TABLE ans")
BenchmarkResults[7, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB.csv"))
rm(list = c("BenchmarkResults","end","start"))


## 1M 2N 1D 2G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB.csv"))
start <- Sys.time()
dbExecute(con, "CREATE TABLE ans AS SELECT Date, Customer, Brand, sum(DailyLiters) AS v1, sum(DailyUnits) AS v2 FROM bmdata1M GROUP BY Date, Customer, Brand")
print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
end <- Sys.time()
dbExecute(con, "DROP TABLE ans")
BenchmarkResults[8, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB.csv"))
rm(list = c("BenchmarkResults","end","start"))


## 1M 2N 1D 3G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB.csv"))
start <- Sys.time()
dbExecute(con, "CREATE TABLE ans AS SELECT Date, Customer, Brand, Category, sum(DailyLiters) AS v1, sum(DailyUnits) AS v2 FROM bmdata1M GROUP BY Date, Customer, Brand, Category")
print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
end <- Sys.time()
dbExecute(con, "DROP TABLE ans")
BenchmarkResults[9, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB.csv"))
rm(list = c("BenchmarkResults","end","start"))


## 1M 2N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB.csv"))
start <- Sys.time()
dbExecute(con, "CREATE TABLE ans AS SELECT Date, Customer, Brand, Category, BeverageFlavor, sum(DailyLiters) AS v1, sum(DailyUnits) AS v2 FROM bmdata1M GROUP BY Date, Customer, Brand, Category, BeverageFlavor")
print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
end <- Sys.time()
dbExecute(con, "DROP TABLE ans")
BenchmarkResults[10, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB.csv"))
rm(list = c("BenchmarkResults","end","start"))


## 1M 3N 1D 0G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB.csv"))
start <- Sys.time()
dbExecute(con, "CREATE TABLE ans AS SELECT Date, sum(DailyLiters) AS v1, sum(DailyUnits) AS v2, sum(DailyMargin) AS v3 FROM bmdata1M GROUP BY Date")
print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
end <- Sys.time()
dbExecute(con, "DROP TABLE ans")
BenchmarkResults[11, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB.csv"))
rm(list = c("BenchmarkResults","end","start"))


## 1M 3N 1D 1G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB.csv"))
start <- Sys.time()
dbExecute(con, "CREATE TABLE ans AS SELECT Date, Customer, sum(DailyLiters) AS v1, sum(DailyUnits) AS v2, sum(DailyMargin) AS v3 FROM bmdata1M GROUP BY Date, Customer")
print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
end <- Sys.time()
dbExecute(con, "DROP TABLE ans")
BenchmarkResults[12, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB.csv"))
rm(list = c("BenchmarkResults","end","start"))


## 1M 3N 1D 2G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB.csv"))
start <- Sys.time()
dbExecute(con, "CREATE TABLE ans AS SELECT Date, Customer, Brand, sum(DailyLiters) AS v1, sum(DailyUnits) AS v2, sum(DailyMargin) AS v3 FROM bmdata1M GROUP BY Date, Customer, Brand")
print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
end <- Sys.time()
dbExecute(con, "DROP TABLE ans")
BenchmarkResults[13, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB.csv"))
rm(list = c("BenchmarkResults","end","start"))


## 1M 3N 1D 3G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB.csv"))
start <- Sys.time()
dbExecute(con, "CREATE TABLE ans AS SELECT Date, Customer, Brand, Category, sum(DailyLiters) AS v1, sum(DailyUnits) AS v2, sum(DailyMargin) AS v3 FROM bmdata1M GROUP BY Date, Customer, Brand, Category")
print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
end <- Sys.time()
dbExecute(con, "DROP TABLE ans")
BenchmarkResults[14, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB.csv"))
rm(list = c("BenchmarkResults","end","start"))


## 1M 3N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB.csv"))
start <- Sys.time()
dbExecute(con, "CREATE TABLE ans AS SELECT Date, Customer, Brand, Category, BeverageFlavor, sum(DailyLiters) AS v1, sum(DailyUnits) AS v2, sum(DailyMargin) AS v3 FROM bmdata1M GROUP BY Date, Customer, Brand, Category, BeverageFlavor")
print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
end <- Sys.time()
dbExecute(con, "DROP TABLE ans")
BenchmarkResults[15, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB.csv"))
rm(list = c("BenchmarkResults","end","start"))


###################################################################################################
###################################################################################################
###################################################################################################
###################################################################################################
###################################################################################################

dbDisconnect(con)
data <- data.table::fread(paste0(Path, "FakeBevData10M.csv"))
data.table::setnames(data, c("Beverage Flavor", "Daily Liters", "Daily Margin", "Daily Revenue", "Daily Units"), c("BeverageFlavor", "DailyLiters", "DailyMargin", "DailyRevenue", "DailyUnits"))
con = dbConnect(duckdb::duckdb(), config = list(threads = 32))
ncores = parallel::detectCores()
invisible(dbExecute(con, sprintf("PRAGMA THREADS=%d", ncores)))
invisible(dbExecute(con, sprintf("SET THREADS=%d", ncores)))
table_name <- "bmdata10M"
dbWriteTable(con, "bmdata10M", data, overwrite = TRUE)
rm(data)

# Aggregation 10M

# Sum 1 Numeric Variable:

## 10M 1N 1D 0G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB.csv"))
start <- Sys.time()
dbExecute(con, "CREATE TABLE ans AS SELECT Date, sum(DailyLiters) AS v1 FROM bmdata10M GROUP BY Date")
print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
end <- Sys.time()
dbExecute(con, "DROP TABLE ans")
BenchmarkResults[16, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB.csv"))
rm(list = c("BenchmarkResults","end","start"))


## 10M 1N 1D 1G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB.csv"))
start <- Sys.time()
dbExecute(con, "CREATE TABLE ans AS SELECT Date, Customer, sum(DailyLiters) AS v1 FROM bmdata10M GROUP BY Date, Customer")
print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
end <- Sys.time()
dbExecute(con, "DROP TABLE ans")
BenchmarkResults[17, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB.csv"))
rm(list = c("BenchmarkResults","end","start"))


## 10M 1N 1D 2G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB.csv"))
start <- Sys.time()
dbExecute(con, "CREATE TABLE ans AS SELECT Date, Customer, Brand, sum(DailyLiters) AS v1 FROM bmdata10M GROUP BY Date, Customer, Brand")
print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
end <- Sys.time()
dbExecute(con, "DROP TABLE ans")
BenchmarkResults[18, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB.csv"))
rm(list = c("BenchmarkResults","end","start"))


## 10M 1N 1D 3G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB.csv"))
start <- Sys.time()
dbExecute(con, "CREATE TABLE ans AS SELECT Date, Customer, Brand, Category, sum(DailyLiters) AS v1 FROM bmdata10M GROUP BY Date, Customer, Brand, Category")
print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
end <- Sys.time()
dbExecute(con, "DROP TABLE ans")
BenchmarkResults[19, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB.csv"))
rm(list = c("BenchmarkResults","end","start"))


## 10M 1N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB.csv"))
start <- Sys.time()
dbExecute(con, "CREATE TABLE ans AS SELECT Date, Customer, Brand, Category, BeverageFlavor, sum(DailyLiters) AS v1 FROM bmdata10M GROUP BY Date, Customer, Brand, Category, BeverageFlavor")
print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
end <- Sys.time()
dbExecute(con, "DROP TABLE ans")
BenchmarkResults[20, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB.csv"))
rm(list = c("BenchmarkResults","end","start"))


## 10M 2N 1D 0G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB.csv"))
start <- Sys.time()
dbExecute(con, "CREATE TABLE ans AS SELECT Date, sum(DailyLiters) AS v1, sum(DailyUnits) AS v2 FROM bmdata10M GROUP BY Date")
print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
end <- Sys.time()
dbExecute(con, "DROP TABLE ans")
BenchmarkResults[21, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB.csv"))
rm(list = c("BenchmarkResults","end","start"))


## 10M 2N 1D 1G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB.csv"))
start <- Sys.time()
dbExecute(con, "CREATE TABLE ans AS SELECT Date, Customer, sum(DailyLiters) AS v1, sum(DailyUnits) AS v2 FROM bmdata10M GROUP BY Date, Customer")
print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
end <- Sys.time()
dbExecute(con, "DROP TABLE ans")
BenchmarkResults[22, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB.csv"))
rm(list = c("BenchmarkResults","end","start"))


## 10M 2N 1D 2G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB.csv"))
start <- Sys.time()
dbExecute(con, "CREATE TABLE ans AS SELECT Date, Customer, Brand, sum(DailyLiters) AS v1, sum(DailyUnits) AS v2 FROM bmdata10M GROUP BY Date, Customer, Brand")
print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
end <- Sys.time()
dbExecute(con, "DROP TABLE ans")
BenchmarkResults[23, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB.csv"))
rm(list = c("BenchmarkResults","end","start"))


## 10M 2N 1D 3G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB.csv"))
start <- Sys.time()
dbExecute(con, "CREATE TABLE ans AS SELECT Date, Customer, Brand, Category, sum(DailyLiters) AS v1, sum(DailyUnits) AS v2 FROM bmdata10M GROUP BY Date, Customer, Brand, Category")
print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
end <- Sys.time()
dbExecute(con, "DROP TABLE ans")
BenchmarkResults[24, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB.csv"))
rm(list = c("BenchmarkResults","end","start"))


## 10M 2N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB.csv"))
start <- Sys.time()
dbExecute(con, "CREATE TABLE ans AS SELECT Date, Customer, Brand, Category, BeverageFlavor, sum(DailyLiters) AS v1, sum(DailyUnits) AS v2 FROM bmdata10M GROUP BY Date, Customer, Brand, Category, BeverageFlavor")
print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
end <- Sys.time()
dbExecute(con, "DROP TABLE ans")
BenchmarkResults[25, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB.csv"))
rm(list = c("BenchmarkResults","end","start"))


## 10M 3N 1D 0G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB.csv"))
start <- Sys.time()
dbExecute(con, "CREATE TABLE ans AS SELECT Date, sum(DailyLiters) AS v1, sum(DailyUnits) AS v2, sum(DailyMargin) AS v3 FROM bmdata10M GROUP BY Date")
print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
end <- Sys.time()
dbExecute(con, "DROP TABLE ans")
BenchmarkResults[26, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB.csv"))
rm(list = c("BenchmarkResults","end","start"))


## 10M 3N 1D 1G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB.csv"))
start <- Sys.time()
dbExecute(con, "CREATE TABLE ans AS SELECT Date, Customer, sum(DailyLiters) AS v1, sum(DailyUnits) AS v2, sum(DailyMargin) AS v3 FROM bmdata10M GROUP BY Date, Customer")
print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
end <- Sys.time()
dbExecute(con, "DROP TABLE ans")
BenchmarkResults[27, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB.csv"))
rm(list = c("BenchmarkResults","end","start"))


## 10M 3N 1D 2G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB.csv"))
start <- Sys.time()
dbExecute(con, "CREATE TABLE ans AS SELECT Date, Customer, Brand, sum(DailyLiters) AS v1, sum(DailyUnits) AS v2, sum(DailyMargin) AS v3 FROM bmdata10M GROUP BY Date, Customer, Brand")
print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
end <- Sys.time()
dbExecute(con, "DROP TABLE ans")
BenchmarkResults[28, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB.csv"))
rm(list = c("BenchmarkResults","end","start"))


## 10M 3N 1D 3G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB.csv"))
start <- Sys.time()
dbExecute(con, "CREATE TABLE ans AS SELECT Date, Customer, Brand, Category, sum(DailyLiters) AS v1, sum(DailyUnits) AS v2, sum(DailyMargin) AS v3 FROM bmdata10M GROUP BY Date, Customer, Brand, Category")
print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
end <- Sys.time()
dbExecute(con, "DROP TABLE ans")
BenchmarkResults[29, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB.csv"))
rm(list = c("BenchmarkResults","end","start"))


## 10M 3N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB.csv"))
start <- Sys.time()
dbExecute(con, "CREATE TABLE ans AS SELECT Date, Customer, Brand, Category, BeverageFlavor, sum(DailyLiters) AS v1, sum(DailyUnits) AS v2, sum(DailyMargin) AS v3 FROM bmdata10M GROUP BY Date, Customer, Brand, Category, BeverageFlavor")
print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
end <- Sys.time()
dbExecute(con, "DROP TABLE ans")
BenchmarkResults[30, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB.csv"))
rm(list = c("BenchmarkResults","end","start"))



###################################################################################################
###################################################################################################
###################################################################################################
###################################################################################################
###################################################################################################

dbDisconnect(con)
data <- data.table::fread(paste0(Path, "FakeBevData100M.csv"))
data.table::setnames(data, c("Beverage Flavor", "Daily Liters", "Daily Margin", "Daily Revenue", "Daily Units"), c("BeverageFlavor", "DailyLiters", "DailyMargin", "DailyRevenue", "DailyUnits"))
con = dbConnect(duckdb::duckdb(), config = list(threads = 32))
ncores = parallel::detectCores()
invisible(dbExecute(con, sprintf("PRAGMA THREADS=%d", ncores)))
invisible(dbExecute(con, sprintf("SET THREADS=%d", ncores)))
table_name <- "bmdata100M"
dbWriteTable(con, "bmdata100M", data, overwrite = TRUE)
rm(data)

# Aggregation 100M

# Sum 1 Numeric Variable:

## 100M 1N 1D 0G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB.csv"))
start <- Sys.time()
dbExecute(con, "CREATE TABLE ans AS SELECT Date, sum(DailyLiters) AS v1 FROM bmdata100M GROUP BY Date")
print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
end <- Sys.time()
dbExecute(con, "DROP TABLE ans")
BenchmarkResults[31, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB.csv"))
rm(list = c("BenchmarkResults","end","start"))


## 100M 1N 1D 1G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB.csv"))
start <- Sys.time()
dbExecute(con, "CREATE TABLE ans AS SELECT Date, Customer, sum(DailyLiters) AS v1 FROM bmdata100M GROUP BY Date, Customer")
print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
end <- Sys.time()
dbExecute(con, "DROP TABLE ans")
BenchmarkResults[32, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB.csv"))
rm(list = c("BenchmarkResults","end","start"))


## 100M 1N 1D 2G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB.csv"))
start <- Sys.time()
dbExecute(con, "CREATE TABLE ans AS SELECT Date, Customer, Brand, sum(DailyLiters) AS v1 FROM bmdata100M GROUP BY Date, Customer, Brand")
print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
end <- Sys.time()
dbExecute(con, "DROP TABLE ans")
BenchmarkResults[33, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB.csv"))
rm(list = c("BenchmarkResults","end","start"))


## 100M 1N 1D 3G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB.csv"))
start <- Sys.time()
dbExecute(con, "CREATE TABLE ans AS SELECT Date, Customer, Brand, Category, sum(DailyLiters) AS v1 FROM bmdata100M GROUP BY Date, Customer, Brand, Category")
print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
end <- Sys.time()
dbExecute(con, "DROP TABLE ans")
BenchmarkResults[34, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB.csv"))
rm(list = c("BenchmarkResults","end","start"))


## 100M 1N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB.csv"))
start <- Sys.time()
dbExecute(con, "CREATE TABLE ans AS SELECT Date, Customer, Brand, Category, BeverageFlavor, sum(DailyLiters) AS v1 FROM bmdata100M GROUP BY Date, Customer, Brand, Category, BeverageFlavor")
print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
end <- Sys.time()
dbExecute(con, "DROP TABLE ans")
BenchmarkResults[35, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB.csv"))
rm(list = c("BenchmarkResults","end","start"))


## 100M 2N 1D 0G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB.csv"))
start <- Sys.time()
dbExecute(con, "CREATE TABLE ans AS SELECT Date, sum(DailyLiters) AS v1, sum(DailyUnits) AS v2 FROM bmdata100M GROUP BY Date")
print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
end <- Sys.time()
dbExecute(con, "DROP TABLE ans")
BenchmarkResults[36, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB.csv"))
rm(list = c("BenchmarkResults","end","start"))


## 100M 2N 1D 1G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB.csv"))
start <- Sys.time()
dbExecute(con, "CREATE TABLE ans AS SELECT Date, Customer, sum(DailyLiters) AS v1, sum(DailyUnits) AS v2 FROM bmdata100M GROUP BY Date, Customer")
print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
end <- Sys.time()
dbExecute(con, "DROP TABLE ans")
BenchmarkResults[37, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB.csv"))
rm(list = c("BenchmarkResults","end","start"))


## 100M 2N 1D 2G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB.csv"))
start <- Sys.time()
dbExecute(con, "CREATE TABLE ans AS SELECT Date, Customer, Brand, sum(DailyLiters) AS v1, sum(DailyUnits) AS v2 FROM bmdata100M GROUP BY Date, Customer, Brand")
print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
end <- Sys.time()
dbExecute(con, "DROP TABLE ans")
BenchmarkResults[38, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB.csv"))
rm(list = c("BenchmarkResults","end","start"))


## 100M 2N 1D 3G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB.csv"))
start <- Sys.time()
dbExecute(con, "CREATE TABLE ans AS SELECT Date, Customer, Brand, Category, sum(DailyLiters) AS v1, sum(DailyUnits) AS v2 FROM bmdata100M GROUP BY Date, Customer, Brand, Category")
print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
end <- Sys.time()
dbExecute(con, "DROP TABLE ans")
BenchmarkResults[39, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB.csv"))
rm(list = c("BenchmarkResults","end","start"))


## 100M 2N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB.csv"))
start <- Sys.time()
dbExecute(con, "CREATE TABLE ans AS SELECT Date, Customer, Brand, Category, BeverageFlavor, sum(DailyLiters) AS v1, sum(DailyUnits) AS v2 FROM bmdata100M GROUP BY Date, Customer, Brand, Category, BeverageFlavor")
print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
end <- Sys.time()
dbExecute(con, "DROP TABLE ans")
BenchmarkResults[40, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB.csv"))
rm(list = c("BenchmarkResults","end","start"))


## 100M 3N 1D 0G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB.csv"))
start <- Sys.time()
dbExecute(con, "CREATE TABLE ans AS SELECT Date, sum(DailyLiters) AS v1, sum(DailyUnits) AS v2, sum(DailyMargin) AS v3 FROM bmdata100M GROUP BY Date")
print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
end <- Sys.time()
dbExecute(con, "DROP TABLE ans")
BenchmarkResults[41, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB.csv"))
rm(list = c("BenchmarkResults","end","start"))


## 100M 3N 1D 1G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB.csv"))
start <- Sys.time()
dbExecute(con, "CREATE TABLE ans AS SELECT Date, Customer, sum(DailyLiters) AS v1, sum(DailyUnits) AS v2, sum(DailyMargin) AS v3 FROM bmdata100M GROUP BY Date, Customer")
print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
end <- Sys.time()
dbExecute(con, "DROP TABLE ans")
BenchmarkResults[42, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB.csv"))
rm(list = c("BenchmarkResults","end","start"))


## 100M 3N 1D 2G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB.csv"))
start <- Sys.time()
dbExecute(con, "CREATE TABLE ans AS SELECT Date, Customer, Brand, sum(DailyLiters) AS v1, sum(DailyUnits) AS v2, sum(DailyMargin) AS v3 FROM bmdata100M GROUP BY Date, Customer, Brand")
print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
end <- Sys.time()
dbExecute(con, "DROP TABLE ans")
BenchmarkResults[43, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB.csv"))
rm(list = c("BenchmarkResults","end","start"))


## 100M 3N 1D 3G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB.csv"))
start <- Sys.time()
dbExecute(con, "CREATE TABLE ans AS SELECT Date, Customer, Brand, Category, sum(DailyLiters) AS v1, sum(DailyUnits) AS v2, sum(DailyMargin) AS v3 FROM bmdata100M GROUP BY Date, Customer, Brand, Category")
print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
end <- Sys.time()
dbExecute(con, "DROP TABLE ans")
BenchmarkResults[44, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB.csv"))
rm(list = c("BenchmarkResults","end","start"))


## 100M 3N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB.csv"))
start <- Sys.time()
dbExecute(con, "CREATE TABLE ans AS SELECT Date, Customer, Brand, Category, BeverageFlavor, sum(DailyLiters) AS v1, sum(DailyUnits) AS v2, sum(DailyMargin) AS v3 FROM bmdata100M GROUP BY Date, Customer, Brand, Category, BeverageFlavor")
print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
end <- Sys.time()
dbExecute(con, "DROP TABLE ans")
BenchmarkResults[45, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB.csv"))
rm(list = c("BenchmarkResults","end","start"))


###################################################################################################
###################################################################################################
###################################################################################################
###################################################################################################
###################################################################################################

# dbDisconnect(con)
# data <- data.table::fread(paste0(Path, "FakeBevData1B.csv"))
# data.table::setnames(data, c("Beverage Flavor", "Daily Liters", "Daily Margin", "Daily Revenue", "Daily Units"), c("BeverageFlavor", "DailyLiters", "DailyMargin", "DailyRevenue", "DailyUnits"))
# con = dbConnect(duckdb::duckdb(), config = list(threads = 32))
# # ncores = parallel::detectCores()
# # invisible(dbExecute(con, sprintf("PRAGMA THREADS=%d", ncores)))
# # invisible(dbExecute(con, sprintf("SET THREADS=%d", ncores)))
#
# table_name <- "bmdata1B"
# dbWriteTable(con, "bmdata1B", data, overwrite = TRUE)
# rm(data)
#
# # Aggregation 1B
#
# # Sum 1 Numeric Variable:
#
# ## 1B 1N 1D 0G
# BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB.csv"))
# start <- Sys.time()
# dbExecute(con, "CREATE TABLE ans AS SELECT Date, sum(DailyLiters) AS v1 FROM bmdata1B GROUP BY Date")
# print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
# end <- Sys.time()
# dbExecute(con, "DROP TABLE ans")
# BenchmarkResults[46, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
# data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB.csv"))
# rm(list = c("BenchmarkResults","end","start"))
#
#
# ## 1B 1N 1D 1G
# BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB.csv"))
# start <- Sys.time()
# dbExecute(con, "CREATE TABLE ans AS SELECT Date, Customer, sum(DailyLiters) AS v1 FROM bmdata1B GROUP BY Date, Customer")
# print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
# end <- Sys.time()
# dbExecute(con, "DROP TABLE ans")
# BenchmarkResults[47, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
# data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB.csv"))
# rm(list = c("BenchmarkResults","end","start"))
#
#
# ## 1B 1N 1D 2G
# BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB.csv"))
# start <- Sys.time()
# dbExecute(con, "CREATE TABLE ans AS SELECT Date, Customer, Brand, sum(DailyLiters) AS v1 FROM bmdata1B GROUP BY Date, Customer, Brand")
# print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
# end <- Sys.time()
# dbExecute(con, "DROP TABLE ans")
# BenchmarkResults[48, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
# data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB.csv"))
# rm(list = c("BenchmarkResults","end","start"))
#
#
# ## 1B 1N 1D 3G
# BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB.csv"))
# start <- Sys.time()
# dbExecute(con, "CREATE TABLE ans AS SELECT Date, Customer, Brand, Category, sum(DailyLiters) AS v1 FROM bmdata1B GROUP BY Date, Customer, Brand, Category")
# print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
# end <- Sys.time()
# dbExecute(con, "DROP TABLE ans")
# BenchmarkResults[49, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
# data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB.csv"))
# rm(list = c("BenchmarkResults","end","start"))
#
#
# ## 1B 1N 1D 4G
# BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB.csv"))
# start <- Sys.time()
# dbExecute(con, "CREATE TABLE ans AS SELECT Date, Customer, Brand, Category, BeverageFlavor, sum(DailyLiters) AS v1 FROM bmdata1B GROUP BY Date, Customer, Brand, Category, BeverageFlavor")
# print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
# end <- Sys.time()
# dbExecute(con, "DROP TABLE ans")
# BenchmarkResults[50, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
# data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB.csv"))
# rm(list = c("BenchmarkResults","end","start"))
#
#
# ## 1B 2N 1D 0G
# BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB.csv"))
# start <- Sys.time()
# dbExecute(con, "CREATE TABLE ans AS SELECT Date, sum(DailyLiters) AS v1, sum(DailyUnits) AS v2 FROM bmdata1B GROUP BY Date")
# print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
# end <- Sys.time()
# dbExecute(con, "DROP TABLE ans")
# BenchmarkResults[51, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
# data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB.csv"))
# rm(list = c("BenchmarkResults","end","start"))
#
#
# ## 1B 2N 1D 1G
# BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB.csv"))
# start <- Sys.time()
# dbExecute(con, "CREATE TABLE ans AS SELECT Date, Customer, sum(DailyLiters) AS v1, sum(DailyUnits) AS v2 FROM bmdata1B GROUP BY Date, Customer")
# print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
# end <- Sys.time()
# dbExecute(con, "DROP TABLE ans")
# BenchmarkResults[52, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
# data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB.csv"))
# rm(list = c("BenchmarkResults","end","start"))
#
#
# ## 1B 2N 1D 2G
# BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB.csv"))
# start <- Sys.time()
# dbExecute(con, "CREATE TABLE ans AS SELECT Date, Customer, Brand, sum(DailyLiters) AS v1, sum(DailyUnits) AS v2 FROM bmdata1B GROUP BY Date, Customer, Brand")
# print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
# end <- Sys.time()
# dbExecute(con, "DROP TABLE ans")
# BenchmarkResults[53, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
# data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB.csv"))
# rm(list = c("BenchmarkResults","end","start"))
#
#
# ## 1B 2N 1D 3G
# BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB.csv"))
# start <- Sys.time()
# dbExecute(con, "CREATE TABLE ans AS SELECT Date, Customer, Brand, Category, sum(DailyLiters) AS v1, sum(DailyUnits) AS v2 FROM bmdata1B GROUP BY Date, Customer, Brand, Category")
# print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
# end <- Sys.time()
# dbExecute(con, "DROP TABLE ans")
# BenchmarkResults[54, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
# data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB.csv"))
# rm(list = c("BenchmarkResults","end","start"))
#
#
# ## 1B 2N 1D 4G
# BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB.csv"))
# start <- Sys.time()
# dbExecute(con, "CREATE TABLE ans AS SELECT Date, Customer, Brand, Category, BeverageFlavor, sum(DailyLiters) AS v1, sum(DailyUnits) AS v2 FROM bmdata1B GROUP BY Date, Customer, Brand, Category, BeverageFlavor")
# print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
# end <- Sys.time()
# dbExecute(con, "DROP TABLE ans")
# BenchmarkResults[55, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
# data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB.csv"))
# rm(list = c("BenchmarkResults","end","start"))
#
#
# ## 1B 3N 1D 0G
# BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB.csv"))
# start <- Sys.time()
# dbExecute(con, "CREATE TABLE ans AS SELECT Date, sum(DailyLiters) AS v1, sum(DailyUnits) AS v2, sum(DailyMargin) AS v3 FROM bmdata1B GROUP BY Date")
# print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
# end <- Sys.time()
# dbExecute(con, "DROP TABLE ans")
# BenchmarkResults[56, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
# data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB.csv"))
# rm(list = c("BenchmarkResults","end","start"))
#
#
# ## 1B 3N 1D 1G
# BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB.csv"))
# start <- Sys.time()
# dbExecute(con, "CREATE TABLE ans AS SELECT Date, Customer, sum(DailyLiters) AS v1, sum(DailyUnits) AS v2, sum(DailyMargin) AS v3 FROM bmdata1B GROUP BY Date, Customer")
# print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
# end <- Sys.time()
# dbExecute(con, "DROP TABLE ans")
# BenchmarkResults[57, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
# data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB.csv"))
# rm(list = c("BenchmarkResults","end","start"))
#
#
# ## 1B 3N 1D 2G
# BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB.csv"))
# start <- Sys.time()
# dbExecute(con, "CREATE TABLE ans AS SELECT Date, Customer, Brand, sum(DailyLiters) AS v1, sum(DailyUnits) AS v2, sum(DailyMargin) AS v3 FROM bmdata1B GROUP BY Date, Customer, Brand")
# print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
# end <- Sys.time()
# dbExecute(con, "DROP TABLE ans")
# BenchmarkResults[58, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
# data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB.csv"))
# rm(list = c("BenchmarkResults","end","start"))
#
#
# ## 1B 3N 1D 3G
# BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB.csv"))
# start <- Sys.time()
# dbExecute(con, "CREATE TABLE ans AS SELECT Date, Customer, Brand, Category, sum(DailyLiters) AS v1, sum(DailyUnits) AS v2, sum(DailyMargin) AS v3 FROM bmdata1B GROUP BY Date, Customer, Brand, Category")
# print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
# end <- Sys.time()
# dbExecute(con, "DROP TABLE ans")
# BenchmarkResults[59, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
# data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB.csv"))
# rm(list = c("BenchmarkResults","end","start"))
#
#
# ## 1B 3N 1D 4G
# BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB.csv"))
# start <- Sys.time()
# dbExecute(con, "CREATE TABLE ans AS SELECT Date, Customer, Brand, Category, BeverageFlavor, sum(DailyLiters) AS v1, sum(DailyUnits) AS v2, sum(DailyMargin) AS v3 FROM bmdata1B GROUP BY Date, Customer, Brand, Category, BeverageFlavor")
# print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
# end <- Sys.time()
# dbExecute(con, "DROP TABLE ans")
# BenchmarkResults[60, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
# data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB.csv"))
# rm(list = c("BenchmarkResults","end","start"))
#
#
# BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB.csv"))
# BenchmarkResults[61, TimeInSeconds := BenchmarkResults[1:60, sum(TimeInSeconds)]]
# data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB.csv"))
#


