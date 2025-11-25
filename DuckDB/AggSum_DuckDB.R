# Path to data storage
Path <- "C:/Users/Bizon/Documents/GitHub/rappwd/"

# Create Results table
BenchmarkResults <- data.table::data.table(
  Framework = 'duckdb',
  Method = 'sum aggregation',
  Experiment = c(
    "1M 3N 1D 4G",
    "10M 3N 1D 4G",
    "100M 3N 1D 4G"
  ),

  TimeInSeconds = c(rep(-0.1, 3))
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


## 1M 3N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB.csv"))
start <- Sys.time()
dbExecute(con, "CREATE TABLE ans AS SELECT Date, Customer, Brand, Category, BeverageFlavor, sum(DailyLiters) AS v1, sum(DailyUnits) AS v2, sum(DailyMargin) AS v3 FROM bmdata1M GROUP BY Date, Customer, Brand, Category, BeverageFlavor")
print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
end <- Sys.time()
dbExecute(con, "DROP TABLE ans")
BenchmarkResults[1, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
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


## 10M 3N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB.csv"))
start <- Sys.time()
dbExecute(con, "CREATE TABLE ans AS SELECT Date, Customer, Brand, Category, BeverageFlavor, sum(DailyLiters) AS v1, sum(DailyUnits) AS v2, sum(DailyMargin) AS v3 FROM bmdata10M GROUP BY Date, Customer, Brand, Category, BeverageFlavor")
print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
end <- Sys.time()
dbExecute(con, "DROP TABLE ans")
BenchmarkResults[2, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
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


## 100M 3N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB.csv"))
start <- Sys.time()
dbExecute(con, "CREATE TABLE ans AS SELECT Date, Customer, Brand, Category, BeverageFlavor, sum(DailyLiters) AS v1, sum(DailyUnits) AS v2, sum(DailyMargin) AS v3 FROM bmdata100M GROUP BY Date, Customer, Brand, Category, BeverageFlavor")
print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
end <- Sys.time()
dbExecute(con, "DROP TABLE ans")
BenchmarkResults[3, TimeInSeconds := as.numeric(difftime(end, start, units = "secs"))]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB.csv"))
rm(list = c("BenchmarkResults","end","start"))

