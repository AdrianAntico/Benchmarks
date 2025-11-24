# Path to data storage
Path <- "C:/Users/Bizon/Documents/GitHub/rappwd/"

# Create results table
BenchmarkResults <- data.table::data.table(
  Framework = 'duckdb',
  Method = 'melt',
  Experiment = c(
    "1M 4N 1D 4G",
    "10M 4N 1D 4G",
    "100M 4N 1D 4G"
  ),

  TimeInSeconds = c(rep(-0.1, 3))
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
BenchmarkResults[1, TimeInSeconds := median(rts)]
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
BenchmarkResults[2, TimeInSeconds := median(rts)]
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
BenchmarkResults[3, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Melt.csv"))
rm(list = c("BenchmarkResults","data","end","start"))
gc()

DBI::dbDisconnect(con)
