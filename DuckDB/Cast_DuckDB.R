# Path to data storage
Path <- "C:/Users/Bizon/Documents/GitHub/rappwd/"

# Create results table
BenchmarkResults <- data.table::data.table(
  Framework = 'duckdb',
  Method = 'cast',
  Experiment = c(
    "1M 4N 1D 4G",
    "10M 4N 1D 4G",
    "100M 4N 1D 4G"
  ),

  TimeInSeconds = c(rep(-0.1, 3))
)

data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Cast.csv"))
rm(BenchmarkResults)

library(data.table)
library(duckdb)
library(DBI)
data <- data.table::fread(paste0(Path, "FakeBevData1M.csv"))
data.table::setnames(data, c("Beverage Flavor", "Daily Liters", "Daily Margin", "Daily Revenue", "Daily Units"), c("BeverageFlavor", "DailyLiters", "DailyMargin", "DailyRevenue", "DailyUnits"))
temp <- data.table::melt(data = data, id.vars = c("Date","Customer","Brand","Category","BeverageFlavor"), measure.vars = c("DailyLiters","DailyUnits","DailyMargin","DailyRevenue"))
temp <- temp[, lapply(.SD, sum, na.rm = TRUE), .SDcols = c("value"), by = c("Date","Customer","Brand","Category","BeverageFlavor","variable")]
temp <- temp[Customer %chin% paste0("Location ", 1:43)]

con = dbConnect(duckdb::duckdb())
ncores = parallel::detectCores()
invisible(dbExecute(con, sprintf("PRAGMA THREADS=%d", ncores)))
invisible(dbExecute(con, sprintf("SET THREADS=%d", ncores)))
table_name <- "bmdata1M"
dbWriteTable(con, "bmdata1M", temp, overwrite = TRUE)
rm(data, temp)

# Query the schema of the table
query <- sprintf("PRAGMA table_info(%s)", table_name)
schema_info <- dbGetQuery(con, query)
rm(schema_info, ncores, query, table_name)



# Melt 1M

# Melt Numeric Variable:



## 1M 2N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Cast.csv"))
rts <- c(rep(1.1, 3))
for(i in 1:3) {
  print(i)
  start <- Sys.time()
  dbExecute(con, "CREATE TABLE ans AS
      SELECT
        DATE,
        Customer,
        Brand,
        Category,
        BeverageFlavor,
        SUM(CASE WHEN variable = 'DailyLiters' THEN value END) as DailyLiters,
        SUM(CASE WHEN variable = 'DailyUnits' THEN value END) as DailyUnits,
        SUM(CASE WHEN variable = 'DailyMargin' THEN value END) as DailyMargin,
        SUM(CASE WHEN variable = 'DailyRevenue' THEN value END) as DailyRevenue
      FROM bmdata1M,
      GROUP BY
        Date,
        Customer,
        Brand,
        Category,
        BeverageFlavor")
  print(c(
    nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt,
    nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
  end <- Sys.time()
  invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[1, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Cast.csv"))
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
temp <- data.table::melt(data = data, id.vars = c("Date","Customer","Brand","Category","BeverageFlavor"), measure.vars = c("DailyLiters","DailyUnits","DailyMargin","DailyRevenue"))
temp <- temp[, lapply(.SD, sum, na.rm = TRUE), .SDcols = c("value"), by = c("Date","Customer","Brand","Category","BeverageFlavor","variable")]
temp <- temp[Customer %chin% paste0("Location ", 1:482)]

con = dbConnect(duckdb::duckdb())
ncores = parallel::detectCores()
invisible(dbExecute(con, sprintf("PRAGMA THREADS=%d", ncores)))
invisible(dbExecute(con, sprintf("SET THREADS=%d", ncores)))
table_name <- "bmdata10M"
dbWriteTable(con, "bmdata10M", temp, overwrite = TRUE)
rm(data, temp)


## 10M 2N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Cast.csv"))
rts <- c(rep(1.1, 3))
for(i in 1:3) {
  print(i)
  start <- Sys.time()
  dbExecute(con, "CREATE TABLE ans AS
      SELECT
        DATE,
        Customer,
        Brand,
        Category,
        BeverageFlavor,
        SUM(CASE WHEN variable = 'DailyLiters' THEN value END) as DailyLiters,
        SUM(CASE WHEN variable = 'DailyUnits' THEN value END) as DailyUnits,
        SUM(CASE WHEN variable = 'DailyMargin' THEN value END) as DailyMargin,
        SUM(CASE WHEN variable = 'DailyRevenue' THEN value END) as DailyRevenue
      FROM bmdata10M,
      GROUP BY
        Date,
        Customer,
        Brand,
        Category,
        BeverageFlavor")
  print(c(
    nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt,
    nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
  end <- Sys.time()
  invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[2, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Cast.csv"))
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
temp <- data.table::melt(data = data, id.vars = c("Date","Customer","Brand","Category","BeverageFlavor"), measure.vars = c("DailyLiters","DailyUnits","DailyMargin","DailyRevenue"))
temp <- temp[, lapply(.SD, sum, na.rm = TRUE), .SDcols = c("value"), by = c("Date","Customer","Brand","Category","BeverageFlavor","variable")]
temp <- temp[Customer %chin% paste0("Location ", 1:4881)]

con = dbConnect(duckdb::duckdb())
ncores = parallel::detectCores()
invisible(dbExecute(con, sprintf("PRAGMA THREADS=%d", ncores)))
invisible(dbExecute(con, sprintf("SET THREADS=%d", ncores)))
table_name <- "bmdata100M"
dbWriteTable(con, "bmdata100M", temp, overwrite = TRUE)
rm(data, temp)

## 100M 2N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Cast.csv"))
rts <- c(rep(1.1, 3))
for(i in 1:3) {
  print(i)
  start <- Sys.time()
  dbExecute(con, "CREATE TABLE ans AS
      SELECT
        DATE,
        Customer,
        Brand,
        Category,
        BeverageFlavor,
        SUM(CASE WHEN variable = 'DailyLiters' THEN value END) as DailyLiters,
        SUM(CASE WHEN variable = 'DailyUnits' THEN value END) as DailyUnits,
        SUM(CASE WHEN variable = 'DailyMargin' THEN value END) as DailyMargin,
        SUM(CASE WHEN variable = 'DailyRevenue' THEN value END) as DailyRevenue
      FROM bmdata100M,
      GROUP BY
        Date,
        Customer,
        Brand,
        Category,
        BeverageFlavor")
  print(c(
    nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt,
    nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
  end <- Sys.time()
  invisible(dbExecute(con, "DROP TABLE IF EXISTS ans"))
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[3, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Cast.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()
