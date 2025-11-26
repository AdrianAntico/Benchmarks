# Path to data storage
Path <- "C:/Users/Bizon/Documents/GitHub/rappwd/"

# Create results table
BenchmarkResults <- data.table::data.table(
  Framework = 'duckdb',
  Method = 'filter',
  Experiment = c(
    "1M 2N 1D 4G",
    "10M 2N 1D 4G",
    "100M 2N 1D 4G"
  ),

  TimeInSeconds = c(rep(-0.1, 3))
)

data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Filter.csv"))
rm(BenchmarkResults)

# Environment setup
library(duckdb)
library(DBI)
library(data.table)
data <- data.table::fread(paste0(Path, "FakeBevData1M.csv"))
data.table::setnames(data, c("Beverage Flavor", "Daily Liters", "Daily Margin", "Daily Revenue", "Daily Units"), c("BeverageFlavor", "DailyLiters", "DailyMargin", "DailyRevenue", "DailyUnits"))
con = dbConnect(duckdb::duckdb())
ncores = parallel::detectCores()
MaxMem = '243GB'
invisible(dbExecute(con, sprintf("PRAGMA THREADS=%d", ncores)))
invisible(dbExecute(con, sprintf("SET THREADS=%d", ncores)))
invisible(dbExecute(con, sprintf("SET memory_limit=%s", paste0("'", MaxMem, "'"))))
table_name <- "bmdata1M"
dbWriteTable(con, "bmdata1M", data, overwrite = TRUE)
CustList <- data.table(Customer = rep(paste0("Location ", seq(1, length(unique(data[["Customer"]])), 2))))
dbWriteTable(con, "CustList", CustList, overwrite = TRUE)
BrandList <- data.table(Brand = sort(unique(data[["Brand"]]))[c(1,3,5,9,11,13)])
dbWriteTable(con, "BrandList", BrandList, overwrite = TRUE)
CatList <- data.table(Category = sort(unique(data[["Category"]]))[c(1,3,5)])
dbWriteTable(con, "CatList", CatList, overwrite = TRUE)
BevFlavList <- data.table(BeverageFlavor = sort(unique(data[["BeverageFlavor"]]))[seq(1, 21, 2)])
dbWriteTable(con, "BevFlavList", BevFlavList, overwrite = TRUE)
rm(data)


## 1M 1N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Filter.csv"))
rts <- c(rep(1.1, 3))
for(i in 1:3) {
  print(i)
  start <- Sys.time()
  dbExecute(con, "CREATE TABLE ans AS SELECT * FROM bmdata1M
            WHERE Date > '2021-06-01'
            AND Customer IN (select * from CustList)
            AND Brand IN (select * from BrandList)
            AND Category IN (select * from CatList)
            AND BeverageFlavor IN (select * from BevFlavList)
            AND DailyLiters > 20 AND DailyMargin < 100")
  print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
  end <- Sys.time()
  dbExecute(con, "DROP TABLE ans")
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[1, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Filter.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()


###################################################################################################
###################################################################################################
###################################################################################################
###################################################################################################
###################################################################################################

# Filter 10M

# Sum 1 Numeric Variable:
data <- fread(paste0(Path, "FakeBevData10M.csv"))

# Filters
data.table::setnames(data, c("Beverage Flavor", "Daily Liters", "Daily Margin", "Daily Revenue", "Daily Units"), c("BeverageFlavor", "DailyLiters", "DailyMargin", "DailyRevenue", "DailyUnits"))
con = dbConnect(duckdb::duckdb())
ncores = parallel::detectCores()
MaxMem = '243GB'
invisible(dbExecute(con, sprintf("PRAGMA THREADS=%d", ncores)))
invisible(dbExecute(con, sprintf("SET THREADS=%d", ncores)))
invisible(dbExecute(con, sprintf("SET memory_limit=%s", paste0("'", MaxMem, "'"))))
dbWriteTable(con, "bmdata10M", data, overwrite = TRUE)
CustList <- data.table(Customer = rep(paste0("Location ", seq(1, length(unique(data[["Customer"]])), 2))))
dbWriteTable(con, "CustList", CustList, overwrite = TRUE)
BrandList <- data.table(Brand = sort(unique(data[["Brand"]]))[c(1,3,5,9,11,13)])
dbWriteTable(con, "BrandList", BrandList, overwrite = TRUE)
CatList <- data.table(Category = sort(unique(data[["Category"]]))[c(1,3,5)])
dbWriteTable(con, "CatList", CatList, overwrite = TRUE)
BevFlavList <- data.table(BeverageFlavor = sort(unique(data[["BeverageFlavor"]]))[seq(1, 21, 2)])
dbWriteTable(con, "BevFlavList", BevFlavList, overwrite = TRUE)
rm(data)


## 1M 1N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Filter.csv"))
rts <- c(rep(1.1, 3))
for(i in 1:3) {
  print(i)
  start <- Sys.time()
  dbExecute(con, "CREATE TABLE ans AS SELECT * FROM bmdata10M
            WHERE Date > '2021-06-01'
            AND Customer IN (select * from CustList)
            AND Brand IN (select * from BrandList)
            AND Category IN (select * from CatList)
            AND BeverageFlavor IN (select * from BevFlavList)
            AND DailyLiters > 20 AND DailyMargin < 100")
  print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
  end <- Sys.time()
  dbExecute(con, "DROP TABLE ans")
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[2, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Filter.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()


###################################################################################################
###################################################################################################
###################################################################################################
###################################################################################################
###################################################################################################

# Filter 100M

# Sum 1 Numeric Variable:
data <- fread(paste0(Path, "FakeBevData100M.csv"))

# Filters
data.table::setnames(data, c("Beverage Flavor", "Daily Liters", "Daily Margin", "Daily Revenue", "Daily Units"), c("BeverageFlavor", "DailyLiters", "DailyMargin", "DailyRevenue", "DailyUnits"))
con = dbConnect(duckdb::duckdb())
ncores = parallel::detectCores()
MaxMem = '243GB'
invisible(dbExecute(con, sprintf("PRAGMA THREADS=%d", ncores)))
invisible(dbExecute(con, sprintf("SET THREADS=%d", ncores)))
invisible(dbExecute(con, sprintf("SET memory_limit=%s", paste0("'", MaxMem, "'"))))
dbWriteTable(con, "bmdata100M", data, overwrite = TRUE)
CustList <- data.table(Customer = rep(paste0("Location ", seq(1, length(unique(data[["Customer"]])), 2))))
dbWriteTable(con, "CustList", CustList, overwrite = TRUE)
BrandList <- data.table(Brand = sort(unique(data[["Brand"]]))[c(1,3,5,9,11,13)])
dbWriteTable(con, "BrandList", BrandList, overwrite = TRUE)
CatList <- data.table(Category = sort(unique(data[["Category"]]))[c(1,3,5)])
dbWriteTable(con, "CatList", CatList, overwrite = TRUE)
BevFlavList <- data.table(BeverageFlavor = sort(unique(data[["BeverageFlavor"]]))[seq(1, 21, 2)])
dbWriteTable(con, "BevFlavList", BevFlavList, overwrite = TRUE)
rm(data)


## 1M 1N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Filter.csv"))
rts <- c(rep(1.1, 3))
for(i in 1:3) {
  print(i)
  start <- Sys.time()
  dbExecute(con, "CREATE TABLE ans AS SELECT * FROM bmdata100M
            WHERE Date > '2021-06-01'
            AND Customer IN (select * from CustList)
            AND Brand IN (select * from BrandList)
            AND Category IN (select * from CatList)
            AND BeverageFlavor IN (select * from BevFlavList)
            AND DailyLiters > 20 AND DailyMargin < 100")
  print(c(nr<-dbGetQuery(con, "SELECT count(*) AS cnt FROM ans")$cnt, nc<-ncol(dbGetQuery(con, "SELECT * FROM ans LIMIT 0"))))
  end <- Sys.time()
  dbExecute(con, "DROP TABLE ans")
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[3, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsDuckDB_Filter.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()
