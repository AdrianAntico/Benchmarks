# Path to data storage
Path <- "C:/Users/Bizon/Documents/GitHub/rappwd/"

# Create results table
BenchmarkResults <- data.table::data.table(
  Framework = 'collapse',
  Method = 'filter',
  Experiment = c(
    "1M 2N 1D 4G",
    "10M 2N 1D 4G",
    "100M 2N 1D 4G",
    "1B 2N 1D 4G"
  ),

  TimeInSeconds = c(rep(-0.1, 4))
)

data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsCollapse_Filter.csv"))
rm(BenchmarkResults)

library(data.table)
library(collapse)
# setDTthreads(percent = 100)

# Filter 1M

# Sum 1 Numeric Variable:
data <- fread(paste0(Path, "FakeBevData1M.csv"))
set_collapse(nthreads = data.table::getDTthreads(), na.rm = anyNA(num_vars(data)), stable.algo = FALSE, sort = FALSE)

# Filters
CustList <- rep(paste0("Location ", seq(1, length(unique(data[["Customer"]])), 2)))
BrandList <- sort(unique(data[["Brand"]]))[c(1,3,5,9,11,13)]
CatList <- sort(unique(data[["Category"]]))[c(1,3,5)]
BevFlavList <- sort(unique(data[["Beverage Flavor"]]))[seq(1, 21, 2)]


## 1M 1N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsCollapse_Filter.csv"))
rts <- c(rep(1.1, 3))
for(i in 1:3) {
  print(i)
  start <- Sys.time()
  x <- data |> fsubset(Date > "2021-06-01" & Customer %chin% CustList & Brand %chin% BrandList & Category %chin% CatList & `Beverage Flavor` %chin% BevFlavList & `Daily Liters` > 20 & `Daily Margin` < 100)
  end <- Sys.time()
  rm(x)
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[1, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsCollapse_Filter.csv"))
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
CustList <- rep(paste0("Location ", seq(1, length(unique(data[["Customer"]])), 2)))



## 10M 1N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsCollapse_Filter.csv"))
rts <- c(rep(1.1, 3))
for(i in 1:3) {
  print(i)
  start <- Sys.time()
  x <- data |> fsubset(Date > "2021-06-01" & Customer %chin% CustList & Brand %chin% BrandList & Category %chin% CatList & `Beverage Flavor` %chin% BevFlavList & `Daily Liters` > 20 & `Daily Margin` < 100)
  end <- Sys.time()
  rm(x)
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[2, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsCollapse_Filter.csv"))
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

## 100M 1N 1D 0G
CustList <- rep(paste0("Location ", seq(1, length(unique(data[["Customer"]])), 2)))


## 100M 1N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsCollapse_Filter.csv"))
rts <- c(rep(1.1, 3))
for(i in 1:3) {
  print(i)
  start <- Sys.time()
  x <- data |> fsubset(Date > "2021-06-01" & Customer %chin% CustList & Brand %chin% BrandList & Category %chin% CatList & `Beverage Flavor` %chin% BevFlavList & `Daily Liters` > 20 & `Daily Margin` < 100)
  end <- Sys.time()
  rm(x)
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[3, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsCollapse_Filter.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()


###################################################################################################
###################################################################################################
###################################################################################################
###################################################################################################
###################################################################################################

# Filter 1B

# Sum 1 Numeric Variable:
data <- fread(paste0(Path, "FakeBevData1B.csv"))

# Filters
CustList <- rep(paste0("Location ", seq(1, length(unique(data[["Customer"]])), 2)))



## 1B 1N 1D 4G
BenchmarkResults <- data.table::fread(paste0(Path, "BenchmarkResultsCollapse_Filter.csv"))
rts <- c(rep(1.1, 3))
for(i in 1:3) {
  print(i)
  start <- Sys.time()
  x <- data |> fsubset(Date > "2021-06-01" & Customer %chin% CustList & Brand %chin% BrandList & Category %chin% CatList & `Beverage Flavor` %chin% BevFlavList & `Daily Liters` > 20 & `Daily Margin` < 100)
  end <- Sys.time()
  rm(x)
  rts[i] <- as.numeric(difftime(end, start, units = "secs"))
}
BenchmarkResults[4, TimeInSeconds := median(rts)]
data.table::fwrite(BenchmarkResults, paste0(Path, "BenchmarkResultsCollapse_Filter.csv"))
rm(list = c("BenchmarkResults","end","start"))
gc()
