# Set path for data
Path <- "C:/Users/Bizon/Documents/GitHub/rappwd/"

# Prepare base data
library(data.table)
data <- fread(paste0(Path, "FakeBevData.csv"))
data[, .N, by = 'Customer']
data <- data[, .SD, .SDcols = c("Date","Customer", "Brand", "Category", "Beverage Flavor", "Daily Liters", "Daily Margin", "Daily Units", "Daily Revenue")]

# dt <- copy(data)
randomizer <- function(dt, Num) {
  print(i)
  dt1 <- dt[Customer == "Location 1"]
  dt1[, Customer := paste0("Location ", Num)]
  dt1[, `Daily Liters` := (0.5 + runif(.N)) * `Daily Liters`]
  dt1[, `Daily Margin` := (0.5 + runif(.N)) * `Daily Margin`]
  dt1[, `Daily Units` := (0.5 + runif(.N)) * `Daily Units`]
  dt1[, `Daily Revenue` := (0.5 + runif(.N)) * `Daily Revenue`]
  dt1
}

# Time script
start <- Sys.time()

# 1M Rows
dt <- copy(data)
dtlist <- list()
for(i in 51:99) {
  dtlist[[i-50]] <- randomizer(data, i)
}

# 1M Rows Save Data
dt <- rbindlist(dtlist)
data = rbindlist(list(data, dt))
fwrite(data, paste0(Path, "FakeBevData1M.csv"))

# 10M Rows
dt <- copy(data)
dtlist <- list()
for(i in 100:1071) {
  dtlist[[i-99]] <- randomizer(data, i)
}

# 10M Rows Save Data
dt <- rbindlist(dtlist)
data = rbindlist(list(data, dt))
fwrite(data, paste0(Path, "FakeBevData10M.csv"))

# 100M Rows
dt <- copy(data)
dtlist <- list()
for(i in 1072:10793) {
  dtlist[[i-1071]] <- randomizer(data, i)
}

# # 100M Rows Save Data
dt <- rbindlist(dtlist)
data = rbindlist(list(data, dt))
fwrite(data, paste0(Path, "FakeBevData100M.csv"))

# 1B Rows
dt <- copy(data)
dtlist <- list()
for(i in 10794:108017) {
  dtlist[[i-10793]] <- randomizer(data, i)
}

# # 1B Rows Save Data
dt <- rbindlist(dtlist)
data = rbindlist(list(data, dt))
fwrite(data, paste0(Path, "FakeBevData1B.csv"))

# Timing script
end <- Sys.time()
difftime(end,start)
