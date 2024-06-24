# Set path for data
Path <- "C:/Users/Bizon/Documents/GitHub/rappwd/"

# Prepare base data
library(data.table)
data <- fread(paste0(Path, "FakeBevData.csv"))
data <- data[, lapply(.SD, sum, na.rm=TRUE), .SDcols = c("Daily Liters","Daily Units","Daily Margin","Daily Revenue"), by = c("Date","Customer","Brand","Category","Beverage Flavor")]
data[, .N, by = 'Customer'][order(-N)]

# dt <- copy(data)
randomizer <- function(dt, Num) {
  print(i)
  dt1 <- dt[Customer == "Location 23"]
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
# (1000000 - 293436) / 7320 = x
dt <- copy(data)
dtlist <- list()
for(i in 51:(147)) {
  dtlist[[i-50]] <- randomizer(data, i)
}

# 1M Rows Save Data
dt <- rbindlist(dtlist)
data = rbindlist(list(data, dt))
fwrite(data, paste0(Path, "FakeBevData1M.csv"))

# 10M Rows
# (10000000 - 1003476) / 7320 = x
dt <- copy(data)
dtlist <- list()
for(i in 148:1377) {
  dtlist[[i-147]] <- randomizer(data, i)
}

# 10M Rows Save Data
dt <- rbindlist(dtlist)
data = rbindlist(list(data, dt))
fwrite(data, paste0(Path, "FakeBevData10M.csv"))

# 100M Rows
# (100000000 - 10007076) / 7320 = x
dt <- copy(data)
dtlist <- list()
for(i in 1378:13672) {
  dtlist[[i-1071]] <- randomizer(data, i)
}

# # 100M Rows Save Data
dt <- rbindlist(dtlist)
data = rbindlist(list(data, dt))
fwrite(data, paste0(Path, "FakeBevData100M.csv"))

# 1B Rows
# (1000000000 - 100006476) / 7320 = x
dt <- copy(data)
dtlist <- list()
for(i in 13673:136622) {
  dtlist[[i-10793]] <- randomizer(data, i)
}

# # 1B Rows Save Data
dt <- rbindlist(dtlist)
data = rbindlist(list(data, dt))
fwrite(data, paste0(Path, "FakeBevData1B.csv"))

# Timing script
end <- Sys.time()
difftime(end,start)
