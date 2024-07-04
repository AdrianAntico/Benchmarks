Path <- "C:/Users/Bizon/Documents/GitHub/rappwd/"

# Load Benchmark Files
datatable <- data.table::fread(paste0(Path, "BenchmarkResults_Filter.csv"))
polars <- data.table::fread(paste0(Path, "BenchmarkResultsPolars_Filter.csv"))
polars <- polars[, .SD, .SDcols = c("TimeInSeconds")]
duckdb <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Filter.csv"))
duckdb <- duckdb[, .SD, .SDcols = c("TimeInSeconds")]
pandas <- data.table::fread(paste0(Path, "BenchmarkResultsPandas_Filter.csv"))
pandas <- pandas[, .SD, .SDcols = c("TimeInSeconds")]
collapse <- data.table::fread(paste0(Path, "BenchmarkResultsCollapse_Filter.csv"))
collapse <- collapse[, .SD, .SDcols = c("TimeInSeconds")]

# Modify Column Names for Joining
data.table::setnames(datatable, "TimeInSeconds", "4_Datatable")
data.table::setnames(polars, "TimeInSeconds", "1_Polars")
data.table::setnames(duckdb, "TimeInSeconds", "5_DuckDB")
data.table::setnames(pandas, "TimeInSeconds", "2_Pandas")
data.table::setnames(collapse, "TimeInSeconds", "3_Collapse")

# Subset columns
datatable <- datatable[, .SD, .SDcols = c("Method", "Experiment", "4_Datatable")]

# Join data
dt <- cbind(
  collapse,
  datatable,
  duckdb,
  polars,
  pandas)

# Prepare data for plotting
dt <- data.table::melt.data.table(
  data = dt,
  id.vars = c("Method", "Experiment"),
  measure.vars = c(
    "3_Collapse",
    "4_Datatable",
    "5_DuckDB",
    "2_Pandas",
    "1_Polars"),
  value.name = "Time In Seconds")
dt[, `Time In Seconds` := round(`Time In Seconds`, 3)]
data.table::fwrite(dt, file = paste0(Path, "BenchmarkResultsPlot_Melt.csv"))
dt[, `Time In Seconds` := data.table::fifelse(`Time In Seconds` == -0.1, NA_real_, `Time In Seconds`)]
data.table::setorderv(dt, cols = "variable", -1)


# Plot 1M Case
temp <- data.table::copy(dt)
temp[, DataSize := sub(" .*", "", Experiment)]
temp <- temp[DataSize != "Total"]
temp <- temp[, list(`Total Run Time (secs)` = sum(`Time In Seconds`, na.rm = TRUE)), by = .(variable, DataSize)]
temp <- temp[order(DataSize, variable, `Total Run Time (secs)`)]
temp[, variable := gsub("^[^_]*_", "", variable)][]
temp <- temp[variable %in% c("Polars", "Pandas", "DuckDB") & DataSize == "1B", `Total Run Time (secs)`:= NA_real_]
AutoPlots::Plot.Bar(
  dt = temp,
  PreAgg = TRUE,
  XVar = "variable",
  YVar = "Total Run Time (secs)",
  GroupVar = "DataSize",
  LabelValues = NULL,
  YVarTrans = "Identity",
  XVarTrans = "Identity",
  FacetRows = 4,
  FacetCols = 1,
  FacetLevels = NULL,
  AggMethod = "mean",
  Height = NULL,
  Width = NULL,
  Title = "Filter",
  ShowLabels = TRUE,
  Title.YAxis = NULL,
  Title.XAxis = "",
  EchartsTheme = "dark",
  MouseScroll = TRUE,
  TimeLine = TRUE,
  TextColor = "white",
  title.fontSize = 35,
  title.fontWeight = "bold",
  title.textShadowColor = "#63aeff",
  title.textShadowBlur = 5,
  title.textShadowOffsetY = 1,
  title.textShadowOffsetX = -1,
  xaxis.fontSize = 14,
  yaxis.fontSize = 30,
  xaxis.rotate = 0,
  yaxis.rotate = 0,
  ContainLabel = TRUE,
  Debug = FALSE
)


# Plot 1M Case
AutoPlots::Plot.Bar(
  dt = dt[c(1:10, 42:51, 83:92, 124:133, 165:174)],
  PreAgg = TRUE,
  XVar = "Experiment",
  YVar = "Time In Seconds",
  GroupVar = "variable",
  LabelValues = NULL,
  YVarTrans = "Identity",
  XVarTrans = "Identity",
  FacetRows = 1,
  FacetCols = 1,
  FacetLevels = NULL,
  AggMethod = "mean",
  Height = NULL,
  Width = NULL,
  Title = "1M Rows Benchmark",
  ShowLabels = TRUE,
  Title.YAxis = NULL,
  Title.XAxis = "",
  EchartsTheme = "dark",
  MouseScroll = TRUE,
  TimeLine = TRUE,
  TextColor = "white",
  title.fontSize = 40,
  title.fontWeight = "bold",
  title.textShadowColor = "#63aeff",
  title.textShadowBlur = 5,
  title.textShadowOffsetY = 1,
  title.textShadowOffsetX = -1,
  xaxis.fontSize = 14,
  yaxis.fontSize = 35,
  xaxis.rotate = 35,
  yaxis.rotate = 0,
  ContainLabel = TRUE,
  Debug = FALSE
)

# Plot 10M Case
AutoPlots::Plot.Bar(
  dt = dt[c(11:20, 52:61, 93:102, 134:143, 175:184)],
  PreAgg = TRUE,
  XVar = "Experiment",
  YVar = "Time In Seconds",
  GroupVar = "variable",
  LabelValues = NULL,
  YVarTrans = "Identity",
  XVarTrans = "Identity",
  FacetRows = 1,
  FacetCols = 1,
  FacetLevels = NULL,
  AggMethod = "mean",
  Height = NULL,
  Width = NULL,
  Title = "10M Rows Benchmark",
  ShowLabels = TRUE,
  Title.YAxis = NULL,
  Title.XAxis = "",
  EchartsTheme = "dark",
  MouseScroll = TRUE,
  TimeLine = TRUE,
  TextColor = "white",
  title.fontSize = 40,
  title.fontWeight = "bold",
  title.textShadowColor = "#63aeff",
  title.textShadowBlur = 5,
  title.textShadowOffsetY = 1,
  title.textShadowOffsetX = -1,
  xaxis.fontSize = 14,
  yaxis.fontSize = 35,
  xaxis.rotate = 35,
  yaxis.rotate = 0,
  ContainLabel = TRUE,
  Debug = FALSE
)

# Plot 100M Case
AutoPlots::Plot.Bar(
  dt = dt[c(21:30, 62:71, 103:112, 144:153, 185:194)],
  PreAgg = TRUE,
  XVar = "Experiment",
  YVar = "Time In Seconds",
  GroupVar = "variable",
  LabelValues = NULL,
  YVarTrans = "Identity",
  XVarTrans = "Identity",
  FacetRows = 1,
  FacetCols = 1,
  FacetLevels = NULL,
  AggMethod = "mean",
  Height = NULL,
  Width = NULL,
  Title = "100M Rows Benchmark",
  ShowLabels = TRUE,
  Title.YAxis = NULL,
  Title.XAxis = "",
  EchartsTheme = "dark",
  MouseScroll = TRUE,
  TimeLine = TRUE,
  TextColor = "white",
  title.fontSize = 40,
  title.fontWeight = "bold",
  title.textShadowColor = "#63aeff",
  title.textShadowBlur = 5,
  title.textShadowOffsetY = 1,
  title.textShadowOffsetX = -1,
  xaxis.fontSize = 14,
  yaxis.fontSize = 35,
  xaxis.rotate = 35,
  yaxis.rotate = 0,
  ContainLabel = TRUE,
  Debug = FALSE
)


# Plot 1B Case
library(data.table)
x <- dt[gsub("^[^_]*_", "", variable) %in% c("Polars", "Pandas", "DuckDB") & substr(Experiment,1,2) == "1B", `Time In Seconds`:= NA_real_][]
AutoPlots::Plot.Bar(
  dt = x[c(31:40, 72:81, 113:122, 154:163, 195:204)],
  PreAgg = TRUE,
  XVar = "Experiment",
  YVar = "Time In Seconds",
  GroupVar = "variable",
  LabelValues = NULL,
  YVarTrans = "Identity",
  XVarTrans = "Identity",
  FacetRows = 1,
  FacetCols = 1,
  FacetLevels = NULL,
  AggMethod = "mean",
  Height = NULL,
  Width = NULL,
  Title = "1B Rows Benchmark",
  ShowLabels = TRUE,
  Title.YAxis = NULL,
  Title.XAxis = "",
  EchartsTheme = "dark",
  MouseScroll = TRUE,
  TimeLine = TRUE,
  TextColor = "white",
  title.fontSize = 40,
  title.fontWeight = "bold",
  title.textShadowColor = "#63aeff",
  title.textShadowBlur = 5,
  title.textShadowOffsetY = 1,
  title.textShadowOffsetX = -1,
  xaxis.fontSize = 14,
  yaxis.fontSize = 35,
  xaxis.rotate = 35,
  yaxis.rotate = 0,
  ContainLabel = TRUE,
  Debug = FALSE
)
