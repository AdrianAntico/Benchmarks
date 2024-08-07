Path <- "C:/Users/Bizon/Documents/GitHub/rappwd/"

# Load Benchmark Files
datatable <- data.table::fread(paste0(Path, "BenchmarkResults_Cast.csv"))
polars <- data.table::fread(paste0(Path, "BenchmarkResultsPolars_Cast.csv"))
polars <- polars[, .SD, .SDcols = c("TimeInSeconds")]
duckdb <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Cast.csv"))
duckdb <- duckdb[, .SD, .SDcols = c("TimeInSeconds")]
pandas <- data.table::fread(paste0(Path, "BenchmarkResultsPandas_Cast.csv"))
pandas <- pandas[, .SD, .SDcols = c("TimeInSeconds")]
collapse <- data.table::fread(paste0(Path, "BenchmarkResultsCollapse_Cast.csv"))
collapse <- collapse[, .SD, .SDcols = c("TimeInSeconds")]

# Modify Column Names for Joining
data.table::setnames(datatable, "TimeInSeconds", "5_Datatable")
data.table::setnames(polars, "TimeInSeconds", "2_Polars")
data.table::setnames(duckdb, "TimeInSeconds", "3_DuckDB")
data.table::setnames(pandas, "TimeInSeconds", "4_Pandas")
data.table::setnames(collapse, "TimeInSeconds", "1_Collapse")

# Subset columns
datatable <- datatable[, .SD, .SDcols = c("Method", "Experiment", "5_Datatable")]

# Join data
dt <- cbind(
  datatable,
  polars,
  duckdb,
  pandas,
  collapse)

# Prepare data for plotting
dt <- data.table::melt.data.table(
  data = dt,
  id.vars = c("Method", "Experiment"),
  measure.vars = c(
    "1_Collapse",
    "5_Datatable",
    "3_DuckDB",
    "4_Pandas",
    "2_Polars"),
  value.name = "Time In Seconds")
dt[, `Time In Seconds` := round(`Time In Seconds`, 3)]
data.table::fwrite(dt, file = paste0(Path, "BenchmarkResultsPlot.csv"))
dt[, `Time In Seconds` := data.table::fifelse(`Time In Seconds` == -0.1, NA_real_, `Time In Seconds`)]
data.table::setorderv(dt, cols = "variable", -1)


temp <- data.table::copy(dt)
temp <- temp[Experiment != "Total Runtime"]
temp[, DataSize := sub(" .*", "", Experiment)]
temp <- temp[, list(`Total Run Time (secs)` = sum(`Time In Seconds`, na.rm = TRUE)), by = .(variable, DataSize)]
temp <- temp[order(DataSize, variable, `Total Run Time (secs)`)]
temp[, variable := gsub("^[^_]*_", "", variable)][]

# Add DataSize for filtering
dt[, DataSize := sub(" .*", "", Experiment)]

# Plot Totals
AutoPlots::Plot.Bar(
  dt = temp,
  PreAgg = TRUE,
  XVar = "variable",
  YVar = "Total Run Time (secs)",
  GroupVar = "DataSize",
  LabelValues = NULL,
  YVarTrans = "Identity",
  XVarTrans = "Identity",
  FacetRows = 3,
  FacetCols = 1,
  FacetLevels = NULL,
  AggMethod = "mean",
  Height = NULL,
  Width = NULL,
  Title = "Cast",
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
echarts4r::e_flip_coords(AutoPlots::Plot.Bar(
  dt = dt[DataSize == "1M"],
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
))

# Plot 10M Case
echarts4r::e_flip_coords(AutoPlots::Plot.Bar(
  dt = dt[DataSize == "10M"],
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
))

# Plot 100M Case
echarts4r::e_flip_coords(AutoPlots::Plot.Bar(
  dt = dt[DataSize == "100M"],
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
))
