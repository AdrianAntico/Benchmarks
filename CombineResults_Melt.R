Path <- "C:/Users/Bizon/Documents/GitHub/rappwd/"

# Load Benchmark Files
datatable <- data.table::fread(paste0(Path, "BenchmarkResults_Melt.csv"))
polars <- data.table::fread(paste0(Path, "BenchmarkResultsPolars_Melt.csv"))
polars <- polars[, .SD, .SDcols = c("TimeInSeconds")]
# duckdb <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Melt.csv"))
# duckdb <- duckdb[, .SD, .SDcols = c("TimeInSeconds")]
# pandas <- data.table::fread(paste0(Path, "BenchmarkResultsPandas_Melt.csv"))
# pandas <- pandas[, .SD, .SDcols = c("TimeInSeconds")]
collapse <- data.table::fread(paste0(Path, "BenchmarkResultsCollapse_Melt.csv"))
collapse <- collapse[, .SD, .SDcols = c("TimeInSeconds")]

# Modify Column Names for Joining
data.table::setnames(datatable, "TimeInSeconds", "1_Datatable")
data.table::setnames(polars, "TimeInSeconds", "3_Polars")
# data.table::setnames(duckdb, "TimeInSeconds", "4_DuckDB")
# data.table::setnames(pandas, "TimeInSeconds", "5_Pandas")
data.table::setnames(collapse, "TimeInSeconds", "2_Collapse")

# Subset columns
datatable <- datatable[, .SD, .SDcols = c("Method", "Experiment", "1_Datatable")]

# Join data
dt <- cbind(
  datatable,
  polars,
  # duckdb,
  # pandas,
  collapse)

# Prepare data for plotting
dt <- data.table::melt.data.table(
  data = dt,
  id.vars = c("Method", "Experiment"),
  measure.vars = c(
    "1_Datatable",
    "3_Polars",
    # "4_DuckDB",
    # "5_Pandas",
    "2_Collapse"),
  value.name = "Time In Seconds")
dt[, `Time In Seconds` := round(`Time In Seconds`, 3)]
data.table::fwrite(dt, file = paste0(Path, "BenchmarkResultsPlot.csv"))
dt[, `Time In Seconds` := data.table::fifelse(`Time In Seconds` == -0.1, NA_real_, `Time In Seconds`)]
data.table::setorderv(dt, cols = "variable", -1)

# Plot 1M Case
AutoPlots::Plot.Bar(
  dt = dt[c(1:15, 47:61, 93:107)],
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
  Title.XAxis = NULL,
  EchartsTheme = "dark",
  MouseScroll = FALSE,
  TimeLine = TRUE,
  TextColor = "white",
  title.fontSize = 22,
  title.fontWeight = "bold",
  title.textShadowColor = "#63aeff",
  title.textShadowBlur = 5,
  title.textShadowOffsetY = 1,
  title.textShadowOffsetX = -1,
  xaxis.fontSize = 14,
  yaxis.fontSize = 14,
  xaxis.rotate = 0,
  yaxis.rotate = 0,
  ContainLabel = TRUE,
  Debug = FALSE
)

# Plot 10M Case
AutoPlots::Plot.Bar(
  dt = dt[c(16:30, 62:76, 108:122)],
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
  Title.XAxis = NULL,
  EchartsTheme = "dark",
  MouseScroll = FALSE,
  TimeLine = TRUE,
  TextColor = "white",
  title.fontSize = 22,
  title.fontWeight = "bold",
  title.textShadowColor = "#63aeff",
  title.textShadowBlur = 5,
  title.textShadowOffsetY = 1,
  title.textShadowOffsetX = -1,
  xaxis.fontSize = 14,
  yaxis.fontSize = 14,
  xaxis.rotate = 0,
  yaxis.rotate = 0,
  ContainLabel = TRUE,
  Debug = FALSE
)

# Plot 100M Case
AutoPlots::Plot.Bar(
  dt = dt[c(31:45, 77:91, 123:137)],
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
  Title.XAxis = NULL,
  EchartsTheme = "dark",
  MouseScroll = FALSE,
  TimeLine = TRUE,
  TextColor = "white",
  title.fontSize = 22,
  title.fontWeight = "bold",
  title.textShadowColor = "#63aeff",
  title.textShadowBlur = 5,
  title.textShadowOffsetY = 1,
  title.textShadowOffsetX = -1,
  xaxis.fontSize = 14,
  yaxis.fontSize = 14,
  xaxis.rotate = 0,
  yaxis.rotate = 0,
  ContainLabel = TRUE,
  Debug = FALSE
)
