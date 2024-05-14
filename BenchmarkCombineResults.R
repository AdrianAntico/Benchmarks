Path <- "C:/Users/Bizon/Documents/GitHub/rappwd/"

# Load Benchmark Files
datatable <- data.table::fread(paste0(Path, "BenchmarkResults.csv"))
polars <- data.table::fread(paste0(Path, "BenchmarkResultsPolars.csv"))
polars <- polars[, .SD, .SDcols = c("TimeInSeconds")]
duckdb <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB.csv"))
duckdb <- duckdb[, .SD, .SDcols = c("TimeInSeconds")]
pandas <- data.table::fread(paste0(Path, "BenchmarkResultsPandas.csv"))
pandas <- pandas[, .SD, .SDcols = c("TimeInSeconds")]

# Modify Column Names for Joining
data.table::setnames(datatable, "TimeInSeconds", "data.table")
data.table::setnames(polars, "TimeInSeconds", "Polars")
data.table::setnames(duckdb, "TimeInSeconds", "DuckDB")
data.table::setnames(pandas, "TimeInSeconds", "Pandas")

# Subset columns
datatable <- datatable[, .SD, .SDcols = c("Method", "Experiment", "data.table")]

# Join data
dt <- cbind(datatable, polars, duckdb, pandas)

# Prepare data for plotting
dt <- data.table::melt.data.table(data = dt, id.vars = c("Method", "Experiment"), measure.vars = c("data.table", "Polars", "DuckDB", "Pandas"), value.name = "Time In Seconds")
dt[, `Time In Seconds` := round(`Time In Seconds`, 3)]
data.table::fwrite(dt, file = paste0(Path, "BenchmarkResultsPlot.csv"))
dt[, `Time In Seconds` := data.table::fifelse(`Time In Seconds` == -1.1, NA_real_, `Time In Seconds`)]

# Plot 1M Case
AutoPlots::Plot.Bar(
  dt = dt[c(1:15, 62:76, 123:137, 184:198)],
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
  MouseScroll = TRUE,
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
  dt = dt[c(16:30, 77:91, 138:152, 199:213)],
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
  MouseScroll = TRUE,
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
  dt = dt[c(31:45, 92:106, 153:167, 214:228)],
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
  MouseScroll = TRUE,
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

# Plot 1B Case
AutoPlots::Plot.Bar(
  dt = dt[c(46:60, 107:121, 168:183, 229:243)],
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
  Title.XAxis = NULL,
  EchartsTheme = "dark",
  MouseScroll = TRUE,
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
