datatable <- data.table::fread("C:/Users/Bizon/Documents/GitHub/rappwd/BenchmarkResults.csv")
polars <- data.table::fread("C:/Users/Bizon/Documents/GitHub/rappwd/BenchmarkResultsPolars.csv")
polars <- polars[, .SD, .SDcols = c("TimeInSeconds")]
duckdb <- data.table::fread("C:/Users/Bizon/Documents/GitHub/rappwd/BenchmarkResultsDuckDB.csv")
duckdb <- duckdb[, .SD, .SDcols = c("TimeInSeconds")]
data.table::setnames(datatable, "TimeInSeconds", "data.table")
data.table::setnames(polars, "TimeInSeconds", "Polars")
data.table::setnames(duckdb, "TimeInSeconds", "DuckDB")
datatable <- datatable[, .SD, .SDcols = c("Method", "Experiment", "data.table")]
dt <- cbind(datatable, polars, duckdb)

dt <- data.table::melt.data.table(data = dt, id.vars = c("Method", "Experiment"), measure.vars = c("data.table", "Polars", "DuckDB"), value.name = "Time In Seconds")
data.table::fwrite(dt, file = "C:/Users/Bizon/Documents/GitHub/rappwd/BenchmarkResultsPlot.csv")

# 1M
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
  ShowLabels = FALSE,
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

# 10M
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
  ShowLabels = FALSE,
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

# 100M
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
  ShowLabels = FALSE,
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
