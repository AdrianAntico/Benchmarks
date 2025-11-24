Path <- "C:/Users/Bizon/Documents/GitHub/rappwd/"

# Load Benchmark Files
datatable <- data.table::fread(paste0(Path, "BenchmarkResults.csv"))
polars <- data.table::fread(paste0(Path, "BenchmarkResultsPolars.csv"))
duckdb <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB.csv"))
pandas <- data.table::fread(paste0(Path, "BenchmarkResultsPandas.csv"))
pandas <- pandas[, .SD, .SDcols = names(datatable)]
collapse <- data.table::fread(paste0(Path, "BenchmarkResultsCollapse.csv"))
master <- data.table::rbindlist(list(
  collapse,
  datatable,
  polars,
  duckdb,
  pandas
))

master <- master[Experiment != "Total Runtime"]
master[, Test := data.table::tstrsplit(Experiment, " ", keep = 1)]
gg <- c("1M 3N 1D 4G","10M 3N 1D 4G","100M 3N 1D 4G","1B 3N 1D 4G")
plot_list <- list()
counter = 0
for (g in gg) {
  counter = counter + 1
  plot_list[[g]] <- AutoPlots::Bar(
    dt = master[Experiment == g][order(-TimeInSeconds)][, TimeInSeconds := round(TimeInSeconds, 3)],
    XVar = "Framework",
    YVar = "TimeInSeconds",
    backgroundStyle.color = if (counter == 1) {
      c("blue","blue","gray")
    } else if (counter == 2) {
      c("darkred","darkred","gray")
    } else if (counter == 3) {
      c("darkgreen","darkgreen","gray")
    } else {
      c("darkorange","darkorange","gray")
    },
    backgroundStyle.opacity = c(0.8,0.7,0.6),
    ShowLabels = TRUE,
    xAxis.title = "",
    title.text = g
  ) |> echarts4r::e_flip_coords()
}

AutoPlots::display_plots_grid(
  plot_list, cols = 2
)

# Modify Column Names for Joining
data.table::setnames(datatable, "TimeInSeconds", "2_Datatable")
data.table::setnames(polars, "TimeInSeconds", "3_Polars")
data.table::setnames(duckdb, "TimeInSeconds", "4_DuckDB")
data.table::setnames(pandas, "TimeInSeconds", "5_Pandas")
data.table::setnames(collapse, "TimeInSeconds", "1_Collapse")

# Subset columns
datatable <- datatable[, .SD, .SDcols = c("Method", "Experiment", "2_Datatable")]



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
    "2_Datatable",
    "4_DuckDB",
    "5_Pandas",
    "3_Polars"),
  value.name = "Time In Seconds")
dt[, `Time In Seconds` := round(`Time In Seconds`, 3)]
data.table::fwrite(dt, file = paste0(Path, "BenchmarkResultsPlot_Melt.csv"))
dt[, `Time In Seconds` := data.table::fifelse(`Time In Seconds` == -0.1, NA_real_, `Time In Seconds`)]
data.table::setorderv(dt, cols = "variable", -1)



# Plot 1M Case
temp <- data.table::copy(dt)
temp[, DataSize := sub(" .*", "", Experiment)]
temp <- temp[, list(`Total Run Time (secs)` = sum(`Time In Seconds`, na.rm = TRUE)), by = .(variable, DataSize)]
temp <- temp[order(DataSize, variable, `Total Run Time (secs)`)]
temp[, variable := gsub("^[^_]*_", "", variable)][]
temp <- temp[DataSize != "Total"]
temp <- temp[variable %in% c("Polars", "Pandas", "DuckDB") & DataSize == "1B", `Total Run Time (secs)`:= NA_real_]


# Add DataSize for filtering
dt[, DataSize := sub(" .*", "", Experiment)]

# Totals
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
  Title = "Aggregation (sum)",
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
  title.fontSize = 40,
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
  title.fontSize = 40,
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
  title.fontSize = 40,
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

# Plot 1B Case
library(data.table)
dt = dt[variable == "5_Pandas" & Experiment %like% "1B", `Time In Seconds` := NA]
echarts4r::e_flip_coords(AutoPlots::Plot.Bar(
  dt = dt[DataSize == "1B"],
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
  yaxis.fontSize = 30,
  xaxis.rotate = 0,
  yaxis.rotate = 0,
  ContainLabel = TRUE,
  Debug = FALSE
))
