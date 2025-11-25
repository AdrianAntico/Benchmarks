Path <- "C:/Users/Bizon/Documents/GitHub/rappwd/"

# Load Benchmark Files
datatable <- data.table::fread(paste0(Path, "BenchmarkResults_InnerJoin.csv"))
polars <- data.table::fread(paste0(Path, "BenchmarkResultsPolars_InnerJoin.csv"))
duckdb <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_InnerJoin.csv"))
pandas <- data.table::fread(paste0(Path, "BenchmarkResultsPandas_InnerJoin.csv"))
pandas <- pandas[, .SD, .SDcols = names(datatable)]
collapse <- data.table::fread(paste0(Path, "BenchmarkResultsCollapse_InnerJoin.csv"))


master <- data.table::rbindlist(list(
  collapse,
  datatable,
  polars,
  duckdb,
  pandas
))

master <- master[Experiment != "Total Runtime"]
master[, Test := data.table::tstrsplit(Experiment, " ", keep = 1)]
gg <- c("1M 3N 1D 4G","10M 3N 1D 4G","100M 3N 1D 4G")
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

