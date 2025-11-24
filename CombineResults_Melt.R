Path <- "C:/Users/Bizon/Documents/GitHub/rappwd/"

# Load Benchmark Files
datatable <- data.table::fread(paste0(Path, "BenchmarkResults_Melt.csv"))
polars <- data.table::fread(paste0(Path, "BenchmarkResultsPolars_Melt.csv"))
duckdb <- data.table::fread(paste0(Path, "BenchmarkResultsDuckDB_Melt.csv"))
pandas <- data.table::fread(paste0(Path, "BenchmarkResultsPandas_Melt.csv"))
pandas <- pandas[, .SD, .SDcols = names(datatable)]
collapse <- data.table::fread(paste0(Path, "BenchmarkResultsCollapse_Melt.csv"))

master <- data.table::rbindlist(list(
  collapse,
  datatable,
  polars,
  duckdb,
  pandas
))

gg <- c("1M 4N 1D 4G","10M 4N 1D 4G","100M 4N 1D 4G")
plot_list <- list()
counter = 0
for (g in gg) {
  counter = counter + 1
  plot_list[[g]] <- AutoPlots::Bar(
    dt = master[Experiment == g][order(-TimeInSeconds)][, TimeInSeconds := round(TimeInSeconds, 3)][],
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

