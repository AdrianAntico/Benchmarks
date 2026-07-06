library(data.table)

bench_dir <- normalizePath(file.path(getwd(), "FeatureEngineering"), mustWork = FALSE)
if (!dir.exists(bench_dir)) {
  bench_dir <- normalizePath(dirname(normalizePath(sys.frame(1)$ofile, mustWork = FALSE)), mustWork = FALSE)
}

out_root <- Sys.getenv("FEATURE_ENGINEERING_BENCHMARK_OUTPUT", unset = file.path(bench_dir, "outputs"))
if (dir.exists(out_root) && !file.exists(file.path(out_root, "r_feature_engineering_summary.csv"))) {
  dirs <- list.dirs(out_root, recursive = FALSE, full.names = TRUE)
  if (length(dirs)) out_root <- dirs[which.max(file.info(dirs)$mtime)]
}

files <- list.files(out_root, pattern = "^(r|python)_feature_engineering_.*summary\\.csv$", full.names = TRUE)
tables <- lapply(files, fread)
summary <- if (length(tables)) rbindlist(tables, fill = TRUE) else data.table()
if (nrow(summary) && "elapsed_seconds" %in% names(summary)) {
  summary[, elapsed_seconds := suppressWarnings(as.numeric(elapsed_seconds))]
}
if (nrow(summary) && !"cardinality" %in% names(summary)) summary[, cardinality := "smoke"]
if (nrow(summary) && !"shape" %in% names(summary)) summary[, shape := "smoke"]
if (nrow(summary) && !"generated_features" %in% names(summary)) summary[, generated_features := NA_integer_]
if (nrow(summary) && !"output_columns" %in% names(summary)) summary[, output_columns := NA_integer_]
if (nrow(summary) && !"output_mb" %in% names(summary)) summary[, output_mb := NA_real_]
if (nrow(summary) && !"predicted_memory_gb" %in% names(summary)) summary[, predicted_memory_gb := NA_real_]
if (nrow(summary) && !"memory_cap_gb" %in% names(summary)) summary[, memory_cap_gb := NA_real_]
if (nrow(summary) && !"repeat_id" %in% names(summary)) summary[, repeat_id := NA_integer_]
if (nrow(summary) && !"case_id" %in% names(summary)) summary[, case_id := NA_character_]

decision <- if (nrow(summary)) {
  summary[status == "success", .SD[which.min(elapsed_seconds)], by = .(family, shape, cardinality)][
    , decision := paste(engine, "was fastest for", family, "on", shape, "shape with", cardinality, "cardinality.")]
} else {
  data.table()
}

fastest_by_family <- if (nrow(summary)) summary[status == "success", .SD[which.min(elapsed_seconds)], by = family] else data.table()
fastest_by_rows <- if (nrow(summary)) summary[status == "success", .SD[which.min(elapsed_seconds)], by = rows] else data.table()
fastest_by_cardinality <- if (nrow(summary)) summary[status == "success", .SD[which.min(elapsed_seconds)], by = cardinality] else data.table()
fastest_by_shape <- if (nrow(summary)) summary[status == "success", .SD[which.min(elapsed_seconds)], by = shape] else data.table()
status_counts <- if (nrow(summary)) summary[, .N, by = .(status, error)][order(status, error)] else data.table()
fastest_by_family_rows <- if (nrow(summary)) summary[status == "success", .SD[which.min(elapsed_seconds)], by = .(family, rows)] else data.table()
fastest_by_shape_rows <- if (nrow(summary)) summary[status == "success", .SD[which.min(elapsed_seconds)], by = .(shape, rows)] else data.table()
fastest_by_cardinality_rows <- if (nrow(summary)) summary[status == "success", .SD[which.min(elapsed_seconds)], by = .(cardinality, rows)] else data.table()
memory_skip_reasons <- if (nrow(summary)) {
  memory_reasons <- c(
    "predicted_memory_exceeds_cap",
    "engine_row_limit",
    "generated_cell_guardrail",
    "dependency_missing",
    "user_disabled_100m",
    "text_large_disabled_by_default"
  )
  summary[
    status == "skipped" & vapply(memory_reasons, function(reason) grepl(reason, error, fixed = TRUE), logical(.N)) |> rowSums() > 0,
    .N,
    by = .(error, rows, shape, cardinality)
  ][order(rows, shape, cardinality, error)]
} else data.table()

engine_family <- if (nrow(summary)) {
  summary[status == "success", .(
    cases = .N,
    median_seconds = round(stats::median(elapsed_seconds, na.rm = TRUE), 6),
    min_seconds = round(min(elapsed_seconds, na.rm = TRUE), 6),
    max_seconds = round(max(elapsed_seconds, na.rm = TRUE), 6)
  ), by = .(engine, family)][order(family, median_seconds)]
} else data.table()

safe_max <- function(x) {
  x <- x[is.finite(x)]
  if (!length(x)) NA_real_ else max(x)
}

scalability_curves <- if (nrow(summary)) {
  summary[status == "success", .(
    cases = .N,
    median_seconds = round(stats::median(elapsed_seconds, na.rm = TRUE), 6),
    min_seconds = round(min(elapsed_seconds, na.rm = TRUE), 6),
    max_seconds = round(max(elapsed_seconds, na.rm = TRUE), 6),
    median_output_mb = round(stats::median(output_mb, na.rm = TRUE), 3),
    max_predicted_memory_gb = round(safe_max(predicted_memory_gb), 3)
  ), by = .(engine, family, rows, shape, cardinality)][order(family, shape, cardinality, engine, rows)]
} else data.table()

recommendation_text <- function(row) {
  engine <- row[["engine"]]
  family <- row[["family"]]
  if (grepl("data.table", engine)) {
    paste("Rodeo internals should consider a", engine, "path for", family, "after validating behavior against vNext specs.")
  } else if (grepl("Rodeo vNext", engine)) {
    paste("Rodeo vNext orchestration is competitive for", family, "in this shape; keep plan/spec ergonomics and inspect hot loops before rewriting.")
  } else if (grepl("Polars lazy", engine)) {
    paste("PolarsFE should prefer lazy expressions for", family, "when chaining larger plans.")
  } else if (grepl("Polars eager", engine)) {
    paste("PolarsFE should prefer eager expressions for", family, "in this shape.")
  } else if (grepl("PolarsFE", engine)) {
    paste("PolarsFE vNext is competitive for", family, "as an orchestration layer; inspect direct Polars deltas before internal rewrites.")
  } else if (grepl("Rodeo legacy", engine)) {
    paste("Legacy Rodeo remains competitive for", family, "and should stay as a behavioral/performance baseline.")
  } else {
    paste("Use", engine, "as the provisional strategy for", family, "pending moderate/overnight confirmation.")
  }
}

implementation_decisions <- copy(decision)
if (nrow(implementation_decisions)) {
  implementation_decisions[, recommendation := vapply(seq_len(.N), function(i) recommendation_text(.SD[i]), character(1L))]
  implementation_decisions[, evidence := paste0("elapsed_seconds=", elapsed_seconds, "; rows=", rows, "; generated_features=", generated_features)]
}

large_data_decisions <- if (nrow(summary)) {
  summary[status == "success", .SD[which.min(elapsed_seconds)], by = .(family, rows, shape)][
    , recommendation := vapply(seq_len(.N), function(i) recommendation_text(.SD[i]), character(1L))
  ][
    , evidence := paste0("elapsed_seconds=", elapsed_seconds, "; cardinality=", cardinality, "; output_mb=", round(output_mb, 3))
  ][order(family, shape, rows)]
} else data.table()

fwrite(summary, file.path(out_root, "feature_engineering_summary_all.csv"))
fwrite(decision, file.path(out_root, "decision_table.csv"))
fwrite(fastest_by_family, file.path(out_root, "fastest_by_family.csv"))
fwrite(fastest_by_rows, file.path(out_root, "fastest_by_rows.csv"))
fwrite(fastest_by_cardinality, file.path(out_root, "fastest_by_cardinality.csv"))
fwrite(fastest_by_shape, file.path(out_root, "fastest_by_shape.csv"))
fwrite(fastest_by_family_rows, file.path(out_root, "fastest_by_family_rows.csv"))
fwrite(fastest_by_shape_rows, file.path(out_root, "fastest_by_shape_rows.csv"))
fwrite(fastest_by_cardinality_rows, file.path(out_root, "fastest_by_cardinality_rows.csv"))
fwrite(memory_skip_reasons, file.path(out_root, "memory_skip_reasons.csv"))
fwrite(scalability_curves, file.path(out_root, "scalability_curves.csv"))
fwrite(engine_family, file.path(out_root, "engine_family_medians.csv"))
fwrite(status_counts, file.path(out_root, "status_counts.csv"))
fwrite(implementation_decisions, file.path(out_root, "implementation_decision_table.csv"))
fwrite(large_data_decisions, file.path(out_root, "large_data_decision_table.csv"))

print_table <- function(x, n = 30L) {
  if (!nrow(x)) return("(no rows)")
  paste(capture.output(print(head(x, n))), collapse = "\n")
}

md <- c(
  "# Feature Engineering Benchmark Summary",
  "",
  paste("Output path:", normalizePath(out_root, winslash = "/")),
  paste("Result rows:", nrow(summary)),
  paste("Successful rows:", nrow(summary[status == "success"])),
  paste("Skipped rows:", nrow(summary[status == "skipped"])),
  paste("Failed rows:", nrow(summary[status == "error"])),
  "",
  "## Status Counts",
  "",
  print_table(status_counts),
  "",
  "## Fastest By Feature Family",
  "",
  print_table(fastest_by_family),
  "",
  "## Fastest By Data Size",
  "",
  print_table(fastest_by_rows),
  "",
  "## Fastest By Cardinality",
  "",
  print_table(fastest_by_cardinality),
  "",
  "## Fastest By Shape",
  "",
  print_table(fastest_by_shape),
  "",
  "## Fastest By Family And Rows",
  "",
  print_table(fastest_by_family_rows, 50L),
  "",
  "## Large-Data Guardrail / Memory Skips",
  "",
  print_table(memory_skip_reasons, 50L),
  "",
  "## Scalability Curves",
  "",
  print_table(scalability_curves, 50L),
  "",
  "## Engine / Family Medians",
  "",
  print_table(engine_family, 50L),
  "",
  "## Implementation Decision Table",
  "",
  print_table(implementation_decisions, 50L),
  "",
  "## Large-Data Decision Table",
  "",
  print_table(large_data_decisions, 50L),
  "",
  "## Combined Results Preview",
  "",
  print_table(summary, 50L)
)
writeLines(md, file.path(out_root, "feature_engineering_summary.md"))
cat(normalizePath(out_root, winslash = "/"), "\n")
