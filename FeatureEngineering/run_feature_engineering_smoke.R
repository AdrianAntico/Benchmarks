options(stringsAsFactors = FALSE)

bench_dir <- normalizePath(file.path(getwd(), "FeatureEngineering"), mustWork = FALSE)
if (!dir.exists(bench_dir)) {
  bench_dir <- normalizePath(dirname(commandArgs(trailingOnly = FALSE)[grep("--file=", commandArgs(trailingOnly = FALSE))[1L]]), mustWork = FALSE)
}

timestamp <- format(Sys.time(), "%Y%m%d_%H%M%S")
out_dir <- Sys.getenv("FEATURE_ENGINEERING_BENCHMARK_OUTPUT", unset = file.path(bench_dir, "outputs", timestamp))
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

rodeo_root <- normalizePath(file.path(getwd(), "..", "Rodeo"), mustWork = FALSE)
if (!dir.exists(rodeo_root)) {
  rodeo_root <- "C:/Users/Bizon/Documents/GitHub/Rodeo"
}

library(data.table)
source(file.path(rodeo_root, "R", "FeatureEngineering_vNext.R"))
source(file.path(rodeo_root, "R", "ModelPrep_vNext.R"))

make_data <- function(n = 10000L) {
  set.seed(42L)
  data.table(
    id = seq_len(n),
    x = runif(n, 0, 100),
    y = rnorm(n, 50, 10),
    cat = sample(c("A", "B", "C", "D", "E"), n, TRUE),
    cat2 = sample(c("K", "L", "M"), n, TRUE),
    target = sample(c("no", "yes"), n, TRUE),
    group = sample(paste0("g", 1:200), n, TRUE),
    date = as.Date("2024-01-01") + sample(0:365, n, TRUE),
    text = sample(c("Hello WORLD", "two words", "ABC123!", "small", "", NA_character_), n, TRUE)
  )
}

time_it <- function(engine, family, expression) {
  gc()
  elapsed <- system.time(force(expression))[["elapsed"]]
  data.table(engine = engine, family = family, rows = nrow(data), elapsed_seconds = as.numeric(elapsed), status = "success")
}

data <- make_data()
results <- list()

plan_numeric <- rodeo_feature_plan(numeric = list(columns = c("x", "y"), transforms = c("log1p", "sqrt", "standardize", "winsorize"), winsorize_probs = c(0.01, 0.99)))
results[[length(results) + 1L]] <- time_it("Rodeo vNext", "numeric", rodeo_fit_transform_feature_plan(data, plan_numeric))

results[[length(results) + 1L]] <- time_it("data.table direct", "numeric", {
  dt <- copy(data)
  set(dt, j = "x_log1p", value = log1p(dt$x))
  set(dt, j = "x_sqrt", value = sqrt(dt$x))
  set(dt, j = "x_standardize", value = (dt$x - mean(dt$x)) / sd(dt$x))
  dt
})

plan_combined <- rodeo_feature_plan(
  numeric = list(columns = c("x", "y"), transforms = c("log1p", "sqrt", "standardize", "winsorize"), winsorize_probs = c(0.01, 0.99)),
  categorical = list(columns = "cat", top_n = 4L, rare_level = "__RARE__", unseen_level = "__UNSEEN__", one_hot = TRUE, keep_original = TRUE),
  calendar = list(columns = "date", features = c("year", "month", "wday", "quarter", "is_weekend")),
  text = list(columns = "text", features = c("char_count", "word_count", "digit_count", "blank")),
  missingness = list(columns = c("x", "cat", "text"), suffix = "_is_missing"),
  interactions = list(numeric_pairs = list(c("x", "y")), categorical_numeric = list(list(categorical = "cat", numeric = "x")), categorical_pairs = list(c("cat", "cat2")), max_features = 20L)
)
results[[length(results) + 1L]] <- time_it("Rodeo vNext", "combined_plan", rodeo_fit_transform_feature_plan(data, plan_combined))

partition_plan <- rodeo_partition_plan(
  method = "stratified",
  fractions = c(train = 0.7, validation = 0.1, test = 0.2),
  target_col = "target",
  seed = 42L,
  k = 5L
)
results[[length(results) + 1L]] <- time_it("Rodeo vNext", "model_prep", {
  fitted <- rodeo_fit_partition_plan(data, partition_plan)
  rodeo_apply_partition_plan(data, fitted)
})

results[[length(results) + 1L]] <- time_it("data.table direct", "model_prep", {
  dt <- copy(data)
  set(dt, j = ".row_id", value = seq_len(nrow(dt)))
  dt[, .partition := sample(c("train", "validation", "test"), .N, TRUE, prob = c(0.7, 0.1, 0.2)), by = target]
  dt[, .fold_id := ((seq_len(.N) - 1L) %% 5L) + 1L]
  dt
})

r_results <- rbindlist(results, use.names = TRUE)
fwrite(r_results, file.path(out_dir, "r_feature_engineering_summary.csv"))

decision <- r_results[, .SD[which.min(elapsed_seconds)], by = family]
decision[, decision := paste(engine, "was fastest in this smoke run; confirm with moderate/overnight matrix before package thresholds.")]
fwrite(decision, file.path(out_dir, "r_decision_table.csv"))

writeLines(capture.output(sessionInfo()), file.path(out_dir, "session_info.txt"))
writeLines(c(
  paste("timestamp:", timestamp),
  paste("machine:", Sys.info()[["nodename"]]),
  paste("system:", paste(Sys.info(), collapse = " | "))
), file.path(out_dir, "system_info.txt"))

md <- c(
  "# Feature Engineering R Smoke Benchmark",
  "",
  paste("Output path:", normalizePath(out_dir, winslash = "/")),
  "",
  "## Results",
  "",
  paste(capture.output(print(r_results)), collapse = "\n"),
  "",
  "## Provisional Decision Table",
  "",
  paste(capture.output(print(decision)), collapse = "\n")
)
writeLines(md, file.path(out_dir, "feature_engineering_r_summary.md"))
cat(normalizePath(out_dir, winslash = "/"), "\n")
