options(stringsAsFactors = FALSE)

library(data.table)

bench_dir <- normalizePath(file.path(getwd(), "FeatureEngineering"), mustWork = FALSE)
if (!dir.exists(bench_dir)) {
  args <- commandArgs(trailingOnly = FALSE)
  file_arg <- args[grep("--file=", args)[1L]]
  bench_dir <- normalizePath(dirname(sub("^--file=", "", file_arg)), mustWork = FALSE)
}

timestamp <- format(Sys.time(), "%Y%m%d_%H%M%S")
out_dir <- Sys.getenv("FEATURE_ENGINEERING_BENCHMARK_OUTPUT", unset = file.path(bench_dir, "outputs", paste0("rodeo_focused_", timestamp)))
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

rodeo_root <- normalizePath(file.path(getwd(), "..", "Rodeo"), mustWork = FALSE)
if (!dir.exists(rodeo_root)) rodeo_root <- "C:/Users/Bizon/Documents/GitHub/Rodeo"
source(file.path(rodeo_root, "R", "FeatureEngineering_vNext.R"))

rows_grid <- as.integer(strsplit(Sys.getenv("FE_FOCUSED_ROWS", "10000,100000,500000"), ",")[[1L]])
card_grid <- c(low = 5L, high = 500L)
shape_grid <- list(
  narrow = list(numeric = 5L, categorical = 2L, date = 1L, text = 1L),
  wide = list(numeric = 100L, categorical = 10L, date = 2L, text = 5L)
)
families <- c("numeric", "categorical", "calendar", "interactions", "combined_plan")
max_generated_cells <- as.numeric(Sys.getenv("FE_BENCH_MAX_GENERATED_CELLS", "80000000"))

batch_assign <- function(dt, column_names, values) {
  if (!length(column_names)) return(invisible(dt))
  data.table::setalloccol(dt, ncol(dt) + length(column_names))
  dt[, (column_names) := values]
  invisible(dt)
}

make_data <- function(n, shape, cardinality) {
  set.seed(2026L + n + cardinality + shape$numeric)
  dt <- data.table(id = seq_len(n))
  for (j in seq_len(shape$numeric)) set(dt, j = paste0("num", j), value = runif(n, 0, 100) + j)
  levels <- paste0("L", seq_len(cardinality))
  for (j in seq_len(shape$categorical)) set(dt, j = paste0("cat", j), value = sample(levels, n, TRUE))
  for (j in seq_len(shape$date)) set(dt, j = paste0("date", j), value = as.Date("2024-01-01") + sample(0:730, n, TRUE))
  text_values <- c("Hello WORLD", "two words", "ABC123!", "small text sample", "", NA_character_)
  for (j in seq_len(shape$text)) set(dt, j = paste0("text", j), value = sample(text_values, n, TRUE))
  dt
}

feature_estimate <- function(family, shape, cardinality) {
  top_n <- min(cardinality, 10L)
  switch(
    family,
    numeric = shape$numeric * 4L,
    categorical = shape$categorical * (top_n + 2L),
    calendar = shape$date * 5L,
    interactions = min(20L, max(1L, shape$numeric - 1L) + top_n + shape$categorical),
    combined_plan = shape$numeric * 4L + shape$categorical * (top_n + 2L) + shape$date * 5L + shape$text * 4L +
      min(20L, max(1L, shape$numeric - 1L) + top_n + shape$categorical) + shape$numeric + shape$categorical + shape$text,
    0L
  )
}

make_plan <- function(family, shape, cardinality) {
  num_cols <- paste0("num", seq_len(shape$numeric))
  cat_cols <- paste0("cat", seq_len(shape$categorical))
  date_cols <- paste0("date", seq_len(shape$date))
  text_cols <- paste0("text", seq_len(shape$text))
  top_n <- min(cardinality, 10L)
  if (family == "numeric") {
    return(rodeo_feature_plan(numeric = list(columns = num_cols, transforms = c("log1p", "sqrt", "standardize", "winsorize"), winsorize_probs = c(0.01, 0.99))))
  }
  if (family == "categorical") {
    return(rodeo_feature_plan(categorical = list(columns = cat_cols, top_n = top_n, rare_level = "__RARE__", unseen_level = "__UNSEEN__", one_hot = TRUE, keep_original = TRUE)))
  }
  if (family == "calendar") {
    return(rodeo_feature_plan(calendar = list(columns = date_cols, features = c("year", "month", "wday", "quarter", "is_weekend"))))
  }
  pairs <- Map(c, num_cols[-length(num_cols)], num_cols[-1L])
  if (family == "interactions") {
    return(rodeo_feature_plan(interactions = list(numeric_pairs = pairs, categorical_numeric = list(list(categorical = cat_cols[1L], numeric = num_cols[1L])), categorical_pairs = if (length(cat_cols) >= 2L) list(cat_cols[1:2]) else list(), max_features = 20L)))
  }
  rodeo_feature_plan(
    numeric = list(columns = num_cols, transforms = c("log1p", "sqrt", "standardize", "winsorize"), winsorize_probs = c(0.01, 0.99)),
    categorical = list(columns = cat_cols, top_n = top_n, rare_level = "__RARE__", unseen_level = "__UNSEEN__", one_hot = TRUE, keep_original = TRUE),
    calendar = list(columns = date_cols, features = c("year", "month", "wday", "quarter", "is_weekend")),
    text = list(columns = text_cols, features = c("char_count", "word_count", "digit_count", "blank")),
    missingness = list(columns = c(num_cols, cat_cols, text_cols), suffix = "_is_missing"),
    interactions = list(numeric_pairs = pairs, categorical_numeric = list(list(categorical = cat_cols[1L], numeric = num_cols[1L])), categorical_pairs = if (length(cat_cols) >= 2L) list(cat_cols[1:2]) else list(), max_features = 20L)
  )
}

direct_set <- function(dt, family, shape, cardinality) {
  out <- copy(dt)
  num_cols <- paste0("num", seq_len(shape$numeric))
  cat_cols <- paste0("cat", seq_len(shape$categorical))
  date_cols <- paste0("date", seq_len(shape$date))
  if (family == "numeric") {
    for (col in num_cols) {
      x <- out[[col]]
      set(out, j = paste0(col, "_log1p"), value = log1p(x))
      set(out, j = paste0(col, "_sqrt"), value = sqrt(x))
      set(out, j = paste0(col, "_standardize"), value = (x - mean(x)) / sd(x))
      set(out, j = paste0(col, "_winsorize"), value = pmin(pmax(x, quantile(x, .01)), quantile(x, .99)))
    }
  } else if (family == "categorical") {
    for (col in cat_cols) {
      levels <- names(sort(table(out[[col]]), decreasing = TRUE))[seq_len(min(cardinality, 10L))]
      for (lvl in levels) set(out, j = paste0(col, "__", make.names(lvl)), value = as.integer(out[[col]] == lvl))
    }
  } else if (family == "calendar") {
    for (col in date_cols) {
      d <- as.POSIXlt(out[[col]])
      set(out, j = paste0(col, "_year"), value = d$year + 1900L)
      set(out, j = paste0(col, "_month"), value = d$mon + 1L)
      set(out, j = paste0(col, "_wday"), value = d$wday + 1L)
      set(out, j = paste0(col, "_quarter"), value = d$mon %/% 3L + 1L)
      set(out, j = paste0(col, "_is_weekend"), value = as.integer((d$wday + 1L) %in% c(1L, 7L)))
    }
  } else {
    out <- rodeo_fit_transform_feature_plan(dt, make_plan(family, shape, cardinality))$engineered_data
  }
  out
}

direct_batch <- function(dt, family, shape, cardinality) {
  out <- copy(dt)
  num_cols <- paste0("num", seq_len(shape$numeric))
  cat_cols <- paste0("cat", seq_len(shape$categorical))
  date_cols <- paste0("date", seq_len(shape$date))
  names_to_add <- character()
  values <- list()
  if (family == "numeric") {
    for (col in num_cols) {
      x <- out[[col]]
      names_to_add <- c(names_to_add, paste0(col, c("_log1p", "_sqrt", "_standardize", "_winsorize")))
      values <- c(values, list(log1p(x), sqrt(x), (x - mean(x)) / sd(x), pmin(pmax(x, quantile(x, .01)), quantile(x, .99))))
    }
  } else if (family == "categorical") {
    for (col in cat_cols) {
      levels <- names(sort(table(out[[col]]), decreasing = TRUE))[seq_len(min(cardinality, 10L))]
      for (lvl in levels) {
        names_to_add <- c(names_to_add, paste0(col, "__", make.names(lvl)))
        values[[length(values) + 1L]] <- as.integer(out[[col]] == lvl)
      }
    }
  } else if (family == "calendar") {
    for (col in date_cols) {
      d <- as.POSIXlt(out[[col]])
      names_to_add <- c(names_to_add, paste0(col, c("_year", "_month", "_wday", "_quarter", "_is_weekend")))
      values <- c(values, list(d$year + 1900L, d$mon + 1L, d$wday + 1L, d$mon %/% 3L + 1L, as.integer((d$wday + 1L) %in% c(1L, 7L))))
    }
  } else {
    return(rodeo_fit_transform_feature_plan(dt, make_plan(family, shape, cardinality))$engineered_data)
  }
  batch_assign(out, names_to_add, values)
  out
}

record_case <- function(engine, family, dt, shape_name, card_name, cardinality, expression, generated_features) {
  gc()
  before <- Sys.time()
  status <- "success"
  error <- ""
  out_cols <- NA_integer_
  output_mb <- NA_real_
  tryCatch({
    result <- force(expression)
    if (is.list(result) && !is.null(result$engineered_data)) result <- result$engineered_data
    out_cols <- ncol(result)
    output_mb <- as.numeric(object.size(result)) / 1024^2
  }, error = function(e) {
    status <<- "error"
    error <<- conditionMessage(e)
  })
  data.table(
    engine = engine,
    family = family,
    rows = nrow(dt),
    cardinality = card_name,
    cardinality_levels = cardinality,
    shape = shape_name,
    generated_features = generated_features,
    output_columns = out_cols,
    output_mb = round(output_mb, 3),
    elapsed_seconds = round(as.numeric(difftime(Sys.time(), before, units = "secs")), 6),
    status = status,
    error = error
  )
}

results <- list()
for (n in rows_grid) {
  for (shape_name in names(shape_grid)) {
    shape <- shape_grid[[shape_name]]
    for (card_name in names(card_grid)) {
      cardinality <- card_grid[[card_name]]
      dt <- make_data(n, shape, cardinality)
      for (family in families) {
        generated <- feature_estimate(family, shape, cardinality)
        if (n * generated > max_generated_cells) next
        results[[length(results) + 1L]] <- record_case("Rodeo vNext optimized", family, dt, shape_name, card_name, cardinality, rodeo_fit_transform_feature_plan(dt, make_plan(family, shape, cardinality)), generated)
        results[[length(results) + 1L]] <- record_case("data.table set", family, dt, shape_name, card_name, cardinality, direct_set(dt, family, shape, cardinality), generated)
        results[[length(results) + 1L]] <- record_case("data.table batch", family, dt, shape_name, card_name, cardinality, direct_batch(dt, family, shape, cardinality), generated)
      }
    }
  }
}

summary <- rbindlist(results, fill = TRUE)
fwrite(summary, file.path(out_dir, "rodeo_vnext_focused_summary.csv"))
decision <- summary[status == "success", .SD[which.min(elapsed_seconds)], by = .(family, shape, cardinality)]
fwrite(decision, file.path(out_dir, "rodeo_vnext_focused_decision.csv"))

md <- c(
  "# Rodeo vNext Focused Benchmark",
  "",
  paste("Output path:", normalizePath(out_dir, winslash = "/")),
  paste("Rows:", nrow(summary)),
  paste("Successful rows:", nrow(summary[status == "success"])),
  paste("Failed rows:", nrow(summary[status == "error"])),
  "",
  "## Decision Table",
  "",
  paste(capture.output(print(decision)), collapse = "\n"),
  "",
  "## Engine / Family Median Seconds",
  "",
  paste(capture.output(print(summary[status == "success", .(cases = .N, median_seconds = median(elapsed_seconds)), by = .(engine, family)][order(family, median_seconds)])), collapse = "\n")
)
writeLines(md, file.path(out_dir, "rodeo_vnext_focused_summary.md"))
cat(normalizePath(out_dir, winslash = "/"), "\n")
