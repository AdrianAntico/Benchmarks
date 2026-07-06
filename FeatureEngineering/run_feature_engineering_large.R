options(stringsAsFactors = FALSE)

library(data.table)

bench_dir <- normalizePath(file.path(getwd(), "FeatureEngineering"), mustWork = FALSE)
if (!dir.exists(bench_dir)) {
  args <- commandArgs(trailingOnly = FALSE)
  file_arg <- args[grep("--file=", args)[1L]]
  bench_dir <- normalizePath(dirname(sub("^--file=", "", file_arg)), mustWork = FALSE)
}

timestamp <- format(Sys.time(), "%Y%m%d_%H%M%S")
out_dir <- Sys.getenv("FEATURE_ENGINEERING_BENCHMARK_OUTPUT", unset = file.path(bench_dir, "outputs", paste0("large_", timestamp)))
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

result_file <- file.path(out_dir, "r_feature_engineering_large_summary.csv")
checkpoint_file <- file.path(out_dir, "r_large_checkpoint.csv")
skip_file <- file.path(out_dir, "r_large_skips_failures.csv")
config_file <- file.path(out_dir, "large_benchmark_config.csv")

env_true <- function(name, default = FALSE) {
  value <- tolower(Sys.getenv(name, unset = if (default) "true" else "false"))
  value %in% c("true", "t", "1", "yes", "y")
}

env_csv <- function(name) {
  value <- Sys.getenv(name, unset = "")
  if (!nzchar(value)) return(character())
  trimws(strsplit(value, ",", fixed = TRUE)[[1L]])
}

filter_named <- function(x, env_name) {
  keep <- env_csv(env_name)
  if (!length(keep)) return(x)
  x[intersect(names(x), keep)]
}

filter_values <- function(x, env_name) {
  keep <- env_csv(env_name)
  if (!length(keep)) return(x)
  intersect(x, keep)
}

parse_rows <- function() {
  raw <- Sys.getenv("FE_BENCH_ROWS", unset = "1000000,5000000,10000000")
  rows <- as.numeric(strsplit(raw, ",")[[1L]])
  rows <- rows[!is.na(rows)]
  if (env_true("FE_BENCH_ENABLE_25M")) rows <- unique(c(rows, 25000000))
  if (env_true("FE_BENCH_ENABLE_50M")) rows <- unique(c(rows, 50000000))
  if (env_true("FE_BENCH_ENABLE_100M")) rows <- unique(c(rows, 100000000))
  rows <- rows[rows <= 100000000]
  as.integer(sort(unique(rows)))
}

rodeo_root <- normalizePath(file.path(getwd(), "..", "Rodeo"), mustWork = FALSE)
if (!dir.exists(rodeo_root)) rodeo_root <- "C:/Users/Bizon/Documents/GitHub/Rodeo"
source(file.path(rodeo_root, "R", "FeatureEngineering_vNext.R"))

rows_grid <- parse_rows()
card_grid <- c(low = 5L, medium = 50L, high = 500L)
shape_grid <- list(
  narrow = list(numeric = 5L, categorical = 2L, date = 1L, text = 1L),
  medium = list(numeric = 20L, categorical = 5L, date = 2L, text = 3L),
  wide = list(numeric = 100L, categorical = 10L, date = 2L, text = 5L)
)
families <- c("numeric", "categorical", "calendar", "missingness", "interactions", "combined_plan", "text")
engines <- c("Rodeo vNext", "data.table set", "data.table :=", "data.table batch", "collapse", "Rodeo legacy")

card_grid <- filter_named(card_grid, "FE_BENCH_CARDINALITY")
shape_grid <- filter_named(shape_grid, "FE_BENCH_SHAPES")
families <- filter_values(families, "FE_BENCH_FAMILIES")
engines <- filter_values(engines, "FE_BENCH_ENGINES")

max_ram_gb <- as.numeric(Sys.getenv("FE_BENCH_MAX_RAM_GB", "236"))
ram_fraction <- as.numeric(Sys.getenv("FE_BENCH_RAM_FRACTION", "0.6"))
memory_cap_gb <- max_ram_gb * ram_fraction
max_generated_cells <- as.numeric(Sys.getenv("FE_BENCH_MAX_GENERATED_CELLS", "250000000"))
r_max_rows <- as.numeric(Sys.getenv("FE_BENCH_R_MAX_ROWS", "50000000"))
allow_100m <- env_true("FE_BENCH_ENABLE_100M", FALSE)
resume <- env_true("FE_BENCH_RESUME", TRUE)
repeats <- max(1L, as.integer(Sys.getenv("FE_BENCH_REPEATS", "1")))

if (!allow_100m) rows_grid <- rows_grid[rows_grid < 100000000]
if (!length(rows_grid)) stop("No row scales selected after guardrails.", call. = FALSE)
if (!length(card_grid)) stop("No cardinality levels selected. Check FE_BENCH_CARDINALITY.", call. = FALSE)
if (!length(shape_grid)) stop("No shapes selected. Check FE_BENCH_SHAPES.", call. = FALSE)
if (!length(families)) stop("No feature families selected. Check FE_BENCH_FAMILIES.", call. = FALSE)
if (!length(engines)) stop("No engines selected. Check FE_BENCH_ENGINES.", call. = FALSE)

write_config <- function() {
  cfg <- data.table(
    setting = c(
      "rows_grid", "max_ram_gb", "ram_fraction", "memory_cap_gb", "max_generated_cells",
      "r_max_rows", "enable_25m", "enable_50m", "enable_100m", "repeats", "resume",
      "shapes", "cardinality", "families", "engines"
    ),
    value = c(
      paste(rows_grid, collapse = ","),
      max_ram_gb,
      ram_fraction,
      memory_cap_gb,
      max_generated_cells,
      r_max_rows,
      env_true("FE_BENCH_ENABLE_25M"),
      env_true("FE_BENCH_ENABLE_50M"),
      allow_100m,
      repeats,
      resume,
      paste(names(shape_grid), collapse = ","),
      paste(names(card_grid), collapse = ","),
      paste(families, collapse = ","),
      paste(engines, collapse = ",")
    )
  )
  fwrite(cfg, config_file)
}
write_config()

append_row <- function(row, file = result_file) {
  fwrite(row, file, append = file.exists(file), col.names = !file.exists(file))
}

append_skip_failure <- function(row) {
  fwrite(row, skip_file, append = file.exists(skip_file), col.names = !file.exists(skip_file))
}

case_id <- function(engine, family, rows, shape, cardinality, repeat_id) {
  paste(engine, family, rows, shape, cardinality, repeat_id, sep = "|")
}

completed_cases <- function() {
  if (!resume || !file.exists(result_file)) return(character())
  x <- fread(result_file, showProgress = FALSE)
  if (!nrow(x)) character() else x$case_id
}

feature_estimate <- function(family, shape, cardinality) {
  top_n <- min(cardinality, 10L)
  switch(
    family,
    numeric = shape$numeric * 4L,
    categorical = shape$categorical * (top_n + 2L),
    calendar = shape$date * 5L,
    missingness = shape$numeric + shape$categorical + shape$text,
    interactions = min(20L, max(1L, shape$numeric - 1L) + top_n + shape$categorical),
    combined_plan = shape$numeric * 4L + shape$categorical * (top_n + 2L) + shape$date * 5L + shape$text * 4L +
      min(20L, max(1L, shape$numeric - 1L) + top_n + shape$categorical) + shape$numeric + shape$categorical + shape$text,
    text = shape$text * 4L,
    0L
  )
}

estimate_memory_gb <- function(n, shape, family, generated_features) {
  n_num <- as.numeric(n)
  generated_num <- as.numeric(generated_features)
  input_cols <- shape$numeric + shape$categorical + shape$date + shape$text + 1L
  input_bytes <- n_num * (
    shape$numeric * 8 +
      shape$categorical * 16 +
      shape$date * 8 +
      shape$text * 32 +
      8
  )
  generated_bytes <- n_num * generated_num * 8
  overhead <- 2.5
  (input_bytes + generated_bytes) * overhead / 1024^3
}

make_data <- function(n, shape, cardinality) {
  set.seed(5000L + n + cardinality + shape$numeric)
  dt <- data.table(id = seq_len(n))
  for (j in seq_len(shape$numeric)) set(dt, j = paste0("num", j), value = runif(n, 0, 100) + j)
  levels <- paste0("L", seq_len(cardinality))
  for (j in seq_len(shape$categorical)) set(dt, j = paste0("cat", j), value = sample(levels, n, TRUE))
  for (j in seq_len(shape$date)) set(dt, j = paste0("date", j), value = as.Date("2024-01-01") + sample(0:730, n, TRUE))
  text_values <- c("Hello WORLD", "two words", "ABC123!", "small text sample", "", NA_character_)
  for (j in seq_len(shape$text)) set(dt, j = paste0("text", j), value = sample(text_values, n, TRUE))
  dt
}

make_plan <- function(family, shape, cardinality) {
  num_cols <- paste0("num", seq_len(shape$numeric))
  cat_cols <- paste0("cat", seq_len(shape$categorical))
  date_cols <- paste0("date", seq_len(shape$date))
  text_cols <- paste0("text", seq_len(shape$text))
  top_n <- min(cardinality, 10L)
  if (family == "numeric") return(rodeo_feature_plan(numeric = list(columns = num_cols, transforms = c("log1p", "sqrt", "standardize", "winsorize"), winsorize_probs = c(0.01, 0.99))))
  if (family == "categorical") return(rodeo_feature_plan(categorical = list(columns = cat_cols, top_n = top_n, rare_level = "__RARE__", unseen_level = "__UNSEEN__", one_hot = TRUE, keep_original = TRUE)))
  if (family == "calendar") return(rodeo_feature_plan(calendar = list(columns = date_cols, features = c("year", "month", "wday", "quarter", "is_weekend"))))
  if (family == "missingness") return(rodeo_feature_plan(missingness = list(columns = c(num_cols, cat_cols, text_cols), suffix = "_is_missing")))
  if (family == "text") return(rodeo_feature_plan(text = list(columns = text_cols, features = c("char_count", "word_count", "digit_count", "blank"))))
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

batch_assign <- function(dt, column_names, values) {
  if (!length(column_names)) return(invisible(dt))
  data.table::setalloccol(dt, ncol(dt) + length(column_names))
  dt[, (column_names) := values]
  invisible(dt)
}

run_direct_set <- function(dt, family, shape, cardinality) {
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
  } else if (family == "missingness") {
    for (col in c(num_cols, cat_cols)) set(out, j = paste0(col, "_is_missing"), value = as.integer(is.na(out[[col]])))
  } else {
    out <- rodeo_fit_transform_feature_plan(dt, make_plan(family, shape, cardinality))$engineered_data
  }
  out
}

run_direct_assign <- function(dt, family, shape, cardinality) {
  out <- copy(dt)
  num_cols <- paste0("num", seq_len(shape$numeric))
  if (family == "numeric") {
    for (col in num_cols) {
      out[, paste0(col, "_log1p") := log1p(get(col))]
      out[, paste0(col, "_sqrt") := sqrt(get(col))]
      out[, paste0(col, "_standardize") := (get(col) - mean(get(col))) / sd(get(col))]
    }
  } else {
    out <- run_direct_set(dt, family, shape, cardinality)
  }
  out
}

run_batch <- function(dt, family, shape, cardinality) {
  out <- copy(dt)
  num_cols <- paste0("num", seq_len(shape$numeric))
  cat_cols <- paste0("cat", seq_len(shape$categorical))
  names_to_add <- character()
  values <- list()
  if (family == "numeric") {
    for (col in num_cols) {
      x <- out[[col]]
      names_to_add <- c(names_to_add, paste0(col, c("_log1p", "_sqrt", "_standardize", "_winsorize")))
      values <- c(values, list(log1p(x), sqrt(x), (x - mean(x)) / sd(x), pmin(pmax(x, quantile(x, .01)), quantile(x, .99))))
    }
    batch_assign(out, names_to_add, values)
    return(out)
  }
  if (family == "categorical") {
    for (col in cat_cols) {
      levels <- names(sort(table(out[[col]]), decreasing = TRUE))[seq_len(min(cardinality, 10L))]
      for (lvl in levels) {
        names_to_add <- c(names_to_add, paste0(col, "__", make.names(lvl)))
        values[[length(values) + 1L]] <- as.integer(out[[col]] == lvl)
      }
    }
    batch_assign(out, names_to_add, values)
    return(out)
  }
  run_direct_set(dt, family, shape, cardinality)
}

run_legacy <- function(dt, family, shape, cardinality) {
  if (!exists("Standardize")) stop("Legacy Rodeo functions not loaded")
  num_cols <- paste0("num", seq_len(shape$numeric))
  cat_cols <- paste0("cat", seq_len(shape$categorical))
  date_cols <- paste0("date", seq_len(shape$date))
  if (family == "numeric") return(Standardize(copy(dt), ColNames = num_cols, ScoreTable = FALSE))
  if (family == "categorical") return(DummifyDT(copy(dt), cols = cat_cols, TopN = rep(min(cardinality, 10L), length(cat_cols)), KeepFactorCols = TRUE, OneHot = TRUE, ReturnFactorLevels = FALSE))
  if (family == "calendar") return(CreateCalendarVariables(copy(dt), DateCols = date_cols, AsFactor = FALSE, TimeUnits = c("wday", "month", "quarter", "year")))
  stop("No comparable legacy function")
}

legacy_loaded <- FALSE
try({
  source(file.path(rodeo_root, "R", "FeatureEngineering_NumericTypes.R"))
  source(file.path(rodeo_root, "R", "FeatureEngineering_CharacterTypes.R"))
  source(file.path(rodeo_root, "R", "FeatureEngineering_CalendarTypes.R"))
  legacy_loaded <- TRUE
}, silent = TRUE)

record_case <- function(engine, family, dt, shape_name, card_name, cardinality, repeat_id, expression, generated_features, predicted_memory_gb) {
  gc()
  before <- Sys.time()
  status <- "success"
  error <- ""
  output_columns <- NA_integer_
  output_mb <- NA_real_
  tryCatch({
    result <- force(expression)
    if (is.list(result) && !is.null(result$engineered_data)) result <- result$engineered_data
    output_columns <- ncol(result)
    output_mb <- as.numeric(object.size(result)) / 1024^2
    rm(result)
  }, error = function(e) {
    status <<- "error"
    error <<- conditionMessage(e)
  })
  elapsed <- as.numeric(difftime(Sys.time(), before, units = "secs"))
  row <- data.table(
    case_id = case_id(engine, family, nrow(dt), shape_name, card_name, repeat_id),
    engine = engine,
    family = family,
    rows = nrow(dt),
    cardinality = card_name,
    cardinality_levels = cardinality,
    shape = shape_name,
    repeat_id = repeat_id,
    generated_features = generated_features,
    predicted_memory_gb = round(predicted_memory_gb, 3),
    memory_cap_gb = round(memory_cap_gb, 3),
    output_columns = output_columns,
    output_mb = round(output_mb, 3),
    elapsed_seconds = round(elapsed, 6),
    status = status,
    error = error
  )
  append_row(row)
  if (status != "success") append_skip_failure(row)
  fwrite(row[, .(case_id, status, error, completed_at = Sys.time())], checkpoint_file, append = file.exists(checkpoint_file), col.names = !file.exists(checkpoint_file))
  invisible(row)
}

skip_case <- function(engine, family, n, shape_name, card_name, cardinality, repeat_id, generated_features, predicted_memory_gb, reason) {
  row <- data.table(
    case_id = case_id(engine, family, n, shape_name, card_name, repeat_id),
    engine = engine,
    family = family,
    rows = n,
    cardinality = card_name,
    cardinality_levels = cardinality,
    shape = shape_name,
    repeat_id = repeat_id,
    generated_features = generated_features,
    predicted_memory_gb = round(predicted_memory_gb, 3),
    memory_cap_gb = round(memory_cap_gb, 3),
    output_columns = NA_integer_,
    output_mb = NA_real_,
    elapsed_seconds = NA_real_,
    status = "skipped",
    error = reason
  )
  append_row(row)
  append_skip_failure(row)
  invisible(row)
}

done <- completed_cases()
for (n in rows_grid) {
  for (shape_name in names(shape_grid)) {
    shape <- shape_grid[[shape_name]]
    for (card_name in names(card_grid)) {
      cardinality <- card_grid[[card_name]]
      for (family in families) {
        generated <- feature_estimate(family, shape, cardinality)
        predicted_gb <- estimate_memory_gb(n, shape, family, generated)
        for (engine in engines) {
          for (repeat_id in seq_len(repeats)) {
            id <- case_id(engine, family, n, shape_name, card_name, repeat_id)
            if (id %in% done) next
            if (n == 100000000 && !allow_100m) {
              skip_case(engine, family, n, shape_name, card_name, cardinality, repeat_id, generated, predicted_gb, "user_disabled_100m")
              next
            }
            if (n > r_max_rows) {
              skip_case(engine, family, n, shape_name, card_name, cardinality, repeat_id, generated, predicted_gb, "engine_row_limit")
              next
            }
            if (as.numeric(n) * as.numeric(generated) > max_generated_cells) {
              skip_case(engine, family, n, shape_name, card_name, cardinality, repeat_id, generated, predicted_gb, "generated_cell_guardrail")
              next
            }
            if (predicted_gb > memory_cap_gb) {
              skip_case(engine, family, n, shape_name, card_name, cardinality, repeat_id, generated, predicted_gb, "predicted_memory_exceeds_cap")
              next
            }
            if (family == "text" && n > 1000000) {
              skip_case(engine, family, n, shape_name, card_name, cardinality, repeat_id, generated, predicted_gb, "text_large_disabled_by_default")
              next
            }
            if (engine == "collapse" && !(requireNamespace("collapse", quietly = TRUE) && family == "numeric")) {
              skip_case(engine, family, n, shape_name, card_name, cardinality, repeat_id, generated, predicted_gb, "dependency_or_family_not_supported")
              next
            }
            if (engine == "Rodeo legacy" && !(legacy_loaded && family %in% c("numeric", "categorical", "calendar"))) {
              skip_case(engine, family, n, shape_name, card_name, cardinality, repeat_id, generated, predicted_gb, "legacy_not_comparable")
              next
            }
            dt <- make_data(n, shape, cardinality)
            expr <- switch(
              engine,
              "Rodeo vNext" = rodeo_fit_transform_feature_plan(dt, make_plan(family, shape, cardinality)),
              "data.table set" = run_direct_set(dt, family, shape, cardinality),
              "data.table :=" = run_direct_assign(dt, family, shape, cardinality),
              "data.table batch" = run_batch(dt, family, shape, cardinality),
              "collapse" = {
                out <- copy(dt)
                for (col in paste0("num", seq_len(shape$numeric))) set(out, j = paste0(col, "_centered"), value = out[[col]] - collapse::fmean(out[[col]]))
                out
              },
              "Rodeo legacy" = run_legacy(dt, family, shape, cardinality)
            )
            record_case(engine, family, dt, shape_name, card_name, cardinality, repeat_id, expr, generated, predicted_gb)
            rm(dt, expr)
            gc()
          }
        }
      }
    }
  }
}

writeLines(capture.output(sessionInfo()), file.path(out_dir, "session_info.txt"))
writeLines(c(
  paste("timestamp:", timestamp),
  paste("rows_grid:", paste(rows_grid, collapse = ",")),
  paste("memory_cap_gb:", round(memory_cap_gb, 3)),
  paste("max_generated_cells:", max_generated_cells),
  paste("r_max_rows:", r_max_rows),
  paste("repeats:", repeats),
  paste("machine:", Sys.info()[["nodename"]]),
  paste("system:", paste(Sys.info(), collapse = " | "))
), file.path(out_dir, "system_info.txt"))

cat(normalizePath(out_dir, winslash = "/"), "\n")
