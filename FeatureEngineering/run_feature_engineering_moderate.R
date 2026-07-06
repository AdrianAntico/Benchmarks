options(stringsAsFactors = FALSE)

library(data.table)

bench_dir <- normalizePath(file.path(getwd(), "FeatureEngineering"), mustWork = FALSE)
if (!dir.exists(bench_dir)) {
  args <- commandArgs(trailingOnly = FALSE)
  file_arg <- args[grep("--file=", args)[1L]]
  bench_dir <- normalizePath(dirname(sub("^--file=", "", file_arg)), mustWork = FALSE)
}

timestamp <- format(Sys.time(), "%Y%m%d_%H%M%S")
out_dir <- Sys.getenv("FEATURE_ENGINEERING_BENCHMARK_OUTPUT", unset = file.path(bench_dir, "outputs", paste0("moderate_", timestamp)))
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

rodeo_root <- normalizePath(file.path(getwd(), "..", "Rodeo"), mustWork = FALSE)
if (!dir.exists(rodeo_root)) rodeo_root <- "C:/Users/Bizon/Documents/GitHub/Rodeo"
source(file.path(rodeo_root, "R", "FeatureEngineering_vNext.R"))

legacy_loaded <- FALSE
try({
  source(file.path(rodeo_root, "R", "FeatureEngineering_NumericTypes.R"))
  source(file.path(rodeo_root, "R", "FeatureEngineering_CharacterTypes.R"))
  source(file.path(rodeo_root, "R", "FeatureEngineering_CalendarTypes.R"))
  legacy_loaded <- TRUE
}, silent = TRUE)

rows_grid <- as.integer(strsplit(Sys.getenv("FE_BENCH_ROWS", "10000,100000,500000"), ",")[[1L]])
card_grid <- c(low = 5L, medium = 50L, high = 500L)
shape_grid <- list(
  narrow = list(numeric = 5L, categorical = 2L, date = 1L, text = 1L),
  medium = list(numeric = 20L, categorical = 5L, date = 2L, text = 3L),
  wide = list(numeric = 100L, categorical = 10L, date = 2L, text = 5L)
)
families <- c("numeric", "categorical", "calendar", "text", "interactions", "missingness", "combined_plan")
max_generated_cells <- as.numeric(Sys.getenv("FE_BENCH_MAX_GENERATED_CELLS", "80000000"))

make_data <- function(n, shape, cardinality) {
  set.seed(1000L + n + cardinality + shape$numeric)
  dt <- data.table(id = seq_len(n))
  for (j in seq_len(shape$numeric)) {
    set(dt, j = paste0("num", j), value = runif(n, 0, 100) + j)
  }
  levels <- paste0("L", seq_len(cardinality))
  for (j in seq_len(shape$categorical)) {
    set(dt, j = paste0("cat", j), value = sample(levels, n, TRUE))
  }
  for (j in seq_len(shape$date)) {
    set(dt, j = paste0("date", j), value = as.Date("2024-01-01") + sample(0:730, n, TRUE))
  }
  text_values <- c("Hello WORLD", "two words", "ABC123!", "small text sample", "", NA_character_)
  for (j in seq_len(shape$text)) {
    set(dt, j = paste0("text", j), value = sample(text_values, n, TRUE))
  }
  dt
}

feature_estimate <- function(family, shape, cardinality) {
  top_n <- min(cardinality, 10L)
  switch(
    family,
    numeric = shape$numeric * 4L,
    categorical = shape$categorical * (top_n + 2L),
    calendar = shape$date * 5L,
    text = shape$text * 4L,
    interactions = min(20L, max(1L, shape$numeric - 1L) + top_n + shape$categorical),
    missingness = shape$numeric + shape$categorical + shape$text,
    combined_plan = shape$numeric * 4L + shape$categorical * (top_n + 2L) + shape$date * 5L + shape$text * 4L +
      min(20L, max(1L, shape$numeric - 1L) + top_n + shape$categorical) + shape$numeric + shape$categorical + shape$text,
    0L
  )
}

make_plan <- function(family, data, shape, cardinality) {
  num_cols <- paste0("num", seq_len(shape$numeric))
  cat_cols <- paste0("cat", seq_len(shape$categorical))
  date_cols <- paste0("date", seq_len(shape$date))
  text_cols <- paste0("text", seq_len(shape$text))
  top_n <- min(cardinality, 10L)
  empty <- rodeo_feature_plan()
  if (family == "numeric") {
    return(rodeo_feature_plan(numeric = list(columns = num_cols, transforms = c("log1p", "sqrt", "standardize", "winsorize"), winsorize_probs = c(0.01, 0.99))))
  }
  if (family == "categorical") {
    return(rodeo_feature_plan(categorical = list(columns = cat_cols, top_n = top_n, rare_level = "__RARE__", unseen_level = "__UNSEEN__", one_hot = TRUE, keep_original = TRUE)))
  }
  if (family == "calendar") {
    return(rodeo_feature_plan(calendar = list(columns = date_cols, features = c("year", "month", "wday", "quarter", "is_weekend"))))
  }
  if (family == "text") {
    return(rodeo_feature_plan(text = list(columns = text_cols, features = c("char_count", "word_count", "digit_count", "blank"))))
  }
  if (family == "missingness") {
    return(rodeo_feature_plan(missingness = list(columns = c(num_cols, cat_cols, text_cols), suffix = "_is_missing")))
  }
  if (family == "interactions") {
    pairs <- Map(c, num_cols[-length(num_cols)], num_cols[-1L])
    return(rodeo_feature_plan(interactions = list(
      numeric_pairs = pairs,
      categorical_numeric = list(list(categorical = cat_cols[1L], numeric = num_cols[1L])),
      categorical_pairs = if (length(cat_cols) >= 2L) list(cat_cols[1:2]) else list(),
      max_features = 20L
    )))
  }
  if (family == "combined_plan") {
    pairs <- Map(c, num_cols[-length(num_cols)], num_cols[-1L])
    return(rodeo_feature_plan(
      numeric = list(columns = num_cols, transforms = c("log1p", "sqrt", "standardize", "winsorize"), winsorize_probs = c(0.01, 0.99)),
      categorical = list(columns = cat_cols, top_n = top_n, rare_level = "__RARE__", unseen_level = "__UNSEEN__", one_hot = TRUE, keep_original = TRUE),
      calendar = list(columns = date_cols, features = c("year", "month", "wday", "quarter", "is_weekend")),
      text = list(columns = text_cols, features = c("char_count", "word_count", "digit_count", "blank")),
      missingness = list(columns = c(num_cols, cat_cols, text_cols), suffix = "_is_missing"),
      interactions = list(numeric_pairs = pairs, categorical_numeric = list(list(categorical = cat_cols[1L], numeric = num_cols[1L])), categorical_pairs = if (length(cat_cols) >= 2L) list(cat_cols[1:2]) else list(), max_features = 20L)
    ))
  }
  empty
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
  elapsed <- as.numeric(difftime(Sys.time(), before, units = "secs"))
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
    elapsed_seconds = round(elapsed, 6),
    status = status,
    error = error
  )
}

skip_case <- function(engine, family, n, shape_name, card_name, cardinality, generated_features, reason) {
  data.table(
    engine = engine,
    family = family,
    rows = n,
    cardinality = card_name,
    cardinality_levels = cardinality,
    shape = shape_name,
    generated_features = generated_features,
    output_columns = NA_integer_,
    output_mb = NA_real_,
    elapsed_seconds = NA_real_,
    status = "skipped",
    error = reason
  )
}

run_direct_set <- function(dt, family, shape, cardinality) {
  out <- copy(dt)
  num_cols <- paste0("num", seq_len(shape$numeric))
  cat_cols <- paste0("cat", seq_len(shape$categorical))
  date_cols <- paste0("date", seq_len(shape$date))
  text_cols <- paste0("text", seq_len(shape$text))
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
  } else if (family == "text") {
    for (col in text_cols) {
      x <- as.character(out[[col]])
      x[is.na(x)] <- ""
      set(out, j = paste0(col, "_char_count"), value = nchar(x))
      set(out, j = paste0(col, "_word_count"), value = lengths(regmatches(x, gregexpr("\\S+", x))))
      set(out, j = paste0(col, "_digit_count"), value = nchar(gsub("\\D", "", x)))
      set(out, j = paste0(col, "_blank"), value = as.integer(!nzchar(trimws(x))))
    }
  } else if (family == "missingness") {
    for (col in c(num_cols, cat_cols, text_cols)) set(out, j = paste0(col, "_is_missing"), value = as.integer(is.na(out[[col]])))
  } else if (family == "interactions") {
    for (j in seq_len(min(shape$numeric - 1L, 10L))) set(out, j = paste0(num_cols[j], "_x_", num_cols[j + 1L]), value = out[[num_cols[j]]] * out[[num_cols[j + 1L]]])
    levels <- unique(out[[cat_cols[1L]]])[seq_len(min(length(unique(out[[cat_cols[1L]]])), 10L))]
    for (lvl in levels) set(out, j = paste0(cat_cols[1L], "__", make.names(lvl), "_x_", num_cols[1L]), value = as.integer(out[[cat_cols[1L]]] == lvl) * out[[num_cols[1L]]])
  } else {
    out <- rodeo_fit_transform_feature_plan(dt, make_plan("combined_plan", dt, shape, cardinality))$engineered_data
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

run_base_vectorized <- function(dt, family, shape) {
  out <- copy(dt)
  num_cols <- paste0("num", seq_len(shape$numeric))
  if (family == "numeric") {
    for (col in num_cols) out[[paste0(col, "_log1p")]] <- log1p(out[[col]])
  } else if (family == "missingness") {
    for (col in names(out)) out[[paste0(col, "_is_missing")]] <- as.integer(is.na(out[[col]]))
  } else {
    stop("base vectorized benchmark only covers numeric and missingness in this runner")
  }
  out
}

run_legacy <- function(dt, family, shape, cardinality) {
  if (!legacy_loaded) stop("legacy functions could not be sourced")
  num_cols <- paste0("num", seq_len(shape$numeric))
  cat_cols <- paste0("cat", seq_len(shape$categorical))
  date_cols <- paste0("date", seq_len(shape$date))
  if (family == "numeric") return(Standardize(copy(dt), ColNames = num_cols, ScoreTable = FALSE))
  if (family == "categorical") return(DummifyDT(copy(dt), cols = cat_cols, TopN = rep(min(cardinality, 10L), length(cat_cols)), KeepFactorCols = TRUE, OneHot = TRUE, ReturnFactorLevels = FALSE))
  if (family == "calendar") return(CreateCalendarVariables(copy(dt), DateCols = date_cols, AsFactor = FALSE, TimeUnits = c("wday", "month", "quarter", "year")))
  stop("No comparable Rodeo legacy benchmark for this family")
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
        too_large <- (n * generated) > max_generated_cells
        engines <- c("Rodeo vNext", "data.table set", "data.table :=", "base vectorized", "Rodeo legacy", "collapse")
        for (engine in engines) {
          if (too_large) {
            results[[length(results) + 1L]] <- skip_case(engine, family, n, shape_name, card_name, cardinality, generated, "generated-cell guardrail")
            next
          }
          if (engine == "base vectorized" && !family %in% c("numeric", "missingness")) {
            results[[length(results) + 1L]] <- skip_case(engine, family, n, shape_name, card_name, cardinality, generated, "base vectorized runner only covers numeric/missingness")
            next
          }
          if (engine == "Rodeo legacy" && !family %in% c("numeric", "categorical", "calendar")) {
            results[[length(results) + 1L]] <- skip_case(engine, family, n, shape_name, card_name, cardinality, generated, "no comparable legacy function in moderate runner")
            next
          }
          if (engine == "collapse" && !(requireNamespace("collapse", quietly = TRUE) && family == "numeric")) {
            results[[length(results) + 1L]] <- skip_case(engine, family, n, shape_name, card_name, cardinality, generated, "collapse benchmark only covers numeric when package is available")
            next
          }
          expr <- switch(
            engine,
            "Rodeo vNext" = rodeo_fit_transform_feature_plan(dt, make_plan(family, dt, shape, cardinality)),
            "data.table set" = run_direct_set(dt, family, shape, cardinality),
            "data.table :=" = run_direct_assign(dt, family, shape, cardinality),
            "base vectorized" = run_base_vectorized(dt, family, shape),
            "Rodeo legacy" = run_legacy(dt, family, shape, cardinality),
            "collapse" = {
              out <- copy(dt)
              for (col in paste0("num", seq_len(shape$numeric))) {
                set(out, j = paste0(col, "_centered"), value = out[[col]] - collapse::fmean(out[[col]]))
              }
              out
            }
          )
          results[[length(results) + 1L]] <- record_case(engine, family, dt, shape_name, card_name, cardinality, expr, generated)
        }
      }
    }
  }
}

summary <- rbindlist(results, fill = TRUE)
fwrite(summary, file.path(out_dir, "r_feature_engineering_moderate_summary.csv"))

writeLines(capture.output(sessionInfo()), file.path(out_dir, "session_info.txt"))
writeLines(c(
  paste("timestamp:", timestamp),
  paste("rows_grid:", paste(rows_grid, collapse = ",")),
  paste("max_generated_cells:", max_generated_cells),
  paste("machine:", Sys.info()[["nodename"]]),
  paste("system:", paste(Sys.info(), collapse = " | "))
), file.path(out_dir, "system_info.txt"))

cat(normalizePath(out_dir, winslash = "/"), "\n")
