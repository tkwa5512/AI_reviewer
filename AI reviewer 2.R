# AI Reviewer 2
# Purpose: Extract structured data from each included paper by rendering the full
#   publisher webpage in headless Chrome and extracting visible text, with PDF
#   fallback. Classification is submitted to OpenAI as a batch job.
#   Falls back to static HTML fetching when headless rendering is blocked.
# Input:  Dataset 3.xlsx
# Output: Dataset 5.xlsx

library(readxl)
library(writexl)
library(dplyr)
library(purrr)
library(tibble)
library(stringr)
library(httr)
library(httr2)
library(jsonlite)

# chromote and pdftools are required; xml2 is used if available
if (!requireNamespace("chromote", quietly = TRUE)) {
  stop("chromote not installed. Run install.packages('chromote') and restart R.", call. = FALSE)
}
if (!requireNamespace("pdftools", quietly = TRUE)) {
  stop("pdftools not installed. Run install.packages('pdftools').", call. = FALSE)
}

df <- read_xlsx("Dataset 3.xlsx")

`%||%` <- function(a, b) if (!is.null(a) && length(a) && !all(is.na(a))) a else b

## 1) Configuration ---------------------------------------------------------
DEFAULT_MAX_PROMPT_CHARS   <- 120000  # whole payload cap
DEFAULT_MAX_FULLTEXT_CHARS <- 90000   # visible text cap inside payload

OPENAI_MODEL <- "gpt-5.2"
OPENAI_TIMEOUT_SEC <- 180
OPENAI_MAX_RETRIES <- 4
OPENAI_BASE_WAIT   <- 5
OPENAI_API_BASE <- "https://api.openai.com/v1"

# OpenAI Batch API controls
OPENAI_BATCH_REQUESTS_PER_JOB <- 1000
OPENAI_BATCH_POLL_SECONDS <- 30
OPENAI_BATCH_MAX_HOURS <- 24
OPENAI_BATCH_COMPLETION_WINDOW <- "24h"

# Batching / extraction controls (local extraction/checkpoint batching)
BATCH_SIZE <- 100
HEADLESS_WAIT_SEC <- 10
TIMEOUT_SEC <- 45  # used as a soft budget; chromote doesn't obey httr2 timeout
USE_SCROLL <- TRUE

# Output paths
CHECKPOINT_DIR <- "."
OUT_FINAL_XLSX <- "Dataset 5.xlsx"

## 2) OpenAI helpers --------------------------------------------------------
as_chr1_2 <- function(x) {
  if (is.null(x) || length(x) == 0) return(NA_character_)
  as.character(x[[1]])
}

as_int1_2 <- function(x) {
  if (is.null(x) || length(x) == 0 || is.na(x[[1]])) return(NA_integer_)
  suppressWarnings(as.integer(x[[1]]))
}

as_list_chr_2 <- function(x) {
  if (is.null(x) || length(x) == 0) return(list(NA_character_))
  list(as.character(unlist(x)))
}

is_unclear_or_na <- function(x) {
  if (is.list(x)) x <- unlist(x, use.names = FALSE)

  if (length(x) == 0) return(TRUE)
  x <- x[!is.na(x)]
  if (length(x) == 0) return(TRUE)

  if (!is.character(x)) return(FALSE)

  all(tolower(trimws(x)) == "unclear")
}

schema_body <- list(
  type = "object",
  properties = list(
    country_of_first_affiliation_of_first_author = list(type = "string"),
    study_design_type = list(
      type = "string",
      enum = c(
        "Interventional trial",
        "Observational study/model development",
        "Observational study/model development with external validation",
        "Review article",
        "Protocol",
        "Editorial/letter",
        "Conference paper abstract only",
        "Book chapter",
        "other", "unclear"
      )
    ),
    input_data_modality = list(
      type  = "array",
      items = list(type = "string", enum = c("Surface ECG", "EPS", "CIED", "other", "unclear")),
      minItems = 1
    ),
    clinical_purpose_model_output = list(
      type  = "array",
      items = list(
        type = "string",
        enum = c(
          "Atrial arrhythmia diagnosis",
          "Ventricular arrhythmia diagnosis",
          "Atrial and ventricular arrhythmia diagnosis",
          "Atrial mechanism/substrate/localisation",
          "Ventricular mechanism/substrate/localisation",
          "Atrial and ventricular mechanism/substrate/localisation",
          "Ischaemia related diagnosis",
          "Structural heart diagnosis",
          "Other-cardiac diagnosis",
          "Non-cardiac diagnosis",
          "Prognosis",
          "Treatment decision support",
          "Treatment response prediction",
          "unclear"
        )
      ),
      minItems = 1
    ),
    model_architecture = list(type = "string", enum = c("neural network", "other machine learning", "machine learning/AI not used", "unclear")),
    dataset_origin = list(
      type  = "array",
      items = list(type = "string", enum = c("MIT-BIH database", "PTB", "Physionet", "Other public database", "Investigator developed", "unclear")),
      minItems = 1
    ),
    funding_source = list(
      type  = "array",
      items = list(type = "string", enum = c("Academic/public", "Industry", "Charity", "none", "unclear")),
      minItems = 1
    ),
    main_accuracy_metric = list(
      type  = "array",
      items = list(type = "string", enum = c("AUROC", "Sensitivity", "Specificity", "Accuracy", "PPV", "NPV", "F1 score", "C-index", "Cohen's kappa", "other", "unclear")),
      minItems = 1
    )
  ),
  required = c(
    "country_of_first_affiliation_of_first_author",
    "study_design_type",
    "input_data_modality",
    "clinical_purpose_model_output",
    "model_architecture",
    "dataset_origin",
    "funding_source",
    "main_accuracy_metric"
  ),
  additionalProperties = FALSE
)

schema_cols <- c(
  "country_of_first_affiliation_of_first_author",
  "study_design_type",
  "input_data_modality",
  "clinical_purpose_model_output",
  "model_architecture",
  "dataset_origin",
  "funding_source",
  "main_accuracy_metric"
)

build_openai_responses_body_2 <- function(input_text) {
  list(
    model = OPENAI_MODEL,
    input = input_text,
    temperature = 0,
    text = list(
      format = list(
        type = "json_schema",
        name = "ai_arrhythmia_classifier",
        strict = TRUE,
        schema = schema_body
      )
    )
  )
}

extract_response_text_from_responses_body_2 <- function(parsed) {
  out_txt <- NULL

  if (!is.null(parsed$output_text) && is.character(parsed$output_text)) {
    out_txt <- parsed$output_text
  } else if (!is.null(parsed$output)) {
    for (o in parsed$output) {
      if (!is.null(o$content)) {
        for (c in o$content) {
          if (!is.null(c$text) && is.character(c$text)) { out_txt <- c$text; break }
          if (!is.null(c$output_text) && is.character(c$output_text)) { out_txt <- c$output_text; break }
        }
      }
      if (!is.null(out_txt)) break
    }
  }

  if (is.null(out_txt) || !nzchar(out_txt)) return(NULL)
  out_txt
}

parse_classifier_output_text_to_tibble_2 <- function(out_txt) {
  tryCatch({
    tmp <- jsonlite::fromJSON(out_txt, simplifyVector = FALSE)
    tibble::tibble(
      country_of_first_affiliation_of_first_author = as_chr1_2(tmp$country_of_first_affiliation_of_first_author),
      study_design_type = as_chr1_2(tmp$study_design_type),
      input_data_modality = as_list_chr_2(tmp$input_data_modality),
      clinical_purpose_model_output = as_list_chr_2(tmp$clinical_purpose_model_output),
      model_architecture = as_chr1_2(tmp$model_architecture),
      dataset_origin = as_list_chr_2(tmp$dataset_origin),
      funding_source = as_list_chr_2(tmp$funding_source),
      main_accuracy_metric = as_list_chr_2(tmp$main_accuracy_metric)
    )
  }, error = function(e) NULL)
}

parse_responses_body_to_tibble_2 <- function(parsed_responses_body) {
  out_txt <- extract_response_text_from_responses_body_2(parsed_responses_body)
  if (is.null(out_txt)) return(NULL)
  parse_classifier_output_text_to_tibble_2(out_txt)
}

openai_request_2 <- function(input_text,
                             max_retries = OPENAI_MAX_RETRIES,
                             base_wait = OPENAI_BASE_WAIT) {

  stopifnot(nzchar(Sys.getenv("OPENAI_API_KEY")))

  payload <- build_openai_responses_body_2(input_text)

  req <- httr2::request("https://api.openai.com/v1/responses") |>
    httr2::req_headers(
      Authorization = paste("Bearer", Sys.getenv("OPENAI_API_KEY")),
      `Content-Type` = "application/json"
    ) |>
    httr2::req_timeout(OPENAI_TIMEOUT_SEC) |>
    httr2::req_body_json(payload) |>
    httr2::req_error(is_error = function(resp) FALSE)

  attempt <- 1
  wait <- base_wait

  repeat {
    resp <- tryCatch(httr2::req_perform(req), error = function(e) NULL)

    if (is.null(resp)) {
      if (attempt >= max_retries) return(NULL)
      Sys.sleep(wait); attempt <- attempt + 1; wait <- wait * 2
      next
    }

    st <- httr2::resp_status(resp)
    if (st == 429) {
      if (attempt >= max_retries) return(NULL)
      Sys.sleep(wait); attempt <- attempt + 1; wait <- wait * 2
      next
    }
    if (st < 200 || st >= 300) return(NULL)

    parsed <- httr2::resp_body_json(resp, simplifyVector = FALSE)
    return(parse_responses_body_to_tibble_2(parsed))
  }
}

blank_result_tibble_2 <- function() {
  tibble::tibble(
    country_of_first_affiliation_of_first_author = NA_character_,
    study_design_type = NA_character_,
    input_data_modality = list(NA_character_),
    clinical_purpose_model_output = list(NA_character_),
    model_architecture = NA_character_,
    dataset_origin = list(NA_character_),
    funding_source = list(NA_character_),
    main_accuracy_metric = list(NA_character_)
  )
}

empty_class_df_2 <- function() {
  tibble::tibble(
    row_id = integer(),
    country_of_first_affiliation_of_first_author = character(),
    study_design_type = character(),
    input_data_modality = list(),
    clinical_purpose_model_output = list(),
    model_architecture = character(),
    dataset_origin = list(),
    funding_source = list(),
    main_accuracy_metric = list()
  )
}

blank_result_tibble_with_row_id_2 <- function(row_id) {
  dplyr::bind_cols(tibble::tibble(row_id = as.integer(row_id)), blank_result_tibble_2())
}

## 3) OpenAI Batch API helpers ----------------------------------------------
openai_stop_if_no_key_2 <- function() {
  if (identical(Sys.getenv("OPENAI_API_KEY"), "") || !nzchar(Sys.getenv("OPENAI_API_KEY"))) {
    stop("OPENAI_API_KEY is empty. Set it before running.")
  }
  invisible(TRUE)
}

openai_auth_header_httr_2 <- function() {
  httr::add_headers(Authorization = paste("Bearer", Sys.getenv("OPENAI_API_KEY")))
}

openai_write_batch_input_jsonl_2 <- function(payload_df, path) {
  stopifnot(all(c("row_id", "payload") %in% names(payload_df)))

  tasks <- lapply(seq_len(nrow(payload_df)), function(i) {
    input_txt <- payload_df$payload[[i]]
    if (is.null(input_txt) || (length(input_txt) == 1 && is.na(input_txt))) input_txt <- ""

    list(
      custom_id = paste0("row-", payload_df$row_id[[i]]),
      method = "POST",
      url = "/v1/responses",
      body = build_openai_responses_body_2(input_txt)
    )
  })

  lines <- vapply(
    tasks,
    function(x) jsonlite::toJSON(x, auto_unbox = TRUE, null = "null"),
    FUN.VALUE = character(1)
  )

  writeLines(lines, path, useBytes = TRUE)
  invisible(path)
}

openai_upload_batch_file_2 <- function(jsonl_path) {
  res <- httr::POST(
    paste0(OPENAI_API_BASE, "/files"),
    openai_auth_header_httr_2(),
    body = list(
      purpose = "batch",
      file = httr::upload_file(jsonl_path, type = "text/jsonl")
    ),
    encode = "multipart",
    httr::config(connecttimeout = 120, timeout = 600)
  )
  httr::stop_for_status(res)
  httr::content(res, as = "parsed", simplifyVector = FALSE)$id
}

openai_create_batch_job_2 <- function(input_file_id,
                                      completion_window = OPENAI_BATCH_COMPLETION_WINDOW) {
  res <- httr::POST(
    paste0(OPENAI_API_BASE, "/batches"),
    openai_auth_header_httr_2(),
    httr::content_type_json(),
    encode = "json",
    body = list(
      input_file_id = input_file_id,
      endpoint = "/v1/responses",
      completion_window = completion_window
    ),
    httr::config(connecttimeout = 120, timeout = 120)
  )
  httr::stop_for_status(res)
  httr::content(res, as = "parsed", simplifyVector = FALSE)
}

openai_get_batch_2 <- function(batch_id) {
  res <- httr::GET(
    paste0(OPENAI_API_BASE, "/batches/", batch_id),
    openai_auth_header_httr_2(),
    httr::config(connecttimeout = 120, timeout = 120)
  )
  httr::stop_for_status(res)
  httr::content(res, as = "parsed", simplifyVector = FALSE)
}

openai_wait_for_batch_2 <- function(batch_id,
                                    poll_seconds = OPENAI_BATCH_POLL_SECONDS,
                                    max_hours = OPENAI_BATCH_MAX_HOURS) {
  terminal <- c("completed", "failed", "expired", "cancelled")
  t0 <- Sys.time()

  repeat {
    b <- openai_get_batch_2(batch_id)
    rc <- b$request_counts %||% list(total = NA, completed = NA, failed = NA)

    message(
      "OpenAI batch ", batch_id,
      " | status=", b$status,
      " | completed=", rc$completed %||% 0, "/", rc$total %||% 0,
      " | failed=", rc$failed %||% 0
    )

    if (b$status %in% terminal) return(b)

    elapsed_h <- as.numeric(difftime(Sys.time(), t0, units = "hours"))
    if (elapsed_h > max_hours) stop("Timed out waiting for OpenAI batch completion.")
    Sys.sleep(poll_seconds)
  }
}

openai_download_file_text_2 <- function(file_id) {
  res <- httr::GET(
    paste0(OPENAI_API_BASE, "/files/", file_id, "/content"),
    openai_auth_header_httr_2(),
    httr::config(connecttimeout = 120, timeout = 600)
  )
  httr::stop_for_status(res)
  httr::content(res, as = "text", encoding = "UTF-8")
}

parse_jsonl_text_2 <- function(txt) {
  lines <- strsplit(txt, "\n", fixed = TRUE)[[1]]
  lines <- trimws(lines)
  lines <- lines[nzchar(lines)]
  lapply(lines, function(x) jsonlite::fromJSON(x, simplifyVector = FALSE))
}

openai_parse_batch_output_record_2 <- function(rec) {
  custom_id <- rec$custom_id %||% ""
  row_id <- suppressWarnings(as.integer(sub("^row-", "", custom_id)))
  if (is.na(row_id)) return(NULL)

  make_blank <- function(msg) {
    message("row ", row_id, " batch error: ", msg)
    blank_result_tibble_with_row_id_2(row_id)
  }

  if (!is.null(rec$error)) {
    return(make_blank(rec$error$message %||% "unknown"))
  }

  if (is.null(rec$response) || is.null(rec$response$status_code) || rec$response$status_code != 200) {
    status <- rec$response$status_code %||% NA_integer_
    return(make_blank(paste0("HTTP status ", status)))
  }

  parsed_row <- parse_responses_body_to_tibble_2(rec$response$body)
  if (is.null(parsed_row)) {
    return(make_blank("no parseable model JSON output"))
  }

  dplyr::bind_cols(tibble::tibble(row_id = row_id), parsed_row)
}

run_openai_payload_batches_2 <- function(payload_df,
                                         max_requests_per_batch = OPENAI_BATCH_REQUESTS_PER_JOB,
                                         poll_seconds = OPENAI_BATCH_POLL_SECONDS,
                                         max_hours = OPENAI_BATCH_MAX_HOURS,
                                         label = "run",
                                         error_dir = tempdir()) {
  openai_stop_if_no_key_2()

  if (nrow(payload_df) == 0) {
    return(list(class_df = empty_class_df_2(), batches = tibble::tibble()))
  }

  safe_label <- gsub("[^A-Za-z0-9_-]+", "_", label)
  payload_df <- payload_df %>% dplyr::arrange(.data$row_id)

  chunks <- split(payload_df, ceiling(seq_len(nrow(payload_df)) / max_requests_per_batch))
  all_rows <- vector("list", length(chunks))
  batch_meta <- vector("list", length(chunks))

  if (!dir.exists(error_dir)) dir.create(error_dir, recursive = TRUE, showWarnings = FALSE)

  for (k in seq_along(chunks)) {
    chunk_df <- chunks[[k]]
    message("Submitting OpenAI batch chunk ", k, "/", length(chunks), " (", nrow(chunk_df), " requests)")

    jsonl_path <- file.path(
      tempdir(),
      paste0("batch_input_", safe_label, "_", k, "_", format(Sys.time(), "%Y%m%d_%H%M%S"), ".jsonl")
    )
    openai_write_batch_input_jsonl_2(chunk_df, jsonl_path)

    input_file_id <- openai_upload_batch_file_2(jsonl_path)
    batch <- openai_create_batch_job_2(input_file_id)
    message("OpenAI batch created: ", batch$id)

    final_batch <- openai_wait_for_batch_2(batch$id, poll_seconds = poll_seconds, max_hours = max_hours)

    batch_meta[[k]] <- tibble::tibble(
      chunk = k,
      batch_id = final_batch$id %||% NA_character_,
      status = final_batch$status %||% NA_character_,
      input_file_id = final_batch$input_file_id %||% NA_character_,
      output_file_id = final_batch$output_file_id %||% NA_character_,
      error_file_id = final_batch$error_file_id %||% NA_character_,
      total = final_batch$request_counts$total %||% NA_integer_,
      completed = final_batch$request_counts$completed %||% NA_integer_,
      failed = final_batch$request_counts$failed %||% NA_integer_
    )

    if (!is.null(final_batch$output_file_id)) {
      out_text <- openai_download_file_text_2(final_batch$output_file_id)
      out_records <- parse_jsonl_text_2(out_text)
      parsed_rows <- purrr::compact(lapply(out_records, openai_parse_batch_output_record_2))
      chunk_class_df <- if (length(parsed_rows) == 0) empty_class_df_2() else dplyr::bind_rows(parsed_rows)
    } else {
      chunk_class_df <- empty_class_df_2()
    }

    if (!is.null(final_batch$error_file_id)) {
      err_text <- openai_download_file_text_2(final_batch$error_file_id)
      err_path <- file.path(
        error_dir,
        paste0("openai_batch_errors_", safe_label, "_", k, "_", format(Sys.time(), "%Y%m%d_%H%M%S"), ".jsonl")
      )
      writeLines(err_text, err_path, useBytes = TRUE)
      message("Saved OpenAI batch error file to: ", err_path)
    }

    missing_ids <- setdiff(chunk_df$row_id, chunk_class_df$row_id %||% integer())
    if (length(missing_ids)) {
      blanks <- dplyr::bind_rows(lapply(missing_ids, blank_result_tibble_with_row_id_2))
      chunk_class_df <- dplyr::bind_rows(chunk_class_df, blanks)
    }

    all_rows[[k]] <- chunk_class_df %>%
      dplyr::distinct(.data$row_id, .keep_all = TRUE)
  }

  list(
    class_df = dplyr::bind_rows(all_rows) %>%
      dplyr::distinct(.data$row_id, .keep_all = TRUE),
    batches = dplyr::bind_rows(batch_meta)
  )
}

collapse_vals_2 <- function(x) {
  x <- unlist(x)
  x <- x[!is.na(x)]
  if (length(x) == 0) return(NA_character_)
  paste(unique(x), collapse = "; ")
}

collapse_new_listcols_2 <- function(df_in) {
  df_in %>%
    dplyr::mutate(
      input_data_modality_new = purrr::map_chr(input_data_modality_new, collapse_vals_2),
      clinical_purpose_model_output_new = purrr::map_chr(clinical_purpose_model_output_new, collapse_vals_2),
      dataset_origin_new = purrr::map_chr(dataset_origin_new, collapse_vals_2),
      funding_source_new = purrr::map_chr(funding_source_new, collapse_vals_2),
      main_accuracy_metric_new = purrr::map_chr(main_accuracy_metric_new, collapse_vals_2)
    )
}

merge_new_into_df_2 <- function(df_base, class_df, meta_df) {

  class_df_new <- class_df %>%
    dplyr::rename_with(~ paste0(.x, "_new"), dplyr::all_of(schema_cols))

  meta_df_ren <- meta_df %>%
    dplyr::rename_with(~ paste0(.x, "_newmeta"), -row_id)

  df_out <- df_base %>%
    dplyr::mutate(row_id = dplyr::row_number()) %>%
    dplyr::left_join(class_df_new, by = "row_id") %>%
    # force predictable suffixes if collisions occur
    dplyr::left_join(meta_df_ren, by = "row_id", suffix = c("", "__incoming"))

  # If a column already existed, dplyr will create __incoming versions.
  # Coalesce old (existing) with incoming (new batch).
  meta_cols_newmeta <- setdiff(names(meta_df_ren), "row_id")
  for (nm in meta_cols_newmeta) {
    incoming <- paste0(nm, "__incoming")
    if (incoming %in% names(df_out)) {
      df_out[[nm]] <- dplyr::coalesce(df_out[[nm]], df_out[[incoming]])
      df_out[[incoming]] <- NULL
    }
  }

  df_out <- df_out %>%
    dplyr::select(-row_id) %>%
    collapse_new_listcols_2()

  for (col in schema_cols) {
    newcol <- paste0(col, "_new")
    if (newcol %in% names(df_out)) {
      df_out[[col]] <- dplyr::coalesce(df_out[[col]], df_out[[newcol]])
    }
  }

  if (!"botBlocked" %in% names(df_out)) df_out$botBlocked <- NA_integer_
  if (!"sourceUsed" %in% names(df_out)) df_out$sourceUsed <- NA_character_

  if ("botBlocked_newmeta" %in% names(df_out)) {
    df_out$botBlocked <- dplyr::coalesce(df_out$botBlocked, df_out$botBlocked_newmeta)
  }
  if ("sourceUsed_newmeta" %in% names(df_out)) {
    df_out$sourceUsed <- dplyr::coalesce(df_out$sourceUsed, df_out$sourceUsed_newmeta)
  }

  df_out %>%
    dplyr::select(
      -dplyr::any_of(paste0(schema_cols, "_new")),
      -dplyr::any_of(c("botBlocked_newmeta", "sourceUsed_newmeta"))
    )
}

build_payload_df_from_prep_2 <- function(prep_list, row_ids) {
  send_mask <- vapply(prep_list, function(x) isTRUE(x$send_to_openai), logical(1))
  tibble::tibble(
    row_id = as.integer(row_ids[send_mask]),
    payload = purrr::map_chr(prep_list[send_mask], "payload")
  )
}

collect_prefilled_class_rows_2 <- function(prep_list, row_ids) {
  rows <- purrr::compact(purrr::map2(prep_list, row_ids, function(pr, rid) {
    if (isTRUE(pr$send_to_openai)) return(NULL)
    dplyr::bind_cols(
      tibble::tibble(row_id = as.integer(rid)),
      pr$result_prefill %||% blank_result_tibble_2()
    )
  }))

  if (!length(rows)) return(empty_class_df_2())
  dplyr::bind_rows(rows) %>%
    dplyr::distinct(.data$row_id, .keep_all = TRUE)
}

## 4) Visible text extraction -----------------------------------------------
js_eval_value_2 <- function(b, expr, await = FALSE) {
  res <- b$Runtime$evaluate(
    expression = expr,
    awaitPromise = await,
    returnByValue = TRUE
  )
  res$result$value
}

chromote_jitter_sleep_2 <- function(min_sec = 0.2, max_sec = 0.8) {
  lo <- suppressWarnings(as.numeric(min_sec)[1])
  hi <- suppressWarnings(as.numeric(max_sec)[1])
  if (!is.finite(lo)) lo <- 0
  if (!is.finite(hi)) hi <- lo
  if (hi < lo) {
    tmp <- lo
    lo <- hi
    hi <- tmp
  }
  if (hi <= 0) return(invisible(NULL))
  Sys.sleep(stats::runif(1, max(0, lo), hi))
  invisible(NULL)
}

chromote_wait_ready_state_2 <- function(b, timeout_sec = 15, poll_sec = 0.25) {
  t0 <- Sys.time()
  repeat {
    rs <- tryCatch(js_eval_value_2(b, "document.readyState || ''"), error = function(e) "")
    rs1 <- tolower(as.character(rs %||% ""))
    if (length(rs1) && nzchar(rs1[1]) && rs1[1] %in% c("interactive", "complete")) return(TRUE)

    elapsed <- as.numeric(difftime(Sys.time(), t0, units = "secs"))
    if (!is.finite(elapsed) || elapsed >= timeout_sec) break
    Sys.sleep(poll_sec)
  }
  FALSE
}

js_escape_sq_2 <- function(x) {
  x <- as.character(x %||% "")
  x <- gsub("\\\\", "\\\\\\\\", x)
  x <- gsub("'", "\\\\'", x, fixed = TRUE)
  x <- gsub("\r", "", x, fixed = TRUE)
  x <- gsub("\n", " ", x, fixed = TRUE)
  x
}

click_by_text_chromote_2 <- function(b, text, tags = c("button", "a")) {
  tags <- tags[!is.na(tags) & nzchar(tags)]
  if (!length(tags)) tags <- c("button", "a")
  tag_pred <- paste(sprintf("self::%s", tags), collapse = " or ")
  txt_esc <- js_escape_sq_2(text)
  tag_pred_esc <- js_escape_sq_2(tag_pred)

  js <- sprintf(
    "(function(){
       try {
         const xpath = \"//*[\" + '%s' + \"][contains(translate(normalize-space(string(.)), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), '%s')]\"; 
         const node = document.evaluate(xpath, document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null).singleNodeValue;
         if (!node) return { clicked: false, reason: 'not_found' };
         try { node.scrollIntoView({ block: 'center' }); } catch(e) {}
         try { node.click(); } catch(e) {
           const evt = new MouseEvent('click', { bubbles: true, cancelable: true, view: window });
           node.dispatchEvent(evt);
         }
         return { clicked: true };
       } catch(e) {
         return { clicked: false, reason: String(e) };
       }
     })();",
    tag_pred_esc,
    tolower(txt_esc)
  )

  out <- tryCatch(js_eval_value_2(b, js), error = function(e) NULL)
  if (is.null(out) || !is.list(out)) return(FALSE)
  isTRUE(out$clicked)
}

dismiss_common_overlays_chromote_2 <- function(b) {
  js <- "(function(){
    const selectors = [
      '#onetrust-accept-btn-handler',
      'button#onetrust-accept-btn-handler',
      'button[aria-label=\"Accept\"]',
      'button[aria-label=\"Accept all\"]',
      'button[mode=\"primary\"]',
      'button[data-testid=\"accept-all\"]',
      'button[title=\"Accept\"]'
    ];
    let clicked = 0;
    for (const s of selectors) {
      try {
        const el = document.querySelector(s);
        if (el) { el.click(); clicked += 1; }
      } catch(e) {}
    }
    return clicked;
  })();"

  try(js_eval_value_2(b, js), silent = TRUE)
  # Text-based fallbacks for consent modals where selectors vary.
  for (txt in c("accept", "accept all", "i agree", "agree", "ok", "continue")) {
    try(click_by_text_chromote_2(b, txt, tags = c("button", "a")), silent = TRUE)
  }
  invisible(TRUE)
}

extract_inner_text_from_session_2 <- function(
  b,
  wait_sec = 6,
  scroll = TRUE,
  scroll_steps = 10,
  scroll_pause_sec = 0.35
) {
  try(chromote_wait_ready_state_2(b, timeout_sec = max(2, min(15, wait_sec + 3))), silent = TRUE)
  try(dismiss_common_overlays_chromote_2(b), silent = TRUE)
  Sys.sleep(wait_sec)
  try(dismiss_common_overlays_chromote_2(b), silent = TRUE)
  try(chromote_jitter_sleep_2(0.05, 0.25), silent = TRUE)

  if (isTRUE(scroll)) {
    expr_scroll <- sprintf(
      "(async () => {
         const sleep = (ms) => new Promise(r => setTimeout(r, ms));
         for (let i = 0; i < %d; i++) {
           window.scrollTo(0, document.body.scrollHeight);
           await sleep(%d);
         }
         window.scrollTo(0, 0);
         return true;
       })();",
      as.integer(scroll_steps),
      as.integer(scroll_pause_sec * 1000)
    )
    try(js_eval_value_2(b, expr_scroll, await = TRUE), silent = TRUE)
  }

  expr <- "(function(){
    function pickTextInfo(){
      const sels = ['article','main','#main-content','#content','#main',
        '.article-body','.ArticleBody','.c-article-body','.article__body','.article-content',
        '.main-content','.page-content'];
      for (const s of sels){
        const n = document.querySelector(s);
        if (n && n.innerText && n.innerText.trim().length > 200) return { text: n.innerText, selector: s };
      }
      return {
        text: (document.body && document.body.innerText) ? document.body.innerText : '',
        selector: 'body'
      };
    }
    const picked = pickTextInfo();
    const bodyText = (document.body && document.body.innerText) ? document.body.innerText : '';
    return {
      text: picked.text || '',
      bodyText: bodyText,
      pickedSelector: picked.selector || '',
      href: location.href || '',
      title: document.title || '',
      contentType: document.contentType || ''
    };
  })();"

  x <- tryCatch(js_eval_value_2(b, expr), error = function(e) NULL)
  if (is.null(x)) {
    return(list(text = NA_character_, href = NA_character_, contentType = NA_character_))
  }

  txt <- x$text %||% NA_character_
  if (!is.character(txt) || !nzchar(trimws(txt))) txt <- NA_character_

  list(
    text = txt,
    body_text = as.character(x$bodyText %||% NA_character_),
    picked_selector = as.character(x$pickedSelector %||% NA_character_),
    href = as.character(x$href %||% NA_character_),
    title = as.character(x$title %||% NA_character_),
    contentType = as.character(x$contentType %||% NA_character_)
  )
}

find_fulltext_candidates_chromote_2 <- function(b, max_n = 10) {
  expr <- "(function(){
    const bad = /(supplement|supplementary|figures?|tables?|references|permissions|citation|metrics|altmetric|share|author|login|sign in)/i;
    const rows = [];
    const nodes = Array.from(document.querySelectorAll('a,button,[role=\"button\"]'));
    for (let idx = 0; idx < nodes.length; idx++) {
      const el = nodes[idx];
      const txt = ((el.innerText || el.textContent || '').trim());
      const aria = (el.getAttribute('aria-label') || '').trim();
      const title = (el.getAttribute('title') || '').trim();
      const cls = ((el.getAttribute('class') || '') + '').trim();
      const htmlSnippet = ((el.innerHTML || '') + '').replace(/\\s+/g, ' ').slice(0, 600);
      const img = el.querySelector ? el.querySelector('img') : null;
      const imgAlt = img ? ((img.getAttribute('alt') || '') + '').trim() : '';
      const imgSrc = img ? ((img.getAttribute('src') || '') + '').trim() : '';
      const onclickAttr = ((el.getAttribute('onclick') || '') + '').trim();
      const downloadAttr = ((el.getAttribute('download') || '') + '').trim();
      const dataAttrs = [];
      try {
        for (const a of Array.from(el.attributes || [])) {
          if (/^data-/i.test(a.name || '')) dataAttrs.push(String(a.name) + '=' + String(a.value || ''));
        }
      } catch(e) {}
      const attrsBlob = [onclickAttr, downloadAttr, cls, imgAlt, imgSrc, htmlSnippet].concat(dataAttrs).join(' ');
      const hrefRaw = ((el.getAttribute('href') || '') + '').trim();
      let href = ((el.href || hrefRaw || '') + '').trim();
      if (!href && hrefRaw) {
        try { href = new URL(hrefRaw, document.baseURI || location.href).href; } catch (e) {}
      }

      function firstUrlFromText(s){
        if (!s) return '';
        const mAbs = s.match(/https?:\\/\\/[^\\s'\"<>]+/i);
        if (mAbs && mAbs[0]) return mAbs[0];
        const mPdfRel = s.match(/(?:\\.{1,2}\\/|\\/)[^\\s'\"<>]*\\.pdf(?:\\?[^\\s'\"<>]*)?/i);
        if (mPdfRel && mPdfRel[0]) return mPdfRel[0];
        const mViewFile = s.match(/(?:\\.{1,2}\\/|\\/)[^\\s'\"<>]*\\/viewfile\\/[^\\s'\"<>]+/i);
        if (mViewFile && mViewFile[0]) return mViewFile[0];
        return '';
      }

      let auxUrl = firstUrlFromText(attrsBlob);
      if (auxUrl) {
        try { auxUrl = new URL(auxUrl, document.baseURI || location.href).href; } catch(e) {}
      }

      const blob = [txt, aria, title, hrefRaw, href, attrsBlob, auxUrl].join(' ').toLowerCase();
      if (!blob) continue;
      if (bad.test(blob)) continue;

      let score = 0;
      if (/download\\s*pdf|view\\s*pdf|full\\s*text\\s*pdf/.test(blob)) score += 120;
      if (/\\bpdf\\b/.test(blob)) score += 80;
      if (/pdf[-_ ]?icon|fa-file-pdf|bi-file-pdf|material-icons/.test(blob)) score += 60;
      if (/download\\s*article|download\\s*full\\s*text|download\\b/.test(blob)) score += 60;
      if (/full\\s*text|full\\s*article|article\\s*html|view\\s*article|read\\s*article/.test(blob)) score += 70;
      if (/\\.pdf(\\?|$)/.test(href) || /\\/pdf\\//.test(href) || /\\/viewfile\\//i.test(href)) score += 40;
      if (auxUrl && (/\\.pdf(\\?|$)/.test(auxUrl) || /\\/pdf\\//.test(auxUrl) || /\\/viewfile\\//i.test(auxUrl))) score += 45;
      if (onclickAttr || dataAttrs.length) score += 10;

      if (score > 0) {
        rows.push({
          text: txt.slice(0,200),
          href: href,
          hrefRaw: hrefRaw,
          auxUrl: auxUrl,
          onclick: onclickAttr.slice(0,400),
          attrsBlob: attrsBlob.slice(0,600),
          className: cls.slice(0,200),
          imgAlt: imgAlt.slice(0,200),
          imgSrc: imgSrc.slice(0,300),
          score: score,
          domIndex: idx,
          tagName: (el.tagName || '').toLowerCase()
        });
      }
    }

    rows.sort((a,b) => b.score - a.score);

    const seen = new Set();
    const out = [];
    for (const r of rows) {
      const key = (r.href || r.text || '').toLowerCase();
      if (!key || seen.has(key)) continue;
      seen.add(key);
      out.push(r);
      if (out.length >= 20) break;
    }
    return out;
  })();"

  vals <- tryCatch(js_eval_value_2(b, expr), error = function(e) NULL)
  if (is.null(vals) || !length(vals)) {
    return(tibble::tibble(text = character(), href = character(), score = numeric()))
  }

  out <- purrr::map_dfr(vals, function(v) {
    tibble::tibble(
      text = as.character(v$text %||% NA_character_),
      href = as.character(v$href %||% NA_character_),
      href_raw = as.character(v$hrefRaw %||% NA_character_),
      aux_url = as.character(v$auxUrl %||% NA_character_),
      onclick = as.character(v$onclick %||% NA_character_),
      attrs_blob = as.character(v$attrsBlob %||% NA_character_),
      class_name = as.character(v$className %||% NA_character_),
      img_alt = as.character(v$imgAlt %||% NA_character_),
      img_src = as.character(v$imgSrc %||% NA_character_),
      score = as.numeric(v$score %||% NA_real_),
      dom_index = suppressWarnings(as.integer(v$domIndex %||% NA_integer_)),
      tag_name = as.character(v$tagName %||% NA_character_)
    )
  })

  out %>%
    dplyr::filter(!is.na(score)) %>%
    dplyr::arrange(dplyr::desc(score)) %>%
    dplyr::slice_head(n = max_n)
}

looks_like_pdf_url_2 <- function(x) {
  if (is.null(x) || length(x) == 0 || is.na(x[[1]])) return(FALSE)
  u <- as.character(x[[1]])
  if (looks_like_image_url_2(u)) return(FALSE)
  grepl("\\.pdf($|\\?)|/pdf/|/epdf/|/pdfdirect/|\\bpdf\\b", u, ignore.case = TRUE)
}

looks_like_image_url_2 <- function(x) {
  if (is.null(x) || length(x) == 0 || is.na(x[[1]])) return(FALSE)
  grepl("\\.(png|jpe?g|gif|svg|webp|ico)(\\?|$)", as.character(x[[1]]), ignore.case = TRUE)
}

looks_like_http_url_2 <- function(x) {
  if (is.null(x) || length(x) == 0 || is.na(x[[1]])) return(FALSE)
  grepl("^https?://", as.character(x[[1]]), ignore.case = TRUE)
}

is_pdf_content_type_2 <- function(x) {
  if (is.null(x) || length(x) == 0 || is.na(x[[1]])) return(FALSE)
  grepl("pdf", as.character(x[[1]]), ignore.case = TRUE)
}

candidate_prefers_pdf_attempt_2 <- function(href = NA_character_, text = NA_character_, aux_url = NA_character_, attrs_blob = NA_character_) {
  href <- as.character(href %||% NA_character_)
  text <- as.character(text %||% NA_character_)
  aux_url <- as.character(aux_url %||% NA_character_)
  attrs_blob <- as.character(attrs_blob %||% NA_character_)

  isTRUE(
    looks_like_pdf_url_2(href) ||
      looks_like_pdf_url_2(aux_url) ||
      grepl("/viewfile/", href, ignore.case = TRUE) ||
      grepl("/viewfile/", aux_url, ignore.case = TRUE) ||
      grepl("/article/download/", href, ignore.case = TRUE) ||
      grepl("/article/download/", aux_url, ignore.case = TRUE) ||
      grepl("\\bpdf\\b", text, ignore.case = TRUE) ||
      grepl("\\bpdf\\b", attrs_blob, ignore.case = TRUE) ||
      grepl("\\bepdf\\b|enhanced\\s*pdf|article\\s*pdf", text, ignore.case = TRUE) ||
      grepl("\\bepdf\\b|enhanced\\s*pdf|article\\s*pdf", attrs_blob, ignore.case = TRUE) ||
      grepl("download\\s*full\\s*paper", text, ignore.case = TRUE) ||
      grepl("download\\s*full\\s*paper", attrs_blob, ignore.case = TRUE) ||
      grepl("download\\s*article|download\\s*full\\s*text|download\\b", text, ignore.case = TRUE) ||
      grepl("download\\s*article|download\\s*full\\s*text|download\\b", attrs_blob, ignore.case = TRUE)
  )
}

has_usable_href_2 <- function(x) {
  if (is.null(x) || length(x) == 0 || is.na(x[[1]])) return(FALSE)
  x <- stringr::str_trim(as.character(x[[1]]))
  if (!nzchar(x)) return(FALSE)
  if (x %in% c("#", "/#")) return(FALSE)
  if (grepl("^javascript:", x, ignore.case = TRUE)) return(FALSE)
  TRUE
}

text_len_2 <- function(x) {
  if (is.null(x) || length(x) == 0 || is.na(x[[1]])) return(0L)
  nchar(as.character(x[[1]]))
}

best_pdf_request_url_2 <- function(primary, fallback = NA_character_) {
  if (isTRUE(looks_like_http_url_2(primary))) return(as.character(primary))
  if (isTRUE(looks_like_http_url_2(fallback))) return(as.character(fallback))
  NA_character_
}

expand_pdf_candidate_urls_2 <- function(x) {
  if (is.null(x) || length(x) == 0 || is.na(x[[1]]) || !nzchar(trimws(as.character(x[[1]])))) {
    return(character())
  }

  u <- as.character(x[[1]])
  out <- unique(c(u))

  # Wiley enhanced PDF routes often need conversion to /pdf or /pdfdirect.
  if (grepl("onlinelibrary\\.wiley\\.com", u, ignore.case = TRUE)) {
    u_pdf <- sub("/doi/epdf/", "/doi/pdf/", u, ignore.case = TRUE)
    u_pdf <- sub("/doi/full/", "/doi/pdf/", u_pdf, ignore.case = TRUE)
    u_pdfdirect <- sub("/doi/epdf/", "/doi/pdfdirect/", u, ignore.case = TRUE)
    u_pdfdirect <- sub("/doi/full/", "/doi/pdfdirect/", u_pdfdirect, ignore.case = TRUE)
    if (!identical(u_pdfdirect, u)) {
      if (!grepl("\\?", u_pdfdirect, fixed = TRUE)) {
        u_pdfdirect <- paste0(u_pdfdirect, "?download=true")
      } else if (!grepl("download=", u_pdfdirect, ignore.case = TRUE)) {
        u_pdfdirect <- paste0(u_pdfdirect, "&download=true")
      }
    }
    out <- c(out, u_pdf, u_pdfdirect)
  }

  # OJS viewer pages commonly need /article/download/<article>/<galley>.
  if (grepl("/article/view/\\d+/\\d+", u, ignore.case = TRUE)) {
    u_dl <- sub("/article/view/(\\d+)/(\\d+)", "/article/download/\\1/\\2", u, ignore.case = TRUE)
    out <- c(out, u_dl, paste0(u_dl, if (grepl("\\?", u_dl, fixed = TRUE)) "&inline=1" else "?inline=1"))
  }

  # IOP article landing pages often expose a /pdf route.
  if (grepl("iopscience\\.iop\\.org/article/", u, ignore.case = TRUE) && !grepl("/pdf($|\\?)", u, ignore.case = TRUE)) {
    out <- c(out, paste0(sub("/$", "", u), "/pdf"))
  }

  out <- out[!is.na(out) & nzchar(trimws(out))]
  unique(out)
}

extract_pdf_urls_from_text_2 <- function(x, max_n = 5) {
  if (is.null(x) || length(x) == 0 || is.na(x[[1]])) return(character())
  txt <- as.character(x[[1]])
  if (!nzchar(txt)) return(character())

  m <- gregexpr("https?://[^\\s<>\"]+\\.pdf(?:\\?[^\\s<>\"]*)?", txt, perl = TRUE, ignore.case = TRUE)
  hits <- regmatches(txt, m)[[1]]
  if (!length(hits)) return(character())

  hits <- gsub("[\\]\\)>,;]+$", "", hits, perl = TRUE)
  hits <- unique(hits[nzchar(hits)])
  if (!length(hits)) return(character())
  utils::head(hits, max_n)
}

find_embedded_pdf_url_chromote_2 <- function(b) {
  expr <- "(function(){
    const rows = [];
    const add = (u, where) => {
      if (!u) return;
      let href = String(u).trim();
      if (!href || /^data:/i.test(href) || /^blob:/i.test(href)) return;
      try { href = new URL(href, document.baseURI || location.href).href; } catch(e) {}
      rows.push({ url: href, where: where || '' });
    };

    try {
      const u0 = new URL(location.href);
      for (const key of ['src', 'file', 'url']) {
        const v = u0.searchParams.get(key);
        if (v) add(v, 'viewer-query-' + key);
      }
    } catch(e) {}

    for (const el of Array.from(document.querySelectorAll('iframe[src],embed[src],object[data]'))) {
      add(el.getAttribute('src') || el.getAttribute('data') || '', (el.tagName || '').toLowerCase());
    }
    for (const el of Array.from(document.querySelectorAll('a[href]'))) {
      const href = el.getAttribute('href') || '';
      const txt = (el.innerText || el.textContent || '').trim().toLowerCase();
      if (/pdf|epdf|pdfdirect/.test(href) || /\\bpdf\\b/.test(txt)) add(href, 'a');
    }

    const out = [];
    const seen = new Set();
    for (const r of rows) {
      const key = (r.url || '').toLowerCase();
      if (!key || seen.has(key)) continue;
      seen.add(key);
      out.push(r);
    }

    out.sort((a,b) => {
      const as = /\\.pdf(\\?|$)|\\/pdf\\//i.test(a.url) ? 0 : 1;
      const bs = /\\.pdf(\\?|$)|\\/pdf\\//i.test(b.url) ? 0 : 1;
      return as - bs;
    });
    return out.slice(0, 10);
  })();"

  vals <- tryCatch(js_eval_value_2(b, expr), error = function(e) NULL)
  if (is.null(vals) || !length(vals)) {
    return(tibble::tibble(url = character(), where = character()))
  }

  purrr::map_dfr(vals, function(v) {
    tibble::tibble(
      url = as.character(v$url %||% NA_character_),
      where = as.character(v$where %||% NA_character_)
    )
  }) %>%
    dplyr::filter(!is.na(.data$url), nzchar(.data$url))
}

chromote_user_agent_2 <- function(b) {
  ua <- tryCatch(js_eval_value_2(b, "navigator.userAgent || ''"), error = function(e) NULL)
  if (is.null(ua) || !is.character(ua) || !nzchar(trimws(ua[[1]]))) return(NA_character_)
  as.character(ua[[1]])
}

chromote_get_cookies_2 <- function(b, current_url = NA_character_) {
  try(b$Network$enable(), silent = TRUE)

  attempts <- list(
    function() {
      if (!is.na(current_url) && nzchar(trimws(current_url))) b$Network$getCookies(urls = list(current_url)) else NULL
    },
    function() {
      if (!is.na(current_url) && nzchar(trimws(current_url))) b$Network$getCookies(urls = current_url) else NULL
    },
    function() b$Network$getCookies()
  )

  raw_out <- NULL
  for (fn in attempts) {
    raw_out <- tryCatch(fn(), error = function(e) NULL)
    if (!is.null(raw_out)) break
  }

  cookies <- raw_out$cookies %||% raw_out
  if (is.null(cookies) || !length(cookies)) {
    return(tibble::tibble(name = character(), value = character()))
  }

  if (is.data.frame(cookies)) {
    out <- tibble::as_tibble(cookies)
    if (!all(c("name", "value") %in% names(out))) {
      return(tibble::tibble(name = character(), value = character()))
    }
    return(out)
  }

  # Some chromote versions return list-of-cookie objects.
  rows <- purrr::map_dfr(cookies, function(ck) {
    if (!is.list(ck)) return(tibble::tibble())
    tibble::tibble(
      name = as.character(ck$name %||% NA_character_),
      value = as.character(ck$value %||% NA_character_),
      domain = as.character(ck$domain %||% NA_character_),
      path = as.character(ck$path %||% NA_character_)
    )
  })

  if (!nrow(rows)) {
    return(tibble::tibble(name = character(), value = character()))
  }
  rows
}

chromote_cookie_header_2 <- function(b, current_url = NA_character_) {
  cks <- tryCatch(chromote_get_cookies_2(b, current_url = current_url), error = function(e) NULL)
  if (is.null(cks) || !nrow(cks)) return(NULL)

  cks <- cks %>%
    dplyr::mutate(
      name = as.character(.data$name),
      value = as.character(.data$value)
    ) %>%
    dplyr::filter(!is.na(.data$name), nzchar(.data$name))

  if (!nrow(cks)) return(NULL)

  # Keep the last cookie when names repeat.
  cks <- cks %>%
    dplyr::mutate(.ord = dplyr::row_number()) %>%
    dplyr::arrange(dplyr::desc(.data$.ord)) %>%
    dplyr::distinct(.data$name, .keep_all = TRUE) %>%
    dplyr::arrange(.data$.ord)

  paste(paste0(cks$name, "=", cks$value), collapse = "; ")
}

click_fulltext_candidate_chromote_2 <- function(b, dom_index) {
  if (is.null(dom_index) || length(dom_index) == 0 || is.na(dom_index[[1]])) {
    return(list(clicked = FALSE, reason = "missing_dom_index"))
  }

  expr <- sprintf(
    "(function(){
       try {
         const nodes = Array.from(document.querySelectorAll('a,button,[role=\"button\"]'));
         const el = nodes[%d];
         if (!el) return { clicked: false, reason: 'candidate_not_found' };

         try {
           const oldOpen = window.open;
           window.open = function(u, name, specs) {
             if (u) {
               try { window.location.href = String(u); } catch (e) {}
             }
             return null;
           };
           window.__codex_oldOpen = oldOpen;
         } catch (e) {}

         try { el.scrollIntoView({ block: 'center', inline: 'nearest' }); } catch (e) {}
         try { el.click(); } catch (e) {
           const evt = new MouseEvent('click', { bubbles: true, cancelable: true, view: window });
           el.dispatchEvent(evt);
         }

         return {
           clicked: true,
           href: window.location.href || '',
           tagName: (el.tagName || '').toLowerCase(),
           text: ((el.innerText || el.textContent || '') + '').trim().slice(0, 200)
         };
       } catch (e) {
         return { clicked: false, reason: String(e) };
       }
     })();",
    as.integer(dom_index[[1]])
  )

  x <- tryCatch(js_eval_value_2(b, expr), error = function(e) NULL)
  if (is.null(x) || !is.list(x)) {
    return(list(clicked = FALSE, reason = "click_eval_failed"))
  }
  x
}

extract_pdf_text_from_url_2 <- function(pdf_url,
                                        timeout_sec = 90,
                                        chromote_session = NULL,
                                        referer = NA_character_,
                                        max_html_hops = 2L) {
  tf <- tempfile(fileext = ".pdf")
  on.exit(unlink(tf), add = TRUE)

  out <- tryCatch({
    cookie_header <- NULL
    user_agent <- "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

    if (!is.null(chromote_session)) {
      cookie_header <- tryCatch(
        chromote_cookie_header_2(chromote_session, current_url = referer %||% pdf_url),
        error = function(e) NULL
      )
      user_agent <- tryCatch(chromote_user_agent_2(chromote_session), error = function(e) NA_character_)
    }

    req <- httr2::request(pdf_url) |>
      httr2::req_headers(Accept = "application/pdf,application/octet-stream,*/*") |>
      httr2::req_timeout(timeout_sec) |>
      httr2::req_error(is_error = function(resp) FALSE)

    if (!is.null(cookie_header) && nzchar(cookie_header)) {
      req <- req |> httr2::req_headers(Cookie = cookie_header)
    }
    if (!is.na(referer) && nzchar(trimws(referer))) {
      req <- req |> httr2::req_headers(Referer = referer)
    }
    if (!is.na(user_agent) && nzchar(trimws(user_agent))) {
      req <- req |> httr2::req_headers(`User-Agent` = user_agent)
    }
    # Force HTTP/1.1 for PDF endpoints that break under HTTP/2 (americaspg).
    # curl constant `2L` corresponds to CURL_HTTP_VERSION_1_1.
    req <- req |>
      httr2::req_options(
        http_version = 2L,
        fresh_connect = 1L,
        forbid_reuse = 1L
      )

    resp <- httr2::req_perform(req)

    st <- httr2::resp_status(resp)
    if (st < 200 || st >= 300) stop(sprintf("HTTP %s", st))

    raw_body <- httr2::resp_body_raw(resp)
    ctype <- tolower(httr2::resp_header(resp, "content-type") %||% "")
    cdisp <- tolower(httr2::resp_header(resp, "content-disposition") %||% "")
    final_url <- tryCatch(as.character(httr2::resp_url(resp)), error = function(e) as.character(pdf_url))
    head_n <- min(length(raw_body), 4096L)
    head_raw <- if (head_n > 0) raw_body[seq_len(head_n)] else raw()
    head_chr <- tryCatch(rawToChar(head_raw), error = function(e) "")
    is_pdf_sig <- tryCatch(length(grepRaw(charToRaw("%PDF"), head_raw, fixed = TRUE)) > 0, error = function(e) FALSE)
    is_pdf_hint <- grepl("pdf", ctype, fixed = TRUE) ||
      grepl("\\.pdf", cdisp, ignore.case = TRUE) ||
      grepl("filename\\*?=.*pdf", cdisp, ignore.case = TRUE) ||
      grepl("\\.pdf($|\\?)", final_url, ignore.case = TRUE)

    # Some "download" links return an HTML wrapper/redirect page first. Mine the HTML for a real PDF URL and retry.
    htmlish <- grepl("html", ctype, fixed = TRUE) || grepl("<html|<!doctype html", tolower(head_chr))
    if (!is_pdf_sig && htmlish && as.integer(max_html_hops %||% 0L) > 0L) {
      html_txt <- tryCatch(httr2::resp_body_string(resp), error = function(e) NA_character_)
      if (!is.na(html_txt) && nzchar(html_txt)) {
        hop_urls <- extract_pdfish_urls_from_static_html_2(
          html = html_txt,
          base_url = final_url %||% pdf_url,
          max_n = 20
        )
        hop_urls <- unique(unlist(lapply(hop_urls, expand_pdf_candidate_urls_2), use.names = FALSE))
        hop_urls <- hop_urls[!is.na(hop_urls) & nzchar(trimws(hop_urls))]
        hop_urls <- unique(hop_urls[sapply(hop_urls, looks_like_http_url_2)])
        hop_urls <- unique(hop_urls[!sapply(hop_urls, looks_like_image_url_2)])
        hop_urls <- setdiff(hop_urls, unique(c(as.character(pdf_url), as.character(final_url))))

        for (u_next in hop_urls) {
          nxt <- extract_pdf_text_from_url_2(
            pdf_url = u_next,
            timeout_sec = timeout_sec,
            chromote_session = chromote_session,
            referer = final_url %||% referer %||% pdf_url,
            max_html_hops = as.integer(max_html_hops) - 1L
          )
          if (isTRUE(nxt$ok)) return(nxt)
        }
      }
    }
    # Do not pass HTML/JS wrapper content into pdftools even if the server lies
    # with a PDF-ish content type or filename.
    if (!is_pdf_sig && htmlish) {
      stop(sprintf(
        "response_is_html_not_pdf content_type=%s content_disposition=%s final_url=%s",
        if (nzchar(ctype)) ctype else "unknown",
        if (nzchar(cdisp)) cdisp else "none",
        if (!is.na(final_url) && nzchar(final_url)) final_url else as.character(pdf_url)
      ))
    }

    # Some endpoints return HTML/JS/text under a PDF-looking URL/content-type.
    # Require a PDF file signature before calling pdftools to avoid Poppler spam.
    if (!is_pdf_sig && is_pdf_hint) {
      stop(sprintf(
        "response_missing_pdf_signature content_type=%s content_disposition=%s final_url=%s",
        if (nzchar(ctype)) ctype else "unknown",
        if (nzchar(cdisp)) cdisp else "none",
        if (!is.na(final_url) && nzchar(final_url)) final_url else as.character(pdf_url)
      ))
    }

    if (!is_pdf_hint && !is_pdf_sig) {
      stop(sprintf(
        "response_not_pdf content_type=%s content_disposition=%s final_url=%s",
        if (nzchar(ctype)) ctype else "unknown",
        if (nzchar(cdisp)) cdisp else "none",
        if (!is.na(final_url) && nzchar(final_url)) final_url else as.character(pdf_url)
      ))
    }

    writeBin(raw_body, tf)
    pages <- pdftools::pdf_text(tf)
    txt <- paste(pages, collapse = "\n\n")
    txt <- normalize_space_2(txt)

    list(
      ok = nzchar(txt),
      text = if (nzchar(txt)) txt else NA_character_,
      reason = if (nzchar(txt)) NA_character_ else "empty_pdf_text",
      used_browser_cookies = isTRUE(!is.null(cookie_header) && nzchar(cookie_header)),
      final_url = final_url %||% as.character(pdf_url),
      content_type = ctype %||% NA_character_,
      content_disposition = cdisp %||% NA_character_
    )
  }, error = function(e) {
    list(
      ok = FALSE,
      text = NA_character_,
      reason = paste0("pdf_error: ", conditionMessage(e)),
      used_browser_cookies = FALSE
    )
  })

  out
}

resolve_url_2 <- function(x, base_url = NA_character_) {
  if (is.null(x) || length(x) == 0 || is.na(x[[1]])) return(NA_character_)
  u <- as.character(x[[1]])
  if (!nzchar(trimws(u))) return(NA_character_)

  if (grepl("^https?://", u, ignore.case = TRUE)) return(u)
  if (grepl("^//", u)) {
    if (!is.na(base_url) && grepl("^https://", base_url, ignore.case = TRUE)) return(paste0("https:", u))
    return(paste0("http:", u))
  }

  if (is.na(base_url) || !nzchar(trimws(base_url))) return(u)

  out <- tryCatch({
    if (requireNamespace("xml2", quietly = TRUE)) {
      xml2::url_absolute(u, base_url)
    } else {
      u
    }
  }, error = function(e) u)

  as.character(out %||% u)
}

first_url_from_text_2 <- function(x) {
  if (is.null(x) || length(x) == 0 || is.na(x[[1]])) return(NA_character_)
  s <- as.character(x[[1]])
  if (!nzchar(s)) return(NA_character_)

  pats <- c(
    "https?://[^\\s'\"<>]+",
    "(?:\\.{1,2}/|/)[^\\s'\"<>]*\\.pdf(?:\\?[^\\s'\"<>]*)?",
    "(?:\\.{1,2}/|/)[^\\s'\"<>]*/(?:viewFile|article/download|pdf|epdf|pdfdirect)/[^\\s'\"<>]+"
  )

  for (pat in pats) {
    m <- regexpr(pat, s, perl = TRUE, ignore.case = TRUE)
    if (!is.na(m[1]) && m[1] > 0) {
      hit <- regmatches(s, m)[[1]]
      hit <- gsub("[\\]\\)>,;]+$", "", hit, perl = TRUE)
      if (nzchar(hit)) return(hit)
    }
  }
  NA_character_
}

normalize_space_2 <- function(x) {
  x <- as.character(x)
  x <- stringr::str_replace_all(x, "\r", "\n")
  x <- stringr::str_replace_all(x, "[ \t]+", " ")
  x <- stringr::str_replace_all(x, "\n{3,}", "\n\n")
  stringr::str_trim(x)
}

fetch_html_static_2 <- function(url, timeout_sec = 30) {
  out <- tryCatch({
    req <- httr2::request(url) |>
      httr2::req_user_agent(
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
      ) |>
      httr2::req_headers(
        Accept = "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        `Accept-Language` = "en-AU,en;q=0.9",
        `Cache-Control` = "no-cache",
        Pragma = "no-cache",
        `Upgrade-Insecure-Requests` = "1"
      ) |>
      httr2::req_timeout(timeout_sec) |>
      httr2::req_error(is_error = function(resp) FALSE)

    resp <- httr2::req_perform(req)
    st <- httr2::resp_status(resp)
    ctype <- tolower(httr2::resp_header(resp, "content-type") %||% "")
    html <- NA_character_

    if (st >= 200 && st < 300 && grepl("html", ctype, fixed = TRUE)) {
      html <- tryCatch(httr2::resp_body_string(resp), error = function(e) NA_character_)
    }

    list(
      ok = st >= 200 && st < 300 && !is.na(html) && nzchar(html),
      status = st,
      final_url = tryCatch(as.character(httr2::resp_url(resp)), error = function(e) as.character(url)),
      content_type = ctype,
      html = html,
      reason = NA_character_
    )
  }, error = function(e) {
    list(
      ok = FALSE,
      status = NA_integer_,
      final_url = as.character(url),
      content_type = NA_character_,
      html = NA_character_,
      reason = paste0("static_fetch_error: ", conditionMessage(e))
    )
  })

  out
}

decode_basic_html_entities_2 <- function(x) {
  x <- as.character(x %||% "")
  x <- gsub("&nbsp;", " ", x, fixed = TRUE)
  x <- gsub("&amp;", "&", x, fixed = TRUE)
  x <- gsub("&lt;", "<", x, fixed = TRUE)
  x <- gsub("&gt;", ">", x, fixed = TRUE)
  x <- gsub("&quot;", "\"", x, fixed = TRUE)
  x <- gsub("&#39;", "'", x, fixed = TRUE)
  x
}

static_html_to_text_2 <- function(html) {
  if (is.null(html) || length(html) == 0 || is.na(html[[1]]) || !nzchar(as.character(html[[1]]))) {
    return(NA_character_)
  }

  h <- as.character(html[[1]])

  # Prefer XML/HTML parsing when xml2 is available.
  if (requireNamespace("xml2", quietly = TRUE)) {
    txt <- tryCatch({
      doc <- xml2::read_html(h)
      bad <- xml2::xml_find_all(doc, ".//script|.//style|.//noscript")
      if (length(bad)) xml2::xml_remove(bad)

      candidates <- xml2::xml_find_all(
        doc,
        paste0(
          "//*[self::article or self::main or @id='main-content' or @id='content' or @id='main' or ",
          "contains(concat(' ', normalize-space(@class), ' '), ' article-body ') or ",
          "contains(concat(' ', normalize-space(@class), ' '), ' article__body ') or ",
          "contains(concat(' ', normalize-space(@class), ' '), ' article-content ') or ",
          "contains(concat(' ', normalize-space(@class), ' '), ' main-content ') or ",
          "contains(concat(' ', normalize-space(@class), ' '), ' page-content ')]"
        )
      )

      best <- NULL
      best_n <- 0L
      if (length(candidates)) {
        for (node in candidates) {
          t <- xml2::xml_text(node)
          n <- nchar(t %||% "")
          if (is.finite(n) && n > best_n) {
            best <- t
            best_n <- n
          }
        }
      }

      if (is.null(best) || !nzchar(trimws(best))) {
        best <- xml2::xml_text(xml2::xml_find_first(doc, "//body"))
      }
      normalize_space_2(best)
    }, error = function(e) NA_character_)

    if (!is.na(txt) && nzchar(trimws(txt))) return(txt)
  }

  # Regex fallback if xml2 is unavailable or parsing fails.
  h <- gsub("(?is)<!--.*?-->", " ", h, perl = TRUE)
  h <- gsub("(?is)<script\\b[^>]*>.*?</script>", " ", h, perl = TRUE)
  h <- gsub("(?is)<style\\b[^>]*>.*?</style>", " ", h, perl = TRUE)
  h <- gsub("(?i)<br\\s*/?>", "\n", h, perl = TRUE)
  h <- gsub("(?i)</p\\s*>", "\n\n", h, perl = TRUE)
  h <- gsub("(?is)<[^>]+>", " ", h, perl = TRUE)
  h <- decode_basic_html_entities_2(h)
  h <- normalize_space_2(h)
  if (!nzchar(h)) return(NA_character_)
  h
}

looks_like_block_page_text_2 <- function(x) {
  if (is.null(x) || length(x) == 0 || is.na(x[[1]])) return(FALSE)
  y <- tolower(as.character(x[[1]]))
  grepl(
    paste(
      "access denied",
      "forbidden",
      "verify you are human",
      "captcha",
      "cloudflare",
      "please enable cookies",
      "enable javascript",
      "request blocked",
      "automated access",
      "automated traffic",
      "suspicious traffic",
      "bot detection",
      sep = "|"
    ),
    y
  )
}

extract_pdfish_urls_from_static_html_2 <- function(html, base_url = NA_character_, max_n = 20) {
  if (is.null(html) || length(html) == 0 || is.na(html[[1]]) || !nzchar(as.character(html[[1]]))) {
    return(character())
  }

  h <- as.character(html[[1]])
  out <- character()

  # Raw URLs anywhere in HTML/text.
  out <- c(
    out,
    extract_pdf_urls_from_text_2(h, max_n = max_n),
    unlist(regmatches(h, gregexpr(
      "https?://[^\\s'\"<>]+/(?:pdf|epdf|pdfdirect)/[^\\s'\"<>]+",
      h, perl = TRUE, ignore.case = TRUE
    )), use.names = FALSE)
  )

  if (requireNamespace("xml2", quietly = TRUE)) {
    doc <- tryCatch(xml2::read_html(h), error = function(e) NULL)
    if (!is.null(doc)) {
      nodes <- xml2::xml_find_all(doc, ".//a[@href] | .//iframe[@src] | .//embed[@src] | .//object[@data]")
      if (length(nodes)) {
        scored <- purrr::map_dfr(nodes, function(node) {
          nm <- tolower(xml2::xml_name(node) %||% "")
          href <- if (nm == "object") xml2::xml_attr(node, "data") else xml2::xml_attr(node, "href") %||% xml2::xml_attr(node, "src")
          href <- as.character(href %||% NA_character_)
          href_abs <- resolve_url_2(href, base_url = base_url)
          txt <- tryCatch(xml2::xml_text(node), error = function(e) "")
          title <- as.character(xml2::xml_attr(node, "title") %||% "")
          aria <- as.character(xml2::xml_attr(node, "aria-label") %||% "")
          cls <- as.character(xml2::xml_attr(node, "class") %||% "")
          onclick <- as.character(xml2::xml_attr(node, "onclick") %||% "")
          img <- tryCatch(xml2::xml_find_first(node, ".//img"), error = function(e) NULL)
          img_alt <- if (!is.null(img) && inherits(img, "xml_node")) as.character(xml2::xml_attr(img, "alt") %||% "") else ""
          img_src <- if (!is.null(img) && inherits(img, "xml_node")) as.character(xml2::xml_attr(img, "src") %||% "") else ""
          aux_url <- resolve_url_2(first_url_from_text_2(paste(onclick, cls, img_alt, img_src, sep = " ")), base_url = base_url)
          parent_text <- tryCatch(as.character(xml2::xml_text(xml2::xml_parent(node)) %||% ""), error = function(e) "")

          blob <- tolower(paste(href, href_abs, txt, title, aria, cls, onclick, img_alt, img_src, parent_text, aux_url, collapse = " "))
          if (!nzchar(blob)) {
            return(tibble::tibble(url = character(), score = numeric()))
          }

          score <- 0
          if (grepl("\\bpdf\\b|epdf|pdfdirect", blob)) score <- score + 80
          if (grepl("download\\s*article|download\\s*full\\s*paper|download\\s*full\\s*text|article\\s*pdf", blob)) score <- score + 70
          if (grepl("/pdf/|/epdf/|/pdfdirect/|/viewfile/|/article/download/", tolower(href_abs %||% ""))) score <- score + 60
          if (grepl("pdf[-_ ]?icon|fa-file-pdf|bi-file-pdf|material-icons", blob)) score <- score + 40

          candidates <- unique(as.character(unlist(c(href_abs, aux_url, href), use.names = FALSE)))
          candidates <- candidates[!is.na(candidates) & nzchar(trimws(candidates))]
          keep_candidates <- vapply(candidates, function(u) {
            looks_like_pdf_url_2(u) || grepl("/viewfile/|/article/download/", u, ignore.case = TRUE)
          }, logical(1))
          candidates <- candidates[keep_candidates]
          if (!length(candidates) && score < 60) {
            return(tibble::tibble(url = character(), score = numeric()))
          }
          if (!length(candidates)) {
            candidates <- unique(as.character(unlist(c(href_abs, aux_url), use.names = FALSE)))
            candidates <- candidates[!is.na(candidates) & nzchar(trimws(candidates))]
          }

          tibble::tibble(
            url = candidates,
            score = as.numeric(score)
          )
        })

        if (nrow(scored)) {
          scored <- scored %>%
            dplyr::filter(!is.na(.data$url), nzchar(.data$url)) %>%
            dplyr::mutate(url = as.character(.data$url)) %>%
            dplyr::arrange(dplyr::desc(.data$score)) %>%
            dplyr::distinct(.data$url, .keep_all = TRUE)
          out <- c(out, scored$url)
        }
      }
    }
  }

  out <- out[!is.na(out) & nzchar(trimws(out))]
  out <- gsub("&amp;", "&", out, fixed = TRUE)
  out <- out[!vapply(out, looks_like_image_url_2, logical(1))]
  out <- unique(out)
  if (length(out) > max_n) out <- out[seq_len(max_n)]
  out
}

fetch_visible_text_static_2 <- function(url, timeout_sec = TIMEOUT_SEC) {
  stat <- fetch_html_static_2(url, timeout_sec = timeout_sec)
  debug_log <- character()
  debug_log <- c(
    debug_log,
    paste0(
      "static_fetch status=", stat$status %||% NA_integer_,
      " final_url=", stat$final_url %||% url,
      " content_type=", stat$content_type %||% NA_character_
    )
  )
  pdf_attempt_rows <- list()
  pdf_failure_rows <- list()
  pdfish_urls_found <- character()

  if (!isTRUE(stat$ok) || is.na(stat$html) || !nzchar(stat$html)) {
    return(list(
      ok = FALSE,
      text = NA_character_,
      source = "STATIC_HTML_FETCH_FAILED",
      reason = stat$reason %||% if (!is.na(stat$status)) paste0("HTTP ", stat$status) else "static_fetch_failed",
      status = stat$status %||% NA_integer_,
      final_url = stat$final_url %||% url,
      debug = list(
        log = c(debug_log, paste0("static_fetch_failed reason=", stat$reason %||% "unknown")),
        pdfish_urls = tibble::tibble(url = character()),
        pdf_attempts = tibble::tibble(),
        pdf_failures = tibble::tibble()
      )
    ))
  }

  txt <- static_html_to_text_2(stat$html)
  debug_log <- c(debug_log, paste0("static_html_text_chars=", text_len_2(txt)))
  if (is.na(txt) || !nzchar(trimws(txt))) {
    return(list(
      ok = FALSE,
      text = NA_character_,
      source = "STATIC_HTML_PARSE_EMPTY",
      reason = "empty_static_html_text",
      status = stat$status %||% NA_integer_,
      final_url = stat$final_url %||% url,
      debug = list(
        log = c(debug_log, "static_html_text_empty"),
        pdfish_urls = tibble::tibble(url = character()),
        pdf_attempts = tibble::tibble(),
        pdf_failures = tibble::tibble()
      )
    ))
  }

  # Static HTML fallback can still expose direct PDF links (including icon-only links and raw PDF URLs).
  pdfish_urls <- extract_pdfish_urls_from_static_html_2(
    stat$html,
    base_url = stat$final_url %||% url,
    max_n = 20
  )
  pdfish_urls_found <- pdfish_urls
  debug_log <- c(
    debug_log,
    paste0(
      "static_pdfish_url_count=", length(pdfish_urls),
      if (length(pdfish_urls)) paste0(" top=", paste(utils::head(pdfish_urls, 5), collapse = " | ")) else ""
    )
  )

  if (length(pdfish_urls)) {
    expanded <- unique(unlist(lapply(pdfish_urls, expand_pdf_candidate_urls_2), use.names = FALSE))
    expanded <- expanded[!is.na(expanded) & nzchar(trimws(expanded))]
    expanded <- unique(expanded[sapply(expanded, looks_like_http_url_2)])
    expanded <- unique(expanded[!sapply(expanded, looks_like_image_url_2)])
    debug_log <- c(
      debug_log,
      paste0("static_pdf_try_url_count=", length(expanded),
             if (length(expanded)) paste0(" top=", paste(utils::head(expanded, 6), collapse = " | ")) else "")
    )

    for (u_try in expanded) {
      pdf_res <- extract_pdf_text_from_url_2(
        pdf_url = u_try,
        referer = stat$final_url %||% url
      )
      pdf_attempt_rows[[length(pdf_attempt_rows) + 1]] <- tibble::tibble(
        url = as.character(u_try),
        referer = as.character(stat$final_url %||% url),
        ok = isTRUE(pdf_res$ok),
        chars = if (!is.na(pdf_res$text %||% NA_character_)) text_len_2(pdf_res$text) else NA_integer_,
        reason = as.character(pdf_res$reason %||% NA_character_)
      )
      if (!isTRUE(pdf_res$ok)) {
        pdf_failure_rows[[length(pdf_failure_rows) + 1]] <- tibble::tibble(
          context = "static_html_pdf_link",
          url = as.character(u_try),
          referer = as.character(stat$final_url %||% url),
          reason = as.character(pdf_res$reason %||% "pdf_error")
        )
      }
      if (isTRUE(pdf_res$ok) &&
          !is.na(pdf_res$text) &&
          nzchar(trimws(pdf_res$text))) {
        return(list(
          ok = TRUE,
          text = pdf_res$text,
          source = "PDF_TEXT_FROM_STATIC_HTML_LINK",
          reason = pdf_res$reason %||% NA_character_,
          status = stat$status %||% NA_integer_,
          final_url = stat$final_url %||% url,
          debug = list(
            log = c(debug_log, paste0("static_pdf_success url=", u_try, " chars=", text_len_2(pdf_res$text))),
            pdfish_urls = tibble::tibble(url = as.character(pdfish_urls_found)),
            pdf_attempts = if (length(pdf_attempt_rows)) dplyr::bind_rows(pdf_attempt_rows) else tibble::tibble(),
            pdf_failures = if (length(pdf_failure_rows)) dplyr::bind_rows(pdf_failure_rows) else tibble::tibble()
          )
        ))
      }
    }
  }

  list(
    ok = TRUE,
    text = txt,
    source = "STATIC_HTML_FALLBACK",
    reason = NA_character_,
    status = stat$status %||% NA_integer_,
    final_url = stat$final_url %||% url,
    debug = list(
      log = debug_log,
      pdfish_urls = tibble::tibble(url = as.character(pdfish_urls_found)),
      pdf_attempts = if (length(pdf_attempt_rows)) dplyr::bind_rows(pdf_attempt_rows) else tibble::tibble(),
      pdf_failures = if (length(pdf_failure_rows)) dplyr::bind_rows(pdf_failure_rows) else tibble::tibble()
    )
  )
}

chromote_available_2 <- function() requireNamespace("chromote", quietly = TRUE)

fetch_visible_text_chromote_2 <- function(
  url,
  wait_sec = 6,
  scroll = TRUE,
  scroll_steps = 10,
  scroll_pause_sec = 0.35,
  min_chars_for_success = 1200,
  max_link_tries = 12
) {
  if (!chromote_available_2()) {
    return(list(ok = FALSE, text = NA_character_, reason = "chromote_not_installed"))
  }

  b <- NULL
  debug_log <- character()
  pdf_failures <- list()
  candidate_snapshot <- tibble::tibble()

  out <- tryCatch({
    b <- chromote::ChromoteSession$new()
    b$Page$navigate(url = url)

    landing <- extract_inner_text_from_session_2(
      b, wait_sec = wait_sec, scroll = scroll,
      scroll_steps = scroll_steps, scroll_pause_sec = scroll_pause_sec
    )
    landing_url <- landing$href %||% url

    best_text <- landing$text
    best_mode <- "HTML_LANDING_INNERTEXT"
    best_reason <- NA_character_
    if (!is.na(landing$body_text) &&
        text_len_2(landing$body_text) > (text_len_2(landing$text) + 500L) &&
        !isTRUE(looks_like_block_page_text_2(landing$body_text))) {
      best_text <- landing$body_text
      best_mode <- "HTML_LANDING_BODY_INNERTEXT"
    }
    debug_log <- c(
      debug_log,
      paste0(
        "landing selector=", landing$picked_selector %||% "",
        " picked_chars=", text_len_2(landing$text),
        " body_chars=", text_len_2(landing$body_text),
        " url=", landing_url %||% url
      )
    )

    try_pdf_extract_local <- function(pdf_url, referer, context) {
      if (is.null(pdf_url) || length(pdf_url) == 0 || is.na(pdf_url[[1]]) || !nzchar(trimws(as.character(pdf_url[[1]])))) {
        msg <- "pdf_error: missing_pdf_url"
        pdf_failures[[length(pdf_failures) + 1]] <<- tibble::tibble(
          context = as.character(context),
          url = NA_character_,
          referer = as.character(referer %||% NA_character_),
          reason = msg
        )
        return(list(ok = FALSE, text = NA_character_, reason = msg, used_browser_cookies = FALSE))
      }

      res <- extract_pdf_text_from_url_2(
        as.character(pdf_url[[1]]),
        chromote_session = b,
        referer = referer
      )
      if (!isTRUE(res$ok)) {
        pdf_failures[[length(pdf_failures) + 1]] <<- tibble::tibble(
          context = as.character(context),
          url = as.character(pdf_url[[1]]),
          referer = as.character(referer %||% NA_character_),
          reason = as.character(res$reason %||% "pdf_error")
        )
      }
      res
    }

    adopt_pdf_result_local <- function(pdf_res, mode_plain, mode_cookie, stop_if_good = TRUE) {
      if (!isTRUE(pdf_res$ok) || is.na(pdf_res$text)) return(FALSE)
      pdf_chars <- text_len_2(pdf_res$text)
      if (pdf_chars < 800L) return(FALSE)
      current_best_is_pdf <- !is.na(best_mode %||% NA_character_) && grepl("^PDF_", best_mode %||% "")
      prefer_pdf_over_nonpdf <- !current_best_is_pdf && pdf_chars >= 1200L
      if (pdf_chars <= text_len_2(best_text) && !prefer_pdf_over_nonpdf) return(FALSE)
      best_text <<- pdf_res$text
      best_mode <<- if (isTRUE(pdf_res$used_browser_cookies)) mode_cookie else mode_plain
      best_reason <<- pdf_res$reason
      if (isTRUE(stop_if_good) && text_len_2(best_text) >= min_chars_for_success) return(TRUE)
      FALSE
    }

    try_pdf_urls_local <- function(urls,
                                   referer,
                                   context_prefix,
                                   mode_plain,
                                   mode_cookie) {
      if (length(urls) == 0) return(FALSE)
      urls <- urls[!is.na(urls) & nzchar(trimws(urls))]
      if (!length(urls)) return(FALSE)

      expanded <- unique(unlist(lapply(urls, expand_pdf_candidate_urls_2), use.names = FALSE))
      expanded <- expanded[!is.na(expanded) & nzchar(trimws(expanded))]
      expanded <- unique(expanded[sapply(expanded, looks_like_http_url_2)])
      expanded <- unique(expanded[!sapply(expanded, looks_like_image_url_2)])
      if (!length(expanded)) return(FALSE)

      debug_log <<- c(
        debug_log,
        paste0(context_prefix, " try_urls=", paste(utils::head(expanded, 6), collapse = " | "))
      )

      for (u_try in expanded) {
        pdf_res <- try_pdf_extract_local(
          u_try,
          referer = referer,
          context = paste0(context_prefix, "::", u_try)
        )
        hit_threshold <- adopt_pdf_result_local(
          pdf_res,
          mode_plain = mode_plain,
          mode_cookie = mode_cookie,
          stop_if_good = TRUE
        )
        if (isTRUE(hit_threshold)) return(TRUE)
      }
      FALSE
    }

    try_pdf_discovery_on_current_page_local <- function(page_info,
                                                        page_url,
                                                        context_prefix = "page",
                                                        include_nested_candidates = TRUE,
                                                        nested_limit = 5) {
      if (is.null(page_info) || !is.list(page_info)) return(FALSE)

      # 1) Raw URLs visible in the rendered text/body.
      raw_urls <- unique(c(
        extract_pdf_urls_from_text_2(page_info$text, max_n = 8),
        extract_pdf_urls_from_text_2(page_info$body_text, max_n = 8)
      ))
      if (length(raw_urls)) {
        if (isTRUE(try_pdf_urls_local(
          raw_urls,
          referer = page_url,
          context_prefix = paste0(context_prefix, "_raw"),
          mode_plain = "PDF_TEXT_FROM_RENDERED_URL",
          mode_cookie = "PDF_TEXT_FROM_RENDERED_URL_WITH_BROWSER_COOKIES"
        ))) return(TRUE)
      }

      # 2) Embedded/browser viewer URLs.
      embeds_now <- tryCatch(find_embedded_pdf_url_chromote_2(b), error = function(e) tibble::tibble())
      if (nrow(embeds_now) > 0) {
        if (isTRUE(try_pdf_urls_local(
          embeds_now$url,
          referer = page_url,
          context_prefix = paste0(context_prefix, "_embed"),
          mode_plain = "PDF_TEXT_FROM_EMBEDDED_VIEWER",
          mode_cookie = "PDF_TEXT_FROM_EMBEDDED_VIEWER_WITH_BROWSER_COOKIES"
        ))) return(TRUE)
      }

      # 3) DOM HTML scan for script-configured PDF/EPDF URLs (e.g., Wiley/IOP viewers).
      dom_pdfish <- tryCatch(extract_pdfish_urls_from_dom_html_chromote_2(b), error = function(e) tibble::tibble())
      if (nrow(dom_pdfish) > 0) {
        if (isTRUE(try_pdf_urls_local(
          dom_pdfish$url,
          referer = page_url,
          context_prefix = paste0(context_prefix, "_domhtml"),
          mode_plain = "PDF_TEXT_FROM_DOM_DISCOVERY",
          mode_cookie = "PDF_TEXT_FROM_DOM_DISCOVERY_WITH_BROWSER_COOKIES"
        ))) return(TRUE)
      }

      if (!isTRUE(include_nested_candidates)) return(FALSE)

      # 4) Re-scan the current page for a second-level PDF link (ETASR/OJS viewer pages).
      nested <- tryCatch(find_fulltext_candidates_chromote_2(b, max_n = nested_limit), error = function(e) tibble::tibble())
      if (nrow(nested) == 0) return(FALSE)
      debug_log <<- c(debug_log, paste0(context_prefix, " nested_candidate_count=", nrow(nested)))

      for (j in seq_len(nrow(nested))) {
        href_j <- nested$href[j] %||% NA_character_
        aux_j <- nested$aux_url[j] %||% NA_character_
        href_raw_j <- nested$href_raw[j] %||% NA_character_
        text_j <- nested$text[j] %||% NA_character_
        attrs_j <- nested$attrs_blob[j] %||% NA_character_
        pdfish_j <- isTRUE(candidate_prefers_pdf_attempt_2(
          href = href_j,
          text = text_j,
          aux_url = aux_j,
          attrs_blob = attrs_j
        ))
        if (!pdfish_j && !isTRUE(has_usable_href_2(href_j)) && !isTRUE(has_usable_href_2(aux_j))) next

        urls_j <- c(href_j, aux_j, href_raw_j)
        if (isTRUE(try_pdf_urls_local(
          urls_j,
          referer = page_url,
          context_prefix = paste0(context_prefix, "_nested#", j),
          mode_plain = "PDF_TEXT_FROM_NESTED_PAGE_LINK",
          mode_cookie = "PDF_TEXT_FROM_NESTED_PAGE_LINK_WITH_BROWSER_COOKIES"
        ))) return(TRUE)
      }

      FALSE
    }

    # Some pages expose the full-text PDF URL directly in rendered text (not a robust anchor/button).
    pdf_urls_in_text <- unique(c(
      extract_pdf_urls_from_text_2(landing$text, max_n = 5),
      extract_pdf_urls_from_text_2(landing$body_text, max_n = 5)
    ))
    if (length(pdf_urls_in_text)) {
      debug_log <- c(debug_log, paste0("raw_pdf_urls_in_text=", paste(utils::head(pdf_urls_in_text, 5), collapse = " | ")))
    }
    if (length(pdf_urls_in_text)) {
      try_pdf_urls_local(
        pdf_urls_in_text,
        referer = landing_url %||% url,
        context_prefix = "landing_raw_pdf_url",
        mode_plain = "PDF_TEXT_FROM_RENDERED_URL",
        mode_cookie = "PDF_TEXT_FROM_RENDERED_URL_WITH_BROWSER_COOKIES"
      )
    }

    # Also inspect landing-page embeds and DOM HTML for hidden viewer URLs.
    try_pdf_discovery_on_current_page_local(
      page_info = landing,
      page_url = landing_url %||% url,
      context_prefix = "landing_page",
      include_nested_candidates = FALSE
    )

    cands <- find_fulltext_candidates_chromote_2(b, max_n = max_link_tries)
    current_page_url <- landing_url
    candidate_snapshot <- if (nrow(cands) > 0) {
      cands %>%
        dplyr::transmute(
          rank = dplyr::row_number(),
          score = .data$score,
          tag = .data$tag_name,
          text = .data$text,
          href = .data$href,
          aux_url = dplyr::coalesce(.data$aux_url, NA_character_),
          href_raw = dplyr::coalesce(.data$href_raw, NA_character_),
          onclick = dplyr::coalesce(.data$onclick, NA_character_)
        )
    } else {
      tibble::tibble()
    }
    if (nrow(candidate_snapshot)) {
      debug_log <- c(debug_log, paste0("candidate_count=", nrow(candidate_snapshot)))
    } else {
      debug_log <- c(debug_log, "candidate_count=0")
    }

    if (nrow(cands) > 0) {
      for (i in seq_len(nrow(cands))) {
        href_i <- cands$href[i]
        aux_i <- cands$aux_url[i] %||% NA_character_
        href_raw_i <- cands$href_raw[i] %||% NA_character_
        href_try_i <- if (isTRUE(has_usable_href_2(href_i))) {
          href_i
        } else if (isTRUE(has_usable_href_2(aux_i))) {
          aux_i
        } else {
          NA_character_
        }
        cand_text <- cands$text[i] %||% NA_character_
        cand_pdfish <- isTRUE(candidate_prefers_pdf_attempt_2(
          href = href_i,
          text = cand_text,
          aux_url = aux_i,
          attrs_blob = cands$attrs_blob[i] %||% NA_character_
        ))
        debug_log <- c(
          debug_log,
          paste0(
            "cand#", i,
            " score=", cands$score[i] %||% NA_real_,
            " text=", substr(as.character(cand_text %||% ""), 1, 120),
            " href=", as.character(href_i %||% ""),
            " aux=", as.character(aux_i %||% ""),
            " href_raw=", as.character(href_raw_i %||% ""),
            " pdfish=", cand_pdfish
          )
        )

        if (isTRUE(has_usable_href_2(href_try_i))) {
          # Try direct PDF parsing first for explicit/likely PDF links (incl. OJS viewFile)
          if (cand_pdfish) {
            try_pdf_urls_local(
              urls = c(href_try_i, href_i, aux_i, href_raw_i),
              referer = current_page_url %||% landing_url %||% url,
              context_prefix = paste0("candidate_direct#", i),
              mode_plain = "PDF_TEXT_FROM_LINK",
              mode_cookie = "PDF_TEXT_FROM_LINK_WITH_BROWSER_COOKIES"
            )
            # If the URL is clearly a PDF and direct extraction succeeded, no need to navigate it in-browser.
            if (grepl("^PDF_", best_mode) &&
                (isTRUE(looks_like_pdf_url_2(href_try_i)) || isTRUE(looks_like_pdf_url_2(href_i)) || isTRUE(looks_like_pdf_url_2(aux_i))) &&
                text_len_2(best_text) >= min_chars_for_success) next
          }

          # HTML full-text path
          try(b$Page$navigate(url = href_try_i), silent = TRUE)
          page_i <- extract_inner_text_from_session_2(
            b, wait_sec = wait_sec, scroll = scroll,
            scroll_steps = scroll_steps, scroll_pause_sec = scroll_pause_sec
          )
          current_page_url <- page_i$href %||% href_try_i

          # Some links resolve to a PDF URL or a PDF response (e.g., OJS viewFile without .pdf in URL).
          if (looks_like_pdf_url_2(current_page_url) || is_pdf_content_type_2(page_i$contentType) || cand_pdfish) {
            try_pdf_urls_local(
              urls = c(current_page_url, href_try_i, href_i, aux_i, href_raw_i),
              referer = href_try_i,
              context_prefix = paste0("candidate_followed#", i),
              mode_plain = "PDF_TEXT_FROM_FOLLOWED_LINK",
              mode_cookie = "PDF_TEXT_FROM_FOLLOWED_LINK_WITH_BROWSER_COOKIES"
            )
          }

          # Embedded PDF viewer case: PDF endpoint is hidden in iframe/embed/object source.
          try_pdf_discovery_on_current_page_local(
            page_info = page_i,
            page_url = current_page_url %||% href_try_i,
            context_prefix = paste0("candidate_page#", i),
            include_nested_candidates = TRUE,
            nested_limit = 6
          )
          if (grepl("^PDF_", best_mode) && text_len_2(best_text) >= min_chars_for_success) break

          page_i_best_html <- page_i$text
          if (!is.na(page_i$body_text) && text_len_2(page_i$body_text) > text_len_2(page_i_best_html) + 300L) {
            page_i_best_html <- page_i$body_text
          }
          if (!is.na(page_i_best_html) && text_len_2(page_i_best_html) > text_len_2(best_text)) {
            best_text <- page_i_best_html
            best_mode <- "HTML_FOLLOWED_FULLTEXT_LINK"
            best_reason <- NA_character_
          }
          next
        }

        # JS-only button fallback (no usable href): click and re-check URL/content.
        dom_index_i <- cands$dom_index[i] %||% NA_integer_
        if (is.na(dom_index_i)) next

        # If we navigated away while evaluating earlier candidates, return to landing page
        # so the saved DOM index still maps to the original controls.
        if (!is.na(landing_url) && !is.na(current_page_url) && !identical(landing_url, current_page_url)) {
          try(b$Page$navigate(url = landing_url), silent = TRUE)
          back_page <- extract_inner_text_from_session_2(
            b, wait_sec = wait_sec, scroll = FALSE,
            scroll_steps = scroll_steps, scroll_pause_sec = scroll_pause_sec
          )
          current_page_url <- back_page$href %||% landing_url
        }

        click_res <- click_fulltext_candidate_chromote_2(b, dom_index_i)
        if (!isTRUE(click_res$clicked)) next

        page_click <- extract_inner_text_from_session_2(
          b, wait_sec = wait_sec, scroll = scroll,
          scroll_steps = scroll_steps, scroll_pause_sec = scroll_pause_sec
        )
        current_page_url <- page_click$href %||% current_page_url %||% landing_url

        if (looks_like_pdf_url_2(current_page_url) || is_pdf_content_type_2(page_click$contentType)) {
          try_pdf_urls_local(
            urls = c(current_page_url),
            referer = landing_url %||% url,
            context_prefix = paste0("js_click_url#", i),
            mode_plain = "PDF_TEXT_FROM_JS_CLICK_URL",
            mode_cookie = "PDF_TEXT_FROM_JS_CLICK_URL_WITH_BROWSER_COOKIES"
          )
        }

        try_pdf_discovery_on_current_page_local(
          page_info = page_click,
          page_url = current_page_url %||% landing_url %||% url,
          context_prefix = paste0("js_click_page#", i),
          include_nested_candidates = TRUE,
          nested_limit = 6
        )
        if (grepl("^PDF_", best_mode) && text_len_2(best_text) >= min_chars_for_success) break

        page_click_best_html <- page_click$text
        if (!is.na(page_click$body_text) && text_len_2(page_click$body_text) > text_len_2(page_click_best_html) + 300L) {
          page_click_best_html <- page_click$body_text
        }
        if (!is.na(page_click_best_html) && text_len_2(page_click_best_html) > text_len_2(best_text)) {
          best_text <- page_click_best_html
          best_mode <- if (looks_like_pdf_url_2(current_page_url)) {
            "HTML_AFTER_JS_CLICK_FALLBACK_ON_PDF_VIEWER"
          } else {
            "HTML_JS_CLICK_FALLBACK_INNERTEXT"
          }
          best_reason <- if (!is.na(cand_text) && nzchar(trimws(cand_text))) paste0("clicked: ", cand_text) else NA_character_
        }

        # Some buttons reveal a PDF/HTML href after click (menu/modal). Re-scan once.
        cands_after_click <- find_fulltext_candidates_chromote_2(b, max_n = 3)
        if (nrow(cands_after_click) > 0) {
          for (j in seq_len(nrow(cands_after_click))) {
            href_j <- cands_after_click$href[j] %||% NA_character_
            aux_j <- cands_after_click$aux_url[j] %||% NA_character_
            href_raw_j <- cands_after_click$href_raw[j] %||% NA_character_
            if (!isTRUE(candidate_prefers_pdf_attempt_2(
              href = href_j,
              text = cands_after_click$text[j] %||% NA_character_,
              aux_url = aux_j,
              attrs_blob = cands_after_click$attrs_blob[j] %||% NA_character_
            ))) next

            try_pdf_urls_local(
              urls = c(href_j, aux_j, href_raw_j),
              referer = current_page_url %||% landing_url %||% url,
              context_prefix = paste0("postclick_link#", i, "#", j),
              mode_plain = "PDF_TEXT_FROM_POSTCLICK_LINK",
              mode_cookie = "PDF_TEXT_FROM_POSTCLICK_LINK_WITH_BROWSER_COOKIES"
            )
            if (text_len_2(best_text) >= min_chars_for_success) break
          }
          if (text_len_2(best_text) >= min_chars_for_success) break
        }
      }
    }

    if (!is.na(best_text) && nzchar(trimws(best_text))) {
      list(
        ok = TRUE,
        text = best_text,
        reason = best_reason,
        mode = best_mode,
        debug = list(
          log = debug_log,
          candidates = candidate_snapshot,
          pdf_failures = if (length(pdf_failures)) dplyr::bind_rows(pdf_failures) else tibble::tibble()
        )
      )
    } else {
      list(
        ok = FALSE,
        text = NA_character_,
        reason = "empty_after_follow_links",
        debug = list(
          log = debug_log,
          candidates = candidate_snapshot,
          pdf_failures = if (length(pdf_failures)) dplyr::bind_rows(pdf_failures) else tibble::tibble()
        )
      )
    }
  }, error = function(e) {
    list(
      ok = FALSE,
      text = NA_character_,
      reason = paste0("chromote_error: ", conditionMessage(e)),
      debug = list(
        log = c(debug_log, paste0("fatal: ", conditionMessage(e))),
        candidates = candidate_snapshot,
        pdf_failures = if (length(pdf_failures)) dplyr::bind_rows(pdf_failures) else tibble::tibble()
      )
    )
  })

  tryCatch(if (!is.null(b)) b$close(), error = function(e) NULL)
  out
}

fetch_visible_text_2 <- function(url, wait_sec = 8, scroll = TRUE) {
  vis <- fetch_visible_text_chromote_2(url = url, wait_sec = wait_sec, scroll = scroll)
  merge_debug_local <- function(chromote_debug = NULL, static_debug = NULL) {
    ch <- if (is.list(chromote_debug)) chromote_debug else list()
    st <- if (is.list(static_debug)) static_debug else list()

    out <- ch

    ch_log <- ch$log %||% character()
    st_log <- st$log %||% character()
    if (length(st_log)) st_log <- paste0("[static] ", st_log)
    out$log <- c(ch_log, st_log)

    if (is.null(out$candidates) || !is.data.frame(out$candidates)) {
      out$candidates <- tibble::tibble()
    }

    ch_fail <- if (is.data.frame(ch$pdf_failures)) tibble::as_tibble(ch$pdf_failures) else tibble::tibble()
    st_fail <- if (is.data.frame(st$pdf_failures)) tibble::as_tibble(st$pdf_failures) else tibble::tibble()
    out$pdf_failures <- dplyr::bind_rows(ch_fail, st_fail)

    if (!is.null(st$pdfish_urls)) out$static_pdfish_urls <- st$pdfish_urls
    if (!is.null(st$pdf_attempts)) out$static_pdf_attempts <- st$pdf_attempts
    out
  }

  chromote_failed <- !isTRUE(vis$ok) || is.na(vis$text) || !nzchar(trimws(vis$text))
  chromote_blockish <- !chromote_failed && isTRUE(looks_like_block_page_text_2(vis$text))
  chromote_short_html <- !chromote_failed &&
    !is.na(vis$mode %||% NA_character_) &&
    !grepl("^PDF_", vis$mode %||% "", ignore.case = FALSE) &&
    text_len_2(vis$text) < 900L

  if (chromote_failed || chromote_blockish || chromote_short_html) {
    static_res <- fetch_visible_text_static_2(url = url, timeout_sec = TIMEOUT_SEC)
    if (isTRUE(static_res$ok) && !is.na(static_res$text) && nzchar(trimws(static_res$text))) {
      if (chromote_failed || text_len_2(static_res$text) > text_len_2(vis$text)) {
        return(list(
          ok = TRUE,
          visible_text = normalize_space_2(static_res$text),
          botBlocked = 0L,
          source = static_res$source %||% "STATIC_HTML_FALLBACK",
          reason = static_res$reason %||% NA_character_,
          debug = merge_debug_local(vis$debug, static_res$debug)
        ))
      }
    }
  }

  if (chromote_failed) {
    return(list(
      ok = FALSE,
      visible_text = NA_character_,
      botBlocked = 1L,
      source = "HEADLESS_RENDER_FAILED",
      reason = vis$reason %||% "unknown",
      debug = vis$debug %||% list()
    ))
  }

  list(
    ok = TRUE,
    visible_text = normalize_space_2(vis$text),
    botBlocked = 0L,
    source = vis$mode %||% "HEADLESS_RENDERED_INNERTEXT",
    reason = NA_character_,
    debug = vis$debug %||% list()
  )
}

extract_pdfish_urls_from_dom_html_chromote_2 <- function(b) {
  expr <- "(function(){
    const html = (document.documentElement && document.documentElement.outerHTML) ? document.documentElement.outerHTML : '';
    const base = document.baseURI || location.href || '';
    if (!html) return [];

    const out = [];
    const seen = new Set();
    const add = (u, where) => {
      if (!u) return;
      let s = String(u).trim();
      if (!s) return;
      s = s.replace(/\\\\\\//g, '/');
      s = s.replace(/^['\\\"]+|['\\\"]+$/g, '');
      if (/^data:/i.test(s) || /^blob:/i.test(s)) return;
      try { s = new URL(s, base).href; } catch(e) {}
      const key = s.toLowerCase();
      if (!key || seen.has(key)) return;
      seen.add(key);
      out.push({ url: s, where: where || 'dom-html' });
    };

    const pats = [
      /https?:\\\\/\\\\/[^\\s'\"<>]+\\\\.pdf(?:\\\\?[^\\s'\"<>]*)?/ig,
      /https?:\\\\/\\\\/[^\\s'\"<>]+\\\\/(?:pdf|epdf|pdfdirect)\\\\/[^\\s'\"<>]+/ig,
      /(?:\\\\.{1,2}\\\\/|\\\\/)[^\\s'\"<>]*\\\\.pdf(?:\\\\?[^\\s'\"<>]*)?/ig,
      /(?:\\\\.{1,2}\\\\/|\\\\/)[^\\s'\"<>]*\\\\/(?:pdf|epdf|pdfdirect|viewFile|article\\\\/download)\\\\/[^\\s'\"<>]+/ig
    ];
    for (const re of pats) {
      const ms = html.match(re) || [];
      for (const m of ms) add(m, 'dom-html');
    }
    return out.slice(0, 25);
  })();"

  vals <- tryCatch(js_eval_value_2(b, expr), error = function(e) NULL)
  if (is.null(vals) || !length(vals)) {
    return(tibble::tibble(url = character(), where = character()))
  }

  purrr::map_dfr(vals, function(v) {
    tibble::tibble(
      url = as.character(v$url %||% NA_character_),
      where = as.character(v$where %||% NA_character_)
    )
  }) %>%
    dplyr::filter(!is.na(.data$url), nzchar(.data$url))
}

extract_all_crude <- function(
  url = NA_character_,
  headless_wait_sec = 8,
  scroll = TRUE
) {
  res <- list(
    visible_text = NA_character_,
    visible_text_source = NA_character_,
    source_used = NA_character_,
    botBlocked = 0L,
    debug = list()
  )

  if (is.na(url) || !nzchar(stringr::str_trim(url))) {
    res$source_used <- "NO_URL"
    return(res)
  }

  vis <- fetch_visible_text_2(url = url, wait_sec = headless_wait_sec, scroll = scroll)

  if (!isTRUE(vis$ok)) {
    res$botBlocked <- 1L
    res$source_used <- vis$source
    res$debug$reason <- vis$reason
    if (is.list(vis$debug)) res$debug <- c(res$debug, vis$debug)
    return(res)
  }

  res$visible_text <- vis$visible_text
  res$visible_text_source <- if (!is.na(vis$source) && grepl("^PDF_", vis$source)) {
    "PDF_TEXT"
  } else if (!is.na(vis$source) && grepl("^STATIC_", vis$source)) {
    "STATIC_HTML_TEXT"
  } else {
    "VISIBLE_INNERTEXT"
  }
  res$source_used <- vis$source
  if (is.list(vis$debug)) res$debug <- c(res$debug, vis$debug)
  res$debug$visible_chars <- nchar(vis$visible_text)

  res
}

## 5) Prompt payload builder ------------------------------------------------
truncate_text_2 <- function(x, max_chars) {
  if (is.na(x) || stringr::str_trim(x) == "") return(NA_character_)
  if (nchar(x) <= max_chars) return(x)
  paste0(substr(x, 1, max_chars), "\n\n[TRUNCATED]")
}

build_prompt_payload_2 <- function(title, authors,
                                   extracted_visible_text = NA_character_,
                                   source_used = NA_character_,
                                   max_prompt_chars = DEFAULT_MAX_PROMPT_CHARS,
                                   max_fulltext_chars = DEFAULT_MAX_FULLTEXT_CHARS) {

  vis_txt2 <- truncate_text_2(extracted_visible_text, max_fulltext_chars)
  text_label <- if (!is.na(source_used) && grepl("^PDF_", source_used)) {
    "Extracted text (PDF text extraction):\n"
  } else if (!is.na(source_used) && grepl("^STATIC_", source_used)) {
    "Extracted text (static HTML fetch fallback):\n"
  } else {
    "Extracted visible text (rendered innerText):\n"
  }

  instruction <-
    "You are a meticulous medical researcher. Use ONLY the provided extracted content which is all visible text rendered from the publisher website; do not guess. If the article identified by the title does not match the article in the extracted text then return 'unclear' for all output."

  parts <- c(
    instruction,
    paste0("Title: ", as.character(title %||% "")),
    if (!is.na(vis_txt2) && nzchar(stringr::str_trim(vis_txt2))) paste0(text_label, vis_txt2) else NA_character_
  )

  parts <- parts[!is.na(parts) & nzchar(stringr::str_trim(parts))]
  payload <- paste(parts, collapse = "\n\n")
  truncate_text_2(payload, max_prompt_chars)
}

## 6) Row preparation and classification ------------------------------------
prepare_row_for_classification_2 <- function(doi, url, title_df, firstAuthor,
                                             headless_wait_sec = HEADLESS_WAIT_SEC,
                                             scroll = USE_SCROLL,
                                             max_prompt_chars = DEFAULT_MAX_PROMPT_CHARS,
                                             max_fulltext_chars = DEFAULT_MAX_FULLTEXT_CHARS) {

  ext <- extract_all_crude(
    url = url,
    headless_wait_sec = headless_wait_sec,
    scroll = scroll
  )

    # Prefer df$title; fall back to first visible line if missing
  title_used <- title_df
  if (is.na(title_used) || !nzchar(stringr::str_trim(title_used))) {
    if (!is.na(ext$visible_text) && nzchar(stringr::str_trim(ext$visible_text))) {
      first_line <- strsplit(ext$visible_text, "\n", fixed = TRUE)[[1]][1] %||% NA_character_
      if (!is.na(first_line) && nzchar(stringr::str_trim(first_line)) && nchar(first_line) <= 220) {
        title_used <- stringr::str_trim(first_line)
      }
    }
  }

  # Skip OpenAI if no usable extracted text
  if (is.na(ext$visible_text) || !nzchar(stringr::str_trim(ext$visible_text)) || nchar(ext$visible_text) < 1200) {
    return(list(
      send_to_openai = FALSE,
      payload = NA_character_,
      result_prefill = blank_result_tibble_2(),
      botBlocked = ext$botBlocked %||% 0L,
      sourceUsed = ext$source_used %||% "unknown",
      debug = ext$debug %||% list()
    ))
  }

  payload <- build_prompt_payload_2(
    title = title_used,
    authors = firstAuthor,
    extracted_visible_text = ext$visible_text,
    source_used = ext$source_used,
    max_prompt_chars = max_prompt_chars,
    max_fulltext_chars = max_fulltext_chars
  )

  list(
    send_to_openai = TRUE,
    payload = payload,
    result_prefill = NULL,
    botBlocked = ext$botBlocked %||% 0L,
    sourceUsed = ext$source_used %||% "unknown",
    debug = ext$debug %||% list()
  )
}

classify_row_2 <- function(doi, url, title_df, firstAuthor,
                           headless_wait_sec = HEADLESS_WAIT_SEC,
                           scroll = USE_SCROLL,
                           max_prompt_chars = DEFAULT_MAX_PROMPT_CHARS,
                           max_fulltext_chars = DEFAULT_MAX_FULLTEXT_CHARS) {

  prep <- prepare_row_for_classification_2(
    doi = doi,
    url = url,
    title_df = title_df,
    firstAuthor = firstAuthor,
    headless_wait_sec = headless_wait_sec,
    scroll = scroll,
    max_prompt_chars = max_prompt_chars,
    max_fulltext_chars = max_fulltext_chars
  )

  if (!isTRUE(prep$send_to_openai)) {
    return(list(
      result     = prep$result_prefill %||% blank_result_tibble_2(),
      botBlocked = prep$botBlocked %||% 0L,
      sourceUsed = prep$sourceUsed %||% "unknown"
    ))
  }

  out <- tryCatch(openai_request_2(prep$payload), error = function(e) NULL)
  if (is.null(out)) out <- blank_result_tibble_2()

  list(
    result     = out,
    botBlocked = prep$botBlocked %||% 0L,
    sourceUsed = prep$sourceUsed %||% "unknown"
  )
}

## 7) Run -------------------------------------------------------------------
if (!dir.exists(CHECKPOINT_DIR)) dir.create(CHECKPOINT_DIR, recursive = TRUE, showWarnings = FALSE)

all_idx <- which(df$score >= 4)
batches <- split(all_idx, ceiling(seq_along(all_idx) / BATCH_SIZE))

df_running <- df
openai_batch_meta_all_2 <- list()

# Phase 1: extract visible text from each paper and save per-batch checkpoints
save_dir <- file.path("batches", "prep_batches")
if (!dir.exists(save_dir)) dir.create(save_dir, recursive = TRUE)
save_every <- 10

for (b in seq_along(batches)) {
  idx <- batches[[b]]
  message(sprintf("Prep pass - Batch %d/%d: %d rows", b, length(batches), length(idx)))

  save_file <- file.path(save_dir, sprintf("prep_batch_b%03d.rds", b))

  if (file.exists(save_file)) {
    prep_batch <- readRDS(save_file)
    if (!is.list(prep_batch) || length(prep_batch) != length(idx)) {
      tmp <- vector("list", length(idx))
      if (is.list(prep_batch)) {
        tmp[seq_len(min(length(prep_batch), length(tmp)))] <- prep_batch[seq_len(min(length(prep_batch), length(tmp)))]
      }
      prep_batch <- tmp
    }
  } else {
    prep_batch <- vector("list", length(idx))
  }

  for (i in seq_along(idx)) {
    if (!is.null(prep_batch[[i]])) next
    ii <- idx[i]
    prep_batch[[i]] <- tryCatch(
      prepare_row_for_classification_2(
        doi         = df_running$DOI[ii],
        url         = df_running$URL[ii],
        title_df    = df_running$title[ii],
        firstAuthor = df_running$firstAuthor[ii]
      ),
      error = function(e) {
        warning(sprintf("Error preparing row %s: %s", ii, e$message))
        list(error = TRUE, error_msg = e$message)
      }
    )

    if (i %% save_every == 0) {
      saveRDS(prep_batch, save_file)
      message(sprintf("Saved prep progress for batch %d (%d/%d)", b, i, length(idx)))
    }
  }

  saveRDS(prep_batch, save_file)
}

message("Preparation pass complete — all prep_batch files written to ", save_dir)

# Phase 2: run OpenAI classification per batch using the saved prep_batch files
for (b in seq_along(batches)) {
  idx <- batches[[b]]
  message(sprintf("Classify pass - Batch %d/%d: %d rows", b, length(batches), length(idx)))

  save_file <- file.path(save_dir, sprintf("prep_batch_b%03d.rds", b))
  if (!file.exists(save_file)) {
    stop(sprintf("Missing prep batch file: %s. Run the preparation pass first.", save_file))
  }

  prep_batch <- readRDS(save_file)

  payload_df_batch <- build_payload_df_from_prep_2(prep_batch, idx)
  class_df_prefilled_batch <- collect_prefilled_class_rows_2(prep_batch, idx)

  meta_df_batch <- tibble::tibble(
    row_id     = idx,
    botBlocked = purrr::map_int(prep_batch, ~ .x$botBlocked %||% NA_integer_),
    sourceUsed = purrr::map_chr(prep_batch, ~ .x$sourceUsed %||% NA_character_),
    sentToOpenAI  = purrr::map_lgl(prep_batch, ~ isTRUE(.x$send_to_openai)),
    extractedChars  = purrr::map_int(prep_batch, ~ .x$debug$visible_chars %||% NA_integer_)
  )

  openai_batch_res <- run_openai_payload_batches_2(
    payload_df = payload_df_batch,
    max_requests_per_batch = OPENAI_BATCH_REQUESTS_PER_JOB,
    poll_seconds = OPENAI_BATCH_POLL_SECONDS,
    max_hours = OPENAI_BATCH_MAX_HOURS,
    label = sprintf("visible_text_batch_%03d", b),
    error_dir = CHECKPOINT_DIR
  )

  class_df_batch <- dplyr::bind_rows(class_df_prefilled_batch, openai_batch_res$class_df) %>%
    dplyr::distinct(.data$row_id, .keep_all = TRUE)

  openai_batch_meta_all_2[[b]] <- openai_batch_res$batches %>%
    dplyr::mutate(local_batch = b, n_rows = length(idx))

  df_running <- merge_new_into_df_2(df_running, class_df_batch, meta_df_batch)

  checkpoint_path <- file.path(CHECKPOINT_DIR, sprintf("labelled_checkpoint_batch_%03d.xlsx", b))
  writexl::write_xlsx(
    df_running %>%
      dplyr::rename(
        visibleTextBotBlocked = botBlocked,
        visibleTextUsed       = sourceUsed,
        visibleTextSentToOpenAI   = sentToOpenAI_newmeta,
        visibleTextChars   = extractedChars_newmeta
      ),
    checkpoint_path
  )

}

df_running_out <- df_running %>%
  dplyr::rename(
        visibleTextBotBlocked = botBlocked,
        visibleTextUsed       = sourceUsed,
        visibleTextSentToOpenAI   = sentToOpenAI_newmeta,
        visibleTextChars   = extractedChars_newmeta
  )

df_running_out <- df_running_out %>%
  dplyr::rowwise() %>%
  dplyr::mutate(
    requestBlocked = as.integer(
      all(
        purrr::map_lgl(
          schema_cols,
          function(col) is_unclear_or_na(.data[[col]])
        )
      )
    )
  ) %>%
  dplyr::ungroup()

writexl::write_xlsx(df_running_out, OUT_FINAL_XLSX)
