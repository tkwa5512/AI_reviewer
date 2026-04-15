# AI Reviewer 3
# Purpose: Extract structured data from each included paper using a three-tier
#   escalating strategy: PMC JATS XML → PubMed XML → publisher HTML (with optional
#   headless rendering). Classification is submitted to OpenAI as a batch job.
# Input:  Dataset 3.xlsx
# Output: Dataset 6.xlsx

library(readxl)
library(writexl)
library(dplyr)
library(purrr)
library(tibble)
library(stringr)
library(httr)
library(httr2)
library(jsonlite)
library(rvest)
library(xml2)

df <- read_xlsx("Dataset 3.xlsx")

`%||%` <- function(a, b) if (!is.null(a) && length(a) && !all(is.na(a))) a else b

## 1) Configuration ---------------------------------------------------------
# Set to your email address for NCBI API polite-use identification (optional)
MAILTO <- ""

DEFAULT_MAX_PROMPT_CHARS   <- 120000  # whole payload cap
DEFAULT_MAX_FULLTEXT_CHARS <- 90000   # full-text cap inside payload

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
TIMEOUT_SEC <- 30
USE_HEADLESS <- TRUE

## 2) OpenAI helpers --------------------------------------------------------
as_chr1 <- function(x) {
  if (is.null(x) || length(x) == 0) return(NA_character_)
  as.character(x[[1]])
}

as_int1 <- function(x) {
  if (is.null(x) || length(x) == 0 || is.na(x[[1]])) return(NA_integer_)
  suppressWarnings(as.integer(x[[1]]))
}

as_list_chr <- function(x) {
  if (is.null(x) || length(x) == 0) return(list(NA_character_))
  list(as.character(unlist(x)))
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

build_openai_responses_body <- function(input_text) {
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

extract_response_text_from_responses_body <- function(parsed) {
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

parse_classifier_output_text_to_tibble <- function(out_txt) {
  tryCatch({
    tmp <- jsonlite::fromJSON(out_txt, simplifyVector = FALSE)
    tibble::tibble(
      country_of_first_affiliation_of_first_author = as_chr1(tmp$country_of_first_affiliation_of_first_author),
      study_design_type = as_chr1(tmp$study_design_type),
      input_data_modality = as_list_chr(tmp$input_data_modality),
      clinical_purpose_model_output = as_list_chr(tmp$clinical_purpose_model_output),
      model_architecture = as_chr1(tmp$model_architecture),
      dataset_origin = as_list_chr(tmp$dataset_origin),
      funding_source = as_list_chr(tmp$funding_source),
      main_accuracy_metric = as_list_chr(tmp$main_accuracy_metric)
    )
  }, error = function(e) NULL)
}

parse_responses_body_to_tibble <- function(parsed_responses_body) {
  out_txt <- extract_response_text_from_responses_body(parsed_responses_body)
  if (is.null(out_txt)) return(NULL)
  parse_classifier_output_text_to_tibble(out_txt)
}

openai_request <- function(input_text,
                           max_retries = OPENAI_MAX_RETRIES,
                           base_wait = OPENAI_BASE_WAIT) {

  stopifnot(nzchar(Sys.getenv("OPENAI_API_KEY")))

  payload <- build_openai_responses_body(input_text)

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
    out <- parse_responses_body_to_tibble(parsed)
    return(out)
  }
}

blank_result_tibble <- function() {
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

empty_class_df <- function() {
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

blank_result_tibble_with_row_id <- function(row_id) {
  dplyr::bind_cols(tibble::tibble(row_id = as.integer(row_id)), blank_result_tibble())
}

## 3) OpenAI Batch API helpers ----------------------------------------------
openai_stop_if_no_key <- function() {
  if (identical(Sys.getenv("OPENAI_API_KEY"), "") || !nzchar(Sys.getenv("OPENAI_API_KEY"))) {
    stop("OPENAI_API_KEY is empty. Set it before running.")
  }
  invisible(TRUE)
}

openai_auth_header_httr <- function() {
  httr::add_headers(Authorization = paste("Bearer", Sys.getenv("OPENAI_API_KEY")))
}

openai_write_batch_input_jsonl <- function(payload_df, path) {
  stopifnot(all(c("row_id", "payload") %in% names(payload_df)))

  tasks <- lapply(seq_len(nrow(payload_df)), function(i) {
    input_txt <- payload_df$payload[[i]]
    if (is.null(input_txt) || (length(input_txt) == 1 && is.na(input_txt))) input_txt <- ""

    list(
      custom_id = paste0("row-", payload_df$row_id[[i]]),
      method = "POST",
      url = "/v1/responses",
      body = build_openai_responses_body(input_txt)
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

openai_upload_batch_file <- function(jsonl_path) {
  res <- httr::POST(
    paste0(OPENAI_API_BASE, "/files"),
    openai_auth_header_httr(),
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

openai_create_batch_job <- function(input_file_id,
                                    completion_window = OPENAI_BATCH_COMPLETION_WINDOW) {
  res <- httr::POST(
    paste0(OPENAI_API_BASE, "/batches"),
    openai_auth_header_httr(),
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

openai_get_batch <- function(batch_id) {
  res <- httr::GET(
    paste0(OPENAI_API_BASE, "/batches/", batch_id),
    openai_auth_header_httr(),
    httr::config(connecttimeout = 120, timeout = 120)
  )
  httr::stop_for_status(res)
  httr::content(res, as = "parsed", simplifyVector = FALSE)
}

openai_wait_for_batch <- function(batch_id,
                                  poll_seconds = OPENAI_BATCH_POLL_SECONDS,
                                  max_hours = OPENAI_BATCH_MAX_HOURS) {
  terminal <- c("completed", "failed", "expired", "cancelled")
  t0 <- Sys.time()

  repeat {
    b <- openai_get_batch(batch_id)
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

openai_download_file_text <- function(file_id) {
  res <- httr::GET(
    paste0(OPENAI_API_BASE, "/files/", file_id, "/content"),
    openai_auth_header_httr(),
    httr::config(connecttimeout = 120, timeout = 600)
  )
  httr::stop_for_status(res)
  httr::content(res, as = "text", encoding = "UTF-8")
}

parse_jsonl_text <- function(txt) {
  lines <- strsplit(txt, "\n", fixed = TRUE)[[1]]
  lines <- trimws(lines)
  lines <- lines[nzchar(lines)]
  lapply(lines, function(x) jsonlite::fromJSON(x, simplifyVector = FALSE))
}

openai_parse_batch_output_record <- function(rec) {
  custom_id <- rec$custom_id %||% ""
  row_id <- suppressWarnings(as.integer(sub("^row-", "", custom_id)))
  if (is.na(row_id)) return(NULL)

  make_blank <- function(msg) {
    message("row ", row_id, " batch error: ", msg)
    blank_result_tibble_with_row_id(row_id)
  }

  if (!is.null(rec$error)) {
    return(make_blank(rec$error$message %||% "unknown"))
  }

  if (is.null(rec$response) || is.null(rec$response$status_code) || rec$response$status_code != 200) {
    status <- rec$response$status_code %||% NA_integer_
    return(make_blank(paste0("HTTP status ", status)))
  }

  parsed_row <- parse_responses_body_to_tibble(rec$response$body)
  if (is.null(parsed_row)) {
    return(make_blank("no parseable model JSON output"))
  }

  dplyr::bind_cols(tibble::tibble(row_id = row_id), parsed_row)
}

run_openai_payload_batches <- function(payload_df,
                                       max_requests_per_batch = OPENAI_BATCH_REQUESTS_PER_JOB,
                                       poll_seconds = OPENAI_BATCH_POLL_SECONDS,
                                       max_hours = OPENAI_BATCH_MAX_HOURS,
                                       label = "run",
                                       error_dir = tempdir()) {
  openai_stop_if_no_key()

  if (nrow(payload_df) == 0) {
    return(list(class_df = empty_class_df(), batches = tibble::tibble()))
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
    openai_write_batch_input_jsonl(chunk_df, jsonl_path)

    input_file_id <- openai_upload_batch_file(jsonl_path)
    batch <- openai_create_batch_job(input_file_id)
    message("OpenAI batch created: ", batch$id)

    final_batch <- openai_wait_for_batch(batch$id, poll_seconds = poll_seconds, max_hours = max_hours)

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
      out_text <- openai_download_file_text(final_batch$output_file_id)
      out_records <- parse_jsonl_text(out_text)
      parsed_rows <- purrr::compact(lapply(out_records, openai_parse_batch_output_record))
      chunk_class_df <- if (length(parsed_rows) == 0) empty_class_df() else dplyr::bind_rows(parsed_rows)
    } else {
      chunk_class_df <- empty_class_df()
    }

    if (!is.null(final_batch$error_file_id)) {
      err_text <- openai_download_file_text(final_batch$error_file_id)
      err_path <- file.path(
        error_dir,
        paste0("openai_batch_errors_", safe_label, "_", k, "_", format(Sys.time(), "%Y%m%d_%H%M%S"), ".jsonl")
      )
      writeLines(err_text, err_path, useBytes = TRUE)
      message("Saved OpenAI batch error file to: ", err_path)
    }

    # Ensure every requested row gets a result row (blank if missing)
    missing_ids <- setdiff(chunk_df$row_id, chunk_class_df$row_id %||% integer())
    if (length(missing_ids)) {
      blanks <- dplyr::bind_rows(lapply(missing_ids, blank_result_tibble_with_row_id))
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

collapse_vals <- function(x) {
  x <- unlist(x)
  x <- x[!is.na(x)]
  if (length(x) == 0) return(NA_character_)
  paste(unique(x), collapse = "; ")
}

collapse_new_listcols <- function(df_in) {
  df_in %>%
    mutate(
      input_data_modality_new = purrr::map_chr(input_data_modality_new, collapse_vals),
      clinical_purpose_model_output_new = purrr::map_chr(clinical_purpose_model_output_new, collapse_vals),
      dataset_origin_new = purrr::map_chr(dataset_origin_new, collapse_vals),
      funding_source_new = purrr::map_chr(funding_source_new, collapse_vals),
      main_accuracy_metric_new = purrr::map_chr(main_accuracy_metric_new, collapse_vals)
    )
}

merge_new_into_df <- function(df_base, class_df, meta_df) {

  class_df_new <- class_df %>%
    dplyr::rename_with(~ paste0(.x, "_new"), dplyr::all_of(schema_cols))

  df_out <- df_base %>%
    dplyr::mutate(row_id = dplyr::row_number()) %>%
    dplyr::left_join(class_df_new, by = "row_id") %>%
    dplyr::left_join(
      meta_df %>% dplyr::rename_with(~ paste0(.x, "_newmeta"), -row_id),
      by = "row_id"
    ) %>%
    dplyr::select(-row_id) %>%
    collapse_new_listcols()

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

## 4) Fetch and parse helpers -----------------------------------------------

normalize_space <- function(x) {
  x <- as.character(x)
  x <- str_replace_all(x, "\r", "\n")
  x <- str_replace_all(x, "[ \t]+", " ")
  x <- str_replace_all(x, "\n{3,}", "\n\n")
  str_trim(x)
}

fetch_url_html <- function(url, timeout_sec = 30) {
  req <- httr2::request(url) |>
    httr2::req_headers(
      `User-Agent` = paste(
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
      ),
      `Accept` = "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
      `Accept-Language` = "en-GB,en;q=0.9",
      `Cache-Control` = "no-cache",
      `Pragma` = "no-cache",
      `Upgrade-Insecure-Requests` = "1"
    ) |>
    httr2::req_options(followlocation = TRUE, timeout = timeout_sec)

  resp <- tryCatch(httr2::req_perform(req), error = function(e) NULL)
  if (is.null(resp)) return(list(ok = FALSE, status = NA_integer_, final_url = NA_character_, html = NA_character_))

  status <- httr2::resp_status(resp)
  final_url <- resp$url %||% NA_character_
  if (status >= 400) return(list(ok = FALSE, status = status, final_url = final_url, html = NA_character_))

  html <- tryCatch(httr2::resp_body_string(resp), error = function(e) NA_character_)
  if (is.na(html) || str_trim(html) == "") return(list(ok = FALSE, status = status, final_url = final_url, html = NA_character_))

  list(ok = TRUE, status = status, final_url = final_url, html = html)
}

parse_meta_tags <- function(doc) {
  metas <- rvest::html_elements(doc, "meta")
  if (length(metas) == 0) {
    return(tibble::tibble(key = character(), value = character()))
  }

  out <- purrr::map_dfr(metas, function(m) {
    tibble::tibble(
      key   = dplyr::coalesce(rvest::html_attr(m, "name"),
                             rvest::html_attr(m, "property")),
      value = rvest::html_attr(m, "content")
    )
  })

  out %>%
    dplyr::filter(
      !is.na(.data$key),
      !is.na(.data$value),
      stringr::str_trim(.data$key) != "",
      stringr::str_trim(.data$value) != ""
    )
}

extract_from_meta <- function(metas) {
  pick_first <- function(keys) {
    v <- metas$value[metas$key %in% keys]
    v <- v[!is.na(v) & str_trim(v) != ""]
    if (length(v)) v[[1]] else NA_character_
  }
  pick_many <- function(keys) {
    v <- metas$value[metas$key %in% keys]
    v <- v[!is.na(v) & str_trim(v) != ""]
    unique(v)
  }

  title <- pick_first(c("citation_title", "dc.title", "DC.Title", "og:title", "title"))
  abstract <- pick_first(c("citation_abstract", "dc.description", "description", "og:description"))

  aff <- pick_many(c("citation_author_institution", "citation_author_affiliation"))
  aff <- if (length(aff)) paste(aff, collapse = " | ") else NA_character_

  pubtype <- pick_first(c(
    "citation_article_type", "dc.type", "DC.Type", "article:section", "og:type"
  ))

  funding_many <- pick_many(c(
    "citation_funding", "citation_funder_name", "citation_funder", "dc.relation", "dc.rights", "citation_acknowledgement"
  ))
  funding <- if (length(funding_many)) paste(funding_many, collapse = " | ") else NA_character_

  list(title = title, abstract = abstract, affiliations = aff, funding = funding, pub_type = pubtype)
}

extract_jsonld <- function(doc) {
  txt <- doc |>
    html_elements('script[type="application/ld+json"]') |>
    html_text2()

  parsed <- purrr::map(txt, function(x) {
    x2 <- str_squish(x)
    if (x2 == "") return(NULL)
    tryCatch(jsonlite::fromJSON(x2, simplifyVector = FALSE), error = function(e) NULL)
  })
  parsed[!vapply(parsed, is.null, logical(1))]
}

extract_next_data <- function(doc) {
  nd <- doc |>
    html_elements('script#__NEXT_DATA__') |>
    html_text2()
  nd <- nd[nzchar(str_trim(nd))]
  if (!length(nd)) return(NULL)
  tryCatch(jsonlite::fromJSON(nd[[1]], simplifyVector = FALSE), error = function(e) NULL)
}

extract_initial_state <- function(html) {
  patterns <- c(
    "window\\.__INITIAL_STATE__\\s*=\\s*",
    "__INITIAL_STATE__\\s*=\\s*",
    "window\\.__APOLLO_STATE__\\s*=\\s*"
  )
  for (p in patterns) {
    m <- str_match(html, paste0(p, "(\\{.*\\})\\s*;"))
    if (!is.na(m[1, 2])) {
      json_txt <- m[1, 2]
      json_txt2 <- json_txt |> str_replace_all("(?<!\\\\)'", "\"")
      out <- tryCatch(jsonlite::fromJSON(json_txt, simplifyVector = FALSE), error = function(e) NULL)
      if (!is.null(out)) return(out)
      out2 <- tryCatch(jsonlite::fromJSON(json_txt2, simplifyVector = FALSE), error = function(e) NULL)
      if (!is.null(out2)) return(out2)
    }
  }
  NULL
}

find_values_by_key <- function(x, key_regex, max_n = 200) {
  found <- character(0)

  walk_any <- function(obj) {
    if (length(found) >= max_n) return(invisible())
    if (is.list(obj)) {
      nms <- names(obj)
      if (!is.null(nms)) {
        for (i in seq_along(obj)) {
          nm <- nms[[i]]
          val <- obj[[i]]
          if (!is.null(nm) && str_detect(nm, regex(key_regex, ignore_case = TRUE))) {
            if (is.character(val) && length(val)) found <<- c(found, val)
            else if (is.atomic(val) && length(val)) found <<- c(found, as.character(val))
          }
          walk_any(val)
        }
      } else {
        for (v in obj) walk_any(v)
      }
    }
    invisible()
  }

  walk_any(x)
  found <- found[!is.na(found) & str_trim(found) != ""]
  unique(found)
}

extract_from_embedded_json <- function(obj) {
  if (is.null(obj)) {
    return(list(
      title = NA_character_,
      abstract = NA_character_,
      affiliations = NA_character_,
      funding = NA_character_,
      pub_type = NA_character_
    ))
  }

  title_candidates <- c(
    find_values_by_key(obj, "title"),
    find_values_by_key(obj, "headline"),
    find_values_by_key(obj, "name")
  )
  title_candidates <- title_candidates[nzchar(str_trim(title_candidates))]

  abs_candidates <- c(find_values_by_key(obj, "abstract"), find_values_by_key(obj, "description"))
  abs_candidates <- abs_candidates[nzchar(str_trim(abs_candidates))]

  aff_candidates <- c(
    find_values_by_key(obj, "affiliat"),
    find_values_by_key(obj, "institution"),
    find_values_by_key(obj, "address")
  )
  aff_candidates <- aff_candidates[nzchar(str_trim(aff_candidates))]

  funding_candidates <- c(
    find_values_by_key(obj, "funder"),
    find_values_by_key(obj, "funding"),
    find_values_by_key(obj, "grant"),
    find_values_by_key(obj, "award")
  )
  funding_candidates <- funding_candidates[nzchar(str_trim(funding_candidates))]

  pubtype_candidates <- c(
    find_values_by_key(obj, "articleType"),
    find_values_by_key(obj, "publicationType"),
    find_values_by_key(obj, "type"),
    find_values_by_key(obj, "@type")
  )
  pubtype_candidates <- pubtype_candidates[nzchar(str_trim(pubtype_candidates))]

  list(
    title = if (length(title_candidates)) title_candidates[[1]] else NA_character_,
    abstract = if (length(abs_candidates)) abs_candidates[[1]] else NA_character_,
    affiliations = if (length(aff_candidates)) paste(unique(aff_candidates), collapse = " | ") else NA_character_,
    funding = if (length(funding_candidates)) paste(unique(funding_candidates), collapse = " | ") else NA_character_,
    pub_type = if (length(pubtype_candidates)) pubtype_candidates[[1]] else NA_character_
  )
}

publisher_extract_full_text <- function(html) {
  doc <- tryCatch(read_html(html), error = function(e) NULL)
  if (is.null(doc)) return(NA_character_)

  selectors <- c(
    "article", "main", "#main-content", "#content",
    ".article", ".article__body", ".ArticleBody", ".article-body", ".c-article-body",
    ".content", ".main-content"
  )

  node <- NULL
  for (sel in selectors) {
    n <- html_elements(doc, sel)
    if (length(n)) { node <- n[[1]]; break }
  }
  if (is.null(node)) return(NA_character_)

  txt <- html_text2(node)
  txt <- normalize_space(txt)

  if (nchar(txt) < 2000) return(NA_character_)
  txt
}

extract_from_publisher_html <- function(html) {
  doc <- tryCatch(read_html(html), error = function(e) NULL)
  if (is.null(doc)) {
    return(list(
      title = NA_character_,
      abstract = NA_character_,
      affiliations = NA_character_,
      funding = NA_character_,
      pub_type = NA_character_,
      full_text = NA_character_,
      debug = list()
    ))
  }

  metas <- parse_meta_tags(doc)
  meta_out <- extract_from_meta(metas)

  jsonld <- extract_jsonld(doc)
  jsonld_out <- if (length(jsonld)) extract_from_embedded_json(jsonld) else extract_from_embedded_json(NULL)

  next_data <- extract_next_data(doc)
  next_out <- extract_from_embedded_json(next_data)

  init_state <- extract_initial_state(html)
  init_out <- extract_from_embedded_json(init_state)

  html_title <- tryCatch({
    t <- doc |> html_element("title") |> html_text2()
    if (!is.null(t)) str_trim(t) else NA_character_
  }, error = function(e) NA_character_)

  full_text <- publisher_extract_full_text(html)

  pick_best <- function(...) {
    vals <- list(...)
    for (v in vals) if (!is.null(v) && !is.na(v) && str_trim(v) != "") return(v)
    NA_character_
  }

  list(
    title = pick_best(meta_out$title, jsonld_out$title, next_out$title, init_out$title, html_title),
    abstract = pick_best(meta_out$abstract, jsonld_out$abstract, next_out$abstract, init_out$abstract),
    affiliations = pick_best(meta_out$affiliations, jsonld_out$affiliations, next_out$affiliations, init_out$affiliations),
    funding = pick_best(meta_out$funding, jsonld_out$funding, next_out$funding, init_out$funding),
    pub_type = pick_best(meta_out$pub_type, jsonld_out$pub_type, next_out$pub_type, init_out$pub_type),
    full_text = full_text,
    debug = list(
      meta_rows = nrow(metas),
      jsonld_blocks = length(jsonld),
      has_next_data = !is.null(next_data),
      has_initial_state = !is.null(init_state),
      full_text_chars = ifelse(is.na(full_text), 0L, nchar(full_text))
    )
  )
}

chromote_available <- function() requireNamespace("chromote", quietly = TRUE)

fetch_rendered_html_chromote <- function(url, wait_sec = 8) {
  if (!chromote_available()) return(NULL)

  b <- NULL
  out <- tryCatch({
    b <- chromote::ChromoteSession$new()
    b$Page$navigate(url = url)
    Sys.sleep(wait_sec)
    dom <- b$Runtime$evaluate(expression = "document.documentElement.outerHTML")
    dom$result$value %||% NULL
  }, error = function(e) NULL)

  tryCatch(if (!is.null(b)) b$close(), error = function(e) NULL)
  out
}

## 5) PMC and PubMed extraction ---------------------------------------------

pmc_fetch_jats_xml <- function(pmcid, timeout_sec = 30, mailto = NULL) {
  url <- "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"
  ua <- paste0("EscalationPipeline/2.0", if (!is.null(mailto) && mailto != "") paste0(" (mailto:", mailto, ")") else "")
  req <- httr2::request(url) |>
    httr2::req_url_query(db = "pmc", id = pmcid, retmode = "xml") |>
    httr2::req_headers(`User-Agent` = ua) |>
    httr2::req_options(timeout = timeout_sec)

  resp <- tryCatch(httr2::req_perform(req), error = function(e) NULL)
  if (is.null(resp) || httr2::resp_status(resp) >= 400) return(NULL)
  xml_txt <- tryCatch(httr2::resp_body_string(resp), error = function(e) NULL)
  if (is.null(xml_txt) || str_trim(xml_txt) == "") return(NULL)
  tryCatch(xml2::read_xml(xml_txt), error = function(e) NULL)
}

pmc_extract_title <- function(jats) {
  if (is.null(jats)) return(NA_character_)
  tnode <- xml_find_first(jats, ".//front//article-title")
  if (inherits(tnode, "xml_missing")) return(NA_character_)
  t <- str_squish(xml_text(tnode))
  if (!nzchar(t)) NA_character_ else t
}

pmc_extract_publication_type <- function(jats) {
  if (is.null(jats)) return(NA_character_)

  subj <- xml_find_all(jats, ".//front//article-categories//subj-group//subject")
  vals <- if (length(subj)) unique(str_squish(xml_text(subj))) else character()
  vals <- vals[nzchar(vals)]

  at <- xml_attr(xml_find_first(jats, ".//article"), "article-type") %||% NA_character_
  at <- str_trim(at)

  out <- c(vals, if (!is.na(at) && nzchar(at)) at else character())
  out <- unique(out[nzchar(out)])
  if (!length(out)) NA_character_ else paste(out, collapse = " | ")
}

pmc_extract_funding <- function(jats) {
  if (is.null(jats)) return(NA_character_)

  fs <- xml_find_all(jats, ".//front//funding-group//funding-statement")
  fs_txt <- if (length(fs)) unique(str_squish(xml_text(fs))) else character()

  ag <- xml_find_all(jats, ".//front//funding-group//award-group")
  award_lines <- character()
  if (length(ag)) {
    award_lines <- purrr::map_chr(ag, function(node) {
      src <- xml_find_all(node, ".//funding-source")
      aid <- xml_find_all(node, ".//award-id")
      txt <- c(
        if (length(src)) str_squish(xml_text(src)) else character(),
        if (length(aid)) paste0("Award: ", str_squish(xml_text(aid))) else character()
      )
      txt <- txt[nzchar(txt)]
      if (!length(txt)) return(NA_character_)
      paste(unique(txt), collapse = "; ")
    })
    award_lines <- award_lines[!is.na(award_lines) & nzchar(award_lines)]
  }

  out <- unique(c(fs_txt, award_lines))
  out <- out[nzchar(out)]
  if (!length(out)) NA_character_ else paste(out, collapse = " | ")
}

pmc_extract_abstract <- function(jats) {
  if (is.null(jats)) return(NA_character_)
  abs_nodes <- xml_find_all(jats, ".//front//abstract")
  if (!length(abs_nodes)) return(NA_character_)
  str_squish(paste(xml_text(abs_nodes), collapse = "\n"))
}

pmc_extract_affiliations_first_author <- function(jats) {
  if (is.null(jats)) return(NA_character_)

  aff_nodes <- xml_find_all(jats, ".//front//aff")
  aff_df <- tibble(
    id = xml_attr(aff_nodes, "id") %||% NA_character_,
    text = str_squish(xml_text(aff_nodes))
  ) |>
    filter(!is.na(text), text != "")

  first_contrib <- xml_find_first(jats, ".//front//contrib-group//contrib[@contrib-type='author'][1]")
  if (inherits(first_contrib, "xml_missing")) {
    return(if (nrow(aff_df)) paste(unique(aff_df$text), collapse = " | ") else NA_character_)
  }

  xrefs <- xml_find_all(first_contrib, ".//xref[@ref-type='aff']")
  rids <- xml_attr(xrefs, "rid")
  rids <- rids[!is.na(rids) & rids != ""]

  if (length(rids) && nrow(aff_df)) {
    aff <- aff_df |>
      filter(id %in% rids) |>
      pull(text)
    if (length(aff)) return(paste(unique(aff), collapse = " | "))
  }

  if (nrow(aff_df)) paste(unique(aff_df$text), collapse = " | ") else NA_character_
}

pmc_extract_full_text <- function(jats) {
  if (is.null(jats)) return(NA_character_)

  body <- xml_find_first(jats, ".//body")
  if (inherits(body, "xml_missing")) return(NA_character_)

  nodes <- xml_find_all(body, ".//sec/title | .//p")
  txt <- xml_text(nodes)
  txt <- str_squish(txt)
  txt <- txt[nzchar(txt)]
  if (!length(txt)) return(NA_character_)
  paste(txt, collapse = "\n\n")
}

pubmed_fetch_xml <- function(pmid, timeout_sec = 30, mailto = NULL) {
  url <- "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"
  ua <- paste0("EscalationPipeline/2.0", if (!is.null(mailto) && mailto != "") paste0(" (mailto:", mailto, ")") else "")
  req <- httr2::request(url) |>
    httr2::req_url_query(db = "pubmed", id = pmid, retmode = "xml") |>
    httr2::req_headers(`User-Agent` = ua) |>
    httr2::req_options(timeout = timeout_sec)

  resp <- tryCatch(httr2::req_perform(req), error = function(e) NULL)
  if (is.null(resp) || httr2::resp_status(resp) >= 400) return(NULL)
  xml_txt <- tryCatch(httr2::resp_body_string(resp), error = function(e) NULL)
  if (is.null(xml_txt) || str_trim(xml_txt) == "") return(NULL)
  tryCatch(xml2::read_xml(xml_txt), error = function(e) NULL)
}

pubmed_extract_core <- function(xdoc) {
  if (is.null(xdoc)) {
    return(list(
      title = NA_character_,
      abstract = NA_character_,
      affiliations = NA_character_,
      funding = NA_character_,
      pub_type = NA_character_
    ))
  }

  tnode <- xml_find_first(xdoc, ".//Article/ArticleTitle")
  title <- if (!inherits(tnode, "xml_missing")) str_squish(xml_text(tnode)) else NA_character_
  if (is.na(title) || !nzchar(title)) title <- NA_character_

  abs_nodes <- xml_find_all(xdoc, ".//Article/Abstract/AbstractText")
  abstract <- if (length(abs_nodes)) str_squish(paste(xml_text(abs_nodes), collapse = "\n")) else NA_character_

  aff_nodes <- xml_find_all(xdoc, ".//Article/AuthorList/Author/AffiliationInfo/Affiliation")
  affiliations <- if (length(aff_nodes)) paste(unique(str_squish(xml_text(aff_nodes))), collapse = " | ") else NA_character_

  pt_nodes <- xml_find_all(xdoc, ".//PublicationTypeList/PublicationType")
  pub_type <- if (length(pt_nodes)) paste(unique(str_squish(xml_text(pt_nodes))), collapse = " | ") else NA_character_

  grants <- xml_find_all(xdoc, ".//GrantList/Grant")
  funding <- NA_character_
  if (length(grants)) {
    lines <- purrr::map_chr(grants, function(g) {
      agency  <- xml_find_first(g, ".//Agency")
      gid     <- xml_find_first(g, ".//GrantID")
      acr     <- xml_find_first(g, ".//Acronym")
      country <- xml_find_first(g, ".//Country")

      parts <- c(
        if (!inherits(agency, "xml_missing")) str_squish(xml_text(agency)) else NA_character_,
        if (!inherits(acr, "xml_missing")) paste0("Acronym: ", str_squish(xml_text(acr))) else NA_character_,
        if (!inherits(gid, "xml_missing")) paste0("Grant: ", str_squish(xml_text(gid))) else NA_character_,
        if (!inherits(country, "xml_missing")) paste0("Country: ", str_squish(xml_text(country))) else NA_character_
      )
      parts <- parts[!is.na(parts) & nzchar(parts)]
      if (!length(parts)) return(NA_character_)
      paste(parts, collapse = "; ")
    })
    lines <- lines[!is.na(lines) & nzchar(lines)]
    if (length(lines)) funding <- paste(unique(lines), collapse = " | ")
  }

  if (is.na(funding) || !nzchar(funding)) {
    fn <- xml_find_all(xdoc, ".//FundingInformation")
    if (length(fn)) {
      t <- unique(str_squish(xml_text(fn)))
      t <- t[nzchar(t)]
      if (length(t)) funding <- paste(t, collapse = " | ")
    }
  }

  list(
    title = title,
    abstract = abstract,
    affiliations = affiliations,
    funding = funding,
    pub_type = pub_type
  )
}

## 6) DOI to PMID/PMCID resolution -----------------------------------------

normalize_doi <- function(x) {
  if (is.null(x)) return(NA_character_)
  x <- as.character(x)
  x <- stringr::str_trim(x)
  x <- sub("^https?://(dx\\.)?doi\\.org/", "", x, ignore.case = TRUE)
  x <- sub("^doi\\s*:\\s*", "", x, ignore.case = TRUE)
  x <- sub("[\\s\\.;,]+$", "", x)
  tolower(x)
}

idconv_batch_resolve <- function(ids, timeout_sec = 30, mailto = NULL) {
  ids <- unique(str_trim(ids))
  ids <- ids[ids != "" & !is.na(ids)]
  if (!length(ids)) return(tibble(input_id = character(), pmid = character(), pmcid = character(), doi = character()))

  base <- "https://www.ncbi.nlm.nih.gov/pmc/utils/idconv/v1.0/"
  ua <- paste0("EscalationPipeline/2.0", if (!is.null(mailto) && mailto != "") paste0(" (mailto:", mailto, ")") else "")

  query <- list(ids = paste(ids, collapse = ","), format = "json")
  if (!is.null(mailto) && mailto != "") query$email <- mailto

  req <- httr2::request(base) |>
    httr2::req_url_query(!!!query) |>
    httr2::req_headers(`User-Agent` = ua) |>
    httr2::req_options(timeout = timeout_sec)

  resp <- tryCatch(httr2::req_perform(req), error = function(e) NULL)
  if (is.null(resp) || httr2::resp_status(resp) >= 400) {
    return(tibble(input_id = ids, pmid = NA_character_, pmcid = NA_character_, doi = NA_character_))
  }

  txt <- tryCatch(httr2::resp_body_string(resp), error = function(e) NULL)
  if (is.null(txt) || str_trim(txt) == "") {
    return(tibble(input_id = ids, pmid = NA_character_, pmcid = NA_character_, doi = NA_character_))
  }

  js <- tryCatch(jsonlite::fromJSON(txt, simplifyVector = TRUE), error = function(e) NULL)
  recs <- js$records %||% NULL
  if (is.null(recs) || !length(recs)) {
    return(tibble(input_id = ids, pmid = NA_character_, pmcid = NA_character_, doi = NA_character_))
  }

  recs_df <- as_tibble(recs)
  out <- tibble(
    input_id = recs_df[["requested-id"]] %||% recs_df[["requested_id"]] %||% NA_character_,
    pmid = recs_df[["pmid"]] %||% NA_character_,
    pmcid = recs_df[["pmcid"]] %||% NA_character_,
    doi = recs_df[["doi"]] %||% NA_character_
  )

  missing <- setdiff(ids, out$input_id %||% character())
  if (length(missing)) out <- bind_rows(out, tibble(input_id = missing, pmid = NA_character_, pmcid = NA_character_, doi = NA_character_))
  out
}

pubmed_esearch_pmid_doi <- function(doi, timeout_sec = 20, mailto = NULL) {
  doi <- normalize_doi(doi)
  if (is.na(doi) || !nzchar(doi)) return(NA_character_)

  ua <- paste0("EscalationPipeline/2.0", if (!is.null(mailto) && mailto != "") paste0(" (mailto:", mailto, ")") else "")

  req <- httr2::request("https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi") |>
    httr2::req_url_query(
      db = "pubmed",
      term = paste0(doi, "[doi]"),
      retmode = "json",
      retmax = 5
    ) |>
    httr2::req_headers(`User-Agent` = ua) |>
    httr2::req_options(timeout = timeout_sec)

  resp <- tryCatch(httr2::req_perform(req), error = function(e) NULL)
  if (is.null(resp) || httr2::resp_status(resp) >= 400) return(NA_character_)

  body <- tryCatch(httr2::resp_body_string(resp), error = function(e) NULL)
  if (is.null(body) || str_trim(body) == "") return(NA_character_)

  js <- tryCatch(jsonlite::fromJSON(body, simplifyVector = TRUE), error = function(e) NULL)
  if (is.null(js)) return(NA_character_)

  ids <- js$esearchresult$idlist %||% character()
  if (!length(ids)) return(NA_character_)
  as.character(ids[[1]])
}

pmc_esearch_pmcid_doi <- function(doi, timeout_sec = 20, mailto = NULL) {
  doi <- normalize_doi(doi)
  if (is.na(doi) || !nzchar(doi)) return(NA_character_)

  ua <- paste0("EscalationPipeline/2.0", if (!is.null(mailto) && mailto != "") paste0(" (mailto:", mailto, ")") else "")

  req <- httr2::request("https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi") |>
    httr2::req_url_query(
      db = "pmc",
      term = paste0(doi, "[doi]"),
      retmode = "json",
      retmax = 5
    ) |>
    httr2::req_headers(`User-Agent` = ua) |>
    httr2::req_options(timeout = timeout_sec)

  resp <- tryCatch(httr2::req_perform(req), error = function(e) NULL)
  if (is.null(resp) || httr2::resp_status(resp) >= 400) return(NA_character_)

  body <- tryCatch(httr2::resp_body_string(resp), error = function(e) NULL)
  if (is.null(body) || str_trim(body) == "") return(NA_character_)

  js <- tryCatch(jsonlite::fromJSON(body, simplifyVector = TRUE), error = function(e) NULL)
  if (is.null(js)) return(NA_character_)

  ids <- js$esearchresult$idlist %||% character()
  if (!length(ids)) return(NA_character_)

  pmc_num <- as.character(ids[[1]])
  if (!nzchar(pmc_num)) return(NA_character_)
  if (grepl("^PMC", pmc_num)) pmc_num else paste0("PMC", pmc_num)
}

resolve_doi_to_ids <- function(dois, chunk_size = 80, timeout_sec = 30, mailto = NULL, polite_sleep = 0.12) {
  dois <- normalize_doi(dois)
  dois <- str_trim(dois)
  dois <- dois[!is.na(dois) & dois != ""]
  if (!length(dois)) return(tibble(doi = character(), pmid = character(), pmcid = character()))

  cache <- new.env(parent = emptyenv())
  get_cached <- function(d) if (exists(d, envir = cache, inherits = FALSE)) get(d, envir = cache) else NULL
  set_cached <- function(d, value) assign(d, value, envir = cache)

  unique_dois <- unique(dois)
  chunks <- split(unique_dois, ceiling(seq_along(unique_dois) / chunk_size))

  all_rows <- vector("list", length(chunks))

  for (i in seq_along(chunks)) {
    ch <- chunks[[i]]

    need <- ch[vapply(ch, function(d) is.null(get_cached(d)), logical(1))]
    if (length(need)) {
      resolved <- idconv_batch_resolve(need, timeout_sec = timeout_sec, mailto = mailto)

      for (k in seq_len(nrow(resolved))) {
        key <- normalize_doi(resolved$input_id[[k]])
        set_cached(key, resolved[k, ])
      }
      Sys.sleep(polite_sleep)
    }

    chunk_rows <- bind_rows(lapply(ch, get_cached)) |>
      mutate(
        doi   = normalize_doi(input_id),
        pmid  = ifelse(is.na(pmid)  | str_trim(pmid)  == "", NA_character_, as.character(pmid)),
        pmcid = ifelse(is.na(pmcid) | str_trim(pmcid) == "", NA_character_, as.character(pmcid))
      ) |>
      select(doi, pmid, pmcid)

    miss_pmid <- chunk_rows$doi[is.na(chunk_rows$pmid) | str_trim(chunk_rows$pmid) == ""]
    miss_pmid <- unique(miss_pmid[!is.na(miss_pmid) & nzchar(miss_pmid)])
    if (length(miss_pmid)) {
      fill_pmid <- purrr::map_chr(miss_pmid, function(d) {
        Sys.sleep(polite_sleep)
        pubmed_esearch_pmid_doi(d, timeout_sec = min(timeout_sec, 20), mailto = mailto)
      })
      fill_df <- tibble(doi = miss_pmid, pmid_fill = fill_pmid)
      chunk_rows <- chunk_rows |>
        left_join(fill_df, by = "doi") |>
        mutate(pmid = coalesce(pmid, pmid_fill)) |>
        select(-pmid_fill)
    }

    miss_pmcid <- chunk_rows$doi[is.na(chunk_rows$pmcid) | str_trim(chunk_rows$pmcid) == ""]
    miss_pmcid <- unique(miss_pmcid[!is.na(miss_pmcid) & nzchar(miss_pmcid)])
    if (length(miss_pmcid)) {
      fill_pmcid <- purrr::map_chr(miss_pmcid, function(d) {
        Sys.sleep(polite_sleep)
        pmc_esearch_pmcid_doi(d, timeout_sec = min(timeout_sec, 20), mailto = mailto)
      })
      fill_df <- tibble(doi = miss_pmcid, pmcid_fill = fill_pmcid)
      chunk_rows <- chunk_rows |>
        left_join(fill_df, by = "doi") |>
        mutate(pmcid = coalesce(pmcid, pmcid_fill)) |>
        select(-pmcid_fill)
    }

    all_rows[[i]] <- chunk_rows
  }

  bind_rows(all_rows) |>
    distinct(doi, .keep_all = TRUE)
}

## 7) Prompt payload builder ------------------------------------------------

truncate_text <- function(x, max_chars) {
  if (is.na(x) || str_trim(x) == "") return(NA_character_)
  if (nchar(x) <= max_chars) return(x)
  paste0(substr(x, 1, max_chars), "\n\n[TRUNCATED]")
}

build_prompt_payload <- function(title, authors,
                                 doi = NA_character_, pmid = NA_character_, pmcid = NA_character_,
                                 publication_type = NA_character_,
                                 funding = NA_character_,
                                 affiliations = NA_character_,
                                 abstract = NA_character_,
                                 full_text = NA_character_,
                                 max_prompt_chars = DEFAULT_MAX_PROMPT_CHARS,
                                 max_fulltext_chars = DEFAULT_MAX_FULLTEXT_CHARS) {

  full_text2 <- truncate_text(full_text, max_fulltext_chars)

  parts <- c(
    "You are a meticulous medical researcher. Use ONLY the provided extracted content; do not guess.",
    paste0("Title: ", as.character(title %||% "")),
    if (!is.na(authors) && nzchar(str_trim(authors))) paste0("First author: ", authors) else NA_character_,
    if (!is.na(publication_type) && nzchar(str_trim(publication_type))) paste0("Publication type: ", publication_type) else NA_character_,
    if (!is.na(funding) && nzchar(str_trim(funding))) paste0("Funding / grants: ", funding) else NA_character_,
    if (!is.na(affiliations) && nzchar(str_trim(affiliations))) paste0("Author affiliations (first author is first): ", affiliations) else NA_character_,
    if (!is.na(abstract) && nzchar(str_trim(abstract))) paste0("Abstract:\n", abstract) else NA_character_,
    if (!is.na(full_text2) && nzchar(str_trim(full_text2))) paste0("Full text:\n", full_text2) else NA_character_
  )
  parts <- parts[!is.na(parts) & nzchar(str_trim(parts))]
  payload <- paste(parts, collapse = "\n\n")
  truncate_text(payload, max_prompt_chars)
}

## 8) Escalating extraction -------------------------------------------------

extract_all_available <- function(
  url = NA_character_,
  doi = NA_character_,
  pmid = NA_character_,
  pmcid = NA_character_,
  use_headless_if_sparse = TRUE,
  headless_wait_sec = 8,
  timeout_sec = 30,
  mailto = NULL
) {
  res <- list(
    title = NA_character_,
    abstract = NA_character_,
    affiliations = NA_character_,
    funding = NA_character_,
    publication_type = NA_character_,
    full_text = NA_character_,
    full_text_source = NA_character_,
    source_used = NA_character_,
    final_url = NA_character_,
    http_status = NA_integer_,
    botBlocked = 0L,
    resolved_pmid = pmid,
    resolved_pmcid = pmcid,
    debug = list()
  )

  if (!is.na(doi) && nzchar(str_trim(doi)) &&
      (is.na(res$resolved_pmid) || !nzchar(str_trim(res$resolved_pmid))) &&
      (is.na(res$resolved_pmcid) || !nzchar(str_trim(res$resolved_pmcid)))) {
    r <- resolve_doi_to_ids(doi, chunk_size = 1, timeout_sec = timeout_sec, mailto = mailto, polite_sleep = 0)
    if (nrow(r)) {
      res$resolved_pmid <- r$pmid[[1]]
      res$resolved_pmcid <- r$pmcid[[1]]
    }
  }

  if (!is.na(res$resolved_pmcid) && nzchar(str_trim(res$resolved_pmcid))) {
    jats <- pmc_fetch_jats_xml(res$resolved_pmcid, timeout_sec = timeout_sec, mailto = mailto)
    if (!is.null(jats)) {
      res$title <- pmc_extract_title(jats)
      res$abstract <- pmc_extract_abstract(jats)
      res$affiliations <- pmc_extract_affiliations_first_author(jats)
      res$funding <- pmc_extract_funding(jats)
      res$publication_type <- pmc_extract_publication_type(jats)

      res$full_text <- pmc_extract_full_text(jats)
      res$full_text_source <- if (!is.na(res$full_text) && nzchar(str_trim(res$full_text))) "PMC_JATS_BODY" else NA_character_
      res$source_used <- "PMC_JATS_XML"
      res$debug$pmc_full_text_chars <- ifelse(is.na(res$full_text), 0L, nchar(res$full_text))
      return(res)
    }
  }

  if (!is.na(res$resolved_pmid) && nzchar(str_trim(res$resolved_pmid))) {
    xdoc <- pubmed_fetch_xml(res$resolved_pmid, timeout_sec = timeout_sec, mailto = mailto)
    core <- pubmed_extract_core(xdoc)
    if (!is.na(core$title) || !is.na(core$abstract) || !is.na(core$affiliations) || !is.na(core$funding) || !is.na(core$pub_type)) {
      res$title <- core$title
      res$abstract <- core$abstract
      res$affiliations <- core$affiliations
      res$funding <- core$funding
      res$publication_type <- core$pub_type
      res$source_used <- "PUBMED_EFETCH_XML"
      return(res)
    }
  }

  if (!is.na(url) && nzchar(str_trim(url))) {
    fetched <- fetch_url_html(url, timeout_sec = timeout_sec)
    res$final_url <- fetched$final_url
    res$http_status <- fetched$status

    if (!isTRUE(fetched$ok)) {
      res$botBlocked <- 1L
      res$source_used <- "PUBLISHER_HTML_FETCH_FAILED"
      return(res)
    }

    out <- extract_from_publisher_html(fetched$html)
    res$title <- out$title
    res$abstract <- out$abstract
    res$affiliations <- out$affiliations
    res$funding <- out$funding
    res$publication_type <- out$pub_type
    res$full_text <- out$full_text
    res$full_text_source <- if (!is.na(res$full_text) && nzchar(str_trim(res$full_text))) "PUBLISHER_HTML_BODY" else NA_character_
    res$source_used <- "PUBLISHER_HTML"
    res$debug <- out$debug

    sparse_full <- is.na(res$full_text) || !nzchar(str_trim(res$full_text))
    sparse_meta <- (is.na(res$title) || !nzchar(str_trim(res$title))) &&
      (is.na(res$abstract) || !nzchar(str_trim(res$abstract))) &&
      (is.na(res$affiliations) || !nzchar(str_trim(res$affiliations))) &&
      (is.na(res$funding) || !nzchar(str_trim(res$funding))) &&
      (is.na(res$publication_type) || !nzchar(str_trim(res$publication_type)))

    if (use_headless_if_sparse && (sparse_full || sparse_meta)) {
      rendered_html <- fetch_rendered_html_chromote(res$final_url %||% url, wait_sec = headless_wait_sec)
      if (!is.null(rendered_html) && nzchar(str_trim(rendered_html))) {
        out2 <- extract_from_publisher_html(rendered_html)

        if (is.na(res$title) || !nzchar(str_trim(res$title))) res$title <- out2$title
        if (is.na(res$abstract) || !nzchar(str_trim(res$abstract))) res$abstract <- out2$abstract
        if (is.na(res$affiliations) || !nzchar(str_trim(res$affiliations))) res$affiliations <- out2$affiliations
        if (is.na(res$funding) || !nzchar(str_trim(res$funding))) res$funding <- out2$funding
        if (is.na(res$publication_type) || !nzchar(str_trim(res$publication_type))) res$publication_type <- out2$pub_type

        if (!is.na(out2$full_text) && nchar(out2$full_text) > (ifelse(is.na(res$full_text), 0L, nchar(res$full_text)) + 500L)) {
          res$full_text <- out2$full_text
          res$full_text_source <- "HEADLESS_RENDERED_HTML_BODY"
        }

        res$source_used <- "HEADLESS_RENDERED_PUBLISHER_HTML"
        res$debug$headless_used <- TRUE
        res$debug$headless_wait_sec <- headless_wait_sec
      } else {
        res$debug$headless_used <- FALSE
        res$debug$headless_reason <- if (!chromote_available()) "chromote_not_installed" else "render_failed_or_empty"
      }
    }

    return(res)
  }

  res$source_used <- "NO_URL_AND_NO_PMCID_PMID"
  res
}

## 9) Row preparation and classification ------------------------------------

prepare_row_for_classification <- function(doi, url, title_df, abstract_df, funding_df, firstAuthor, affiliations_df = NA_character_,
                                           mailto = MAILTO,
                                           use_headless_if_sparse = USE_HEADLESS,
                                           headless_wait_sec = HEADLESS_WAIT_SEC,
                                           timeout_sec = TIMEOUT_SEC,
                                           max_prompt_chars = DEFAULT_MAX_PROMPT_CHARS,
                                           max_fulltext_chars = DEFAULT_MAX_FULLTEXT_CHARS) {

  ext <- extract_all_available(
    url = url,
    doi = doi,
    pmid = NA_character_,
    pmcid = NA_character_,
    use_headless_if_sparse = use_headless_if_sparse,
    headless_wait_sec = headless_wait_sec,
    timeout_sec = timeout_sec,
    mailto = mailto
  )

  # Prefer df columns; fall back to extracted values where missing
  title_used <- title_df
  if (is.na(title_used) || !nzchar(str_trim(title_used))) title_used <- ext$title

  abstract_used <- abstract_df
  if (is.na(abstract_used) || !nzchar(str_trim(abstract_used))) abstract_used <- ext$abstract

  funding_used <- funding_df
  if (is.na(funding_used) || !nzchar(str_trim(funding_used))) funding_used <- ext$funding

  affiliations_used <- affiliations_df
  if (is.na(affiliations_used) || !nzchar(str_trim(affiliations_used))) affiliations_used <- ext$affiliations

  payload <- build_prompt_payload(
    title = title_used,
    authors = firstAuthor,
    doi = normalize_doi(doi),
    pmid = ext$resolved_pmid,
    pmcid = ext$resolved_pmcid,
    publication_type = ext$publication_type,
    funding = funding_used,
    affiliations = affiliations_used,
    abstract = abstract_used,
    full_text = ext$full_text,
    max_prompt_chars = max_prompt_chars,
    max_fulltext_chars = max_fulltext_chars
  )

  list(
    payload = payload,
    botBlocked = ext$botBlocked %||% 0L,
    sourceUsed = ext$source_used %||% "unknown",
    pmcid      = ext$resolved_pmcid %||% NA_character_,
    extracted_title = ext$title %||% NA_character_,
    extracted_abstract = ext$abstract %||% NA_character_,
    extracted_funding = ext$funding %||% NA_character_,
    extracted_publication_type = ext$publication_type %||% NA_character_,
    used_title = title_used %||% NA_character_,
    used_funding = funding_used %||% NA_character_,
    used_abstract = abstract_used %||% NA_character_
  )
}

classify_row <- function(doi, url, title_df, abstract_df, funding_df, firstAuthor, affiliations_df = NA_character_,
                         mailto = MAILTO,
                         use_headless_if_sparse = USE_HEADLESS,
                         headless_wait_sec = HEADLESS_WAIT_SEC,
                         timeout_sec = TIMEOUT_SEC,
                         max_prompt_chars = DEFAULT_MAX_PROMPT_CHARS,
                         max_fulltext_chars = DEFAULT_MAX_FULLTEXT_CHARS) {

  prep <- prepare_row_for_classification(
    doi = doi,
    url = url,
    title_df = title_df,
    abstract_df = abstract_df,
    funding_df = funding_df,
    firstAuthor = firstAuthor,
    affiliations_df = affiliations_df,
    mailto = mailto,
    use_headless_if_sparse = use_headless_if_sparse,
    headless_wait_sec = headless_wait_sec,
    timeout_sec = timeout_sec,
    max_prompt_chars = max_prompt_chars,
    max_fulltext_chars = max_fulltext_chars
  )

  out <- tryCatch(openai_request(prep$payload), error = function(e) NULL)
  if (is.null(out)) out <- blank_result_tibble()

  c(prep, list(result = out))
}

## 10) Run ------------------------------------------------------------------
all_idx <- which(df$score >= 4)
batches <- split(all_idx, ceiling(seq_along(all_idx) / BATCH_SIZE))
CHECKPOINT_DIR <- "."
OUT_FINAL_XLSX <- "Dataset 6.xlsx"

if (!dir.exists(CHECKPOINT_DIR)) dir.create(CHECKPOINT_DIR, recursive = TRUE, showWarnings = FALSE)

df_running <- df
openai_batch_meta_all <- list()

for (b in seq_along(batches)) {

  idx <- batches[[b]]
  message(sprintf("Extraction batch %d/%d: %d rows", b, length(batches), length(idx)))

  prep_batch <- purrr::map(
    idx,
    ~ prepare_row_for_classification(
      doi         = df_running$DOI[.x],
      url         = df_running$URL[.x],
      title_df    = df_running$title[.x],
      abstract_df = df_running$Abstract[.x],
      funding_df  = df_running$Funding.Details[.x],
      firstAuthor = df_running$firstAuthor[.x],
      affiliations_df = df_running$Affiliations[.x]
    )
  )

  payload_df_batch <- tibble::tibble(
    row_id = idx,
    payload = purrr::map_chr(prep_batch, "payload")
  )

  meta_df_batch <- tibble::tibble(
    row_id     = idx,
    botBlocked = purrr::map_int(prep_batch, "botBlocked"),
    sourceUsed = purrr::map_chr(prep_batch, "sourceUsed")
  )

  openai_batch_res <- run_openai_payload_batches(
    payload_df = payload_df_batch,
    max_requests_per_batch = OPENAI_BATCH_REQUESTS_PER_JOB,
    poll_seconds = OPENAI_BATCH_POLL_SECONDS,
    max_hours = OPENAI_BATCH_MAX_HOURS,
    label = sprintf("extract_batch_%03d", b),
    error_dir = CHECKPOINT_DIR
  )

  class_df_batch <- openai_batch_res$class_df

  openai_batch_meta_all[[b]] <- openai_batch_res$batches %>%
    dplyr::mutate(extraction_batch = b, n_rows = length(idx))

  df_running <- merge_new_into_df(df_running, class_df_batch, meta_df_batch)

  checkpoint_path <- file.path(CHECKPOINT_DIR, sprintf("labelled_checkpoint_batch_%03d.xlsx", b))
  writexl::write_xlsx(df_running, checkpoint_path)

  batch_meta_path <- file.path(CHECKPOINT_DIR, sprintf("openai_batch_metadata_batch_%03d.xlsx", b))
  writexl::write_xlsx(openai_batch_res$batches, batch_meta_path)
}

writexl::write_xlsx(df_running, OUT_FINAL_XLSX)

if (length(openai_batch_meta_all)) {
  writexl::write_xlsx(
    dplyr::bind_rows(openai_batch_meta_all),
    file.path(CHECKPOINT_DIR, "openai_batch_metadata_all.xlsx")
  )
}
