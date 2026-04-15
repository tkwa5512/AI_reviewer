# AI Reviewer 1
# Purpose: Extract structured data from each included paper using GPT with web search,
#   submitted as an OpenAI batch job for efficiency.
# Input:  Dataset 3.xlsx
# Output: Dataset 4.xlsx

library(readxl)
library(writexl)
library(dplyr)
library(purrr)
library(httr)
library(jsonlite)
library(tibble)

## 1) API setup -------------------------------------------------------------
# Sys.setenv(OPENAI_API_KEY = "your-key-here")

`%||%` <- function(x, y) if (is.null(x)) y else x

API_BASE <- "https://api.openai.com/v1"

auth_header <- function() {
  add_headers(Authorization = paste("Bearer", Sys.getenv("OPENAI_API_KEY")))
}

stop_if_no_key <- function() {
  if (identical(Sys.getenv("OPENAI_API_KEY"), ""))
    stop("OPENAI_API_KEY is empty. Set it before running.")
}

## 2) Prompt and schema -----------------------------------------------------
build_prompt_web <- function(name, URL) {
  paste0(
    "You are a meticulous medical researcher. Classify this paper with DOI: ", name, ".\n",
    "Actively inspect manuscript at: ", URL,
    ", metadata panels, affiliations, funding and acknowledgement sections for study details. No guessing."
  )
}

extract_response_text <- function(resp) {
  if (!is.null(resp$output_text) && is.character(resp$output_text))
    return(resp$output_text)
  if (!is.null(resp$output)) {
    for (o in resp$output) {
      if (!is.null(o$content)) {
        for (c in o$content) {
          if (!is.null(c$text) && is.character(c$text)) return(c$text)
          if (!is.null(c$output_text) && is.character(c$output_text)) return(c$output_text)
        }
      }
    }
  }
  NULL
}

classifier_schema <- function() {
  list(
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
          "other",
          "unclear"
        )
      ),
      input_data_modality = list(
        type = "array",
        items = list(type = "string", enum = c("Surface ECG", "EPS", "CIED", "other", "unclear")),
        minItems = 1
      ),
      clinical_purpose_model_output = list(
        type = "array",
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
      model_architecture = list(
        type = "string",
        enum = c("neural network", "other machine learning", "machine learning/AI not used", "unclear")
      ),
      dataset_origin = list(
        type = "array",
        items = list(
          type = "string",
          enum = c("MIT-BIH database", "PTB", "Physionet", "Other public database", "Investigator developed", "unclear")
        ),
        minItems = 1
      ),
      funding_source = list(
        type = "array",
        items = list(type = "string", enum = c("Academic/public", "Industry", "Charity", "none", "unclear")),
        minItems = 1
      ),
      main_accuracy_metric = list(
        type = "array",
        items = list(
          type = "string",
          enum = c("AUROC", "Sensitivity", "Specificity", "Accuracy", "PPV", "NPV",
                   "F1 score", "C-index", "Cohen's kappa", "other", "unclear")
        ),
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
}

## 3) Batch job helpers -----------------------------------------------------
build_responses_body <- function(name, URL) {
  list(
    model = "gpt-5.2",
    input = build_prompt_web(name, URL),
    tools = list(list(type = "web_search")),
    tool_choice = "auto",
    temperature = 0,
    text = list(
      format = list(
        type = "json_schema",
        name = "ai_arrhythmia_classifier",
        strict = TRUE,
        schema = classifier_schema()
      )
    )
  )
}

empty_class_df <- function() {
  tibble(
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

write_batch_input_jsonl <- function(df, idx, path) {
  tasks <- lapply(idx, function(i) {
    list(
      custom_id = paste0("row-", i),
      method = "POST",
      url = "/v1/responses",
      body = build_responses_body(df$DOI[[i]], df$URL[[i]])
    )
  })
  lines <- vapply(tasks, function(x) toJSON(x, auto_unbox = TRUE, null = "null"), FUN.VALUE = character(1))
  writeLines(lines, path, useBytes = TRUE)
  invisible(path)
}

upload_batch_file <- function(jsonl_path) {
  res <- POST(
    paste0(API_BASE, "/files"),
    auth_header(),
    body = list(purpose = "batch", file = upload_file(jsonl_path, type = "text/jsonl")),
    encode = "multipart",
    config(connecttimeout = 120, timeout = 600)
  )
  stop_for_status(res)
  content(res, as = "parsed", simplifyVector = FALSE)$id
}

create_batch_job <- function(input_file_id) {
  res <- POST(
    paste0(API_BASE, "/batches"),
    auth_header(),
    content_type_json(),
    encode = "json",
    body = list(input_file_id = input_file_id, endpoint = "/v1/responses", completion_window = "24h"),
    config(connecttimeout = 120, timeout = 120)
  )
  stop_for_status(res)
  content(res, as = "parsed", simplifyVector = FALSE)
}

get_batch <- function(batch_id) {
  res <- GET(
    paste0(API_BASE, "/batches/", batch_id),
    auth_header(),
    config(connecttimeout = 120, timeout = 120)
  )
  stop_for_status(res)
  content(res, as = "parsed", simplifyVector = FALSE)
}

wait_for_batch <- function(batch_id, poll_seconds = 30, max_hours = 24) {
  terminal <- c("completed", "failed", "expired", "cancelled")
  t0 <- Sys.time()

  repeat {
    b <- get_batch(batch_id)
    rc <- b$request_counts %||% list(total = NA, completed = NA, failed = NA)
    message(
      "Batch ", batch_id,
      " | status=", b$status,
      " | completed=", rc$completed %||% 0, "/", rc$total %||% 0,
      " | failed=", rc$failed %||% 0
    )
    if (b$status %in% terminal) return(b)
    if (as.numeric(difftime(Sys.time(), t0, units = "hours")) > max_hours)
      stop("Timed out waiting for batch completion.")
    Sys.sleep(poll_seconds)
  }
}

download_file_text <- function(file_id) {
  res <- GET(
    paste0(API_BASE, "/files/", file_id, "/content"),
    auth_header(),
    config(connecttimeout = 120, timeout = 600)
  )
  stop_for_status(res)
  content(res, as = "text", encoding = "UTF-8")
}

parse_jsonl_text <- function(txt) {
  lines <- trimws(strsplit(txt, "\n", fixed = TRUE)[[1]])
  lapply(lines[nzchar(lines)], function(x) fromJSON(x, simplifyVector = FALSE))
}

parse_batch_output_record <- function(rec) {
  custom_id <- rec$custom_id %||% ""
  row_id <- suppressWarnings(as.integer(sub("^row-", "", custom_id)))
  if (is.na(row_id)) return(NULL)

  if (!is.null(rec$error)) {
    message("row ", row_id, " batch error: ", rec$error$message %||% "unknown")
    return(NULL)
  }

  if (is.null(rec$response) || is.null(rec$response$status_code) || rec$response$status_code != 200) {
    message("row ", row_id, " HTTP status: ", rec$response$status_code %||% NA_integer_)
    return(NULL)
  }

  txt <- extract_response_text(rec$response$body)
  if (is.null(txt)) {
    message("row ", row_id, " has no output text.")
    return(NULL)
  }

  tmp <- tryCatch(
    fromJSON(txt, simplifyVector = FALSE),
    error = function(e) { message("row ", row_id, " JSON parse error: ", e$message); NULL }
  )
  if (is.null(tmp)) return(NULL)

  tibble(
    row_id                                        = row_id,
    country_of_first_affiliation_of_first_author  = tmp$country_of_first_affiliation_of_first_author %||% NA_character_,
    study_design_type                             = tmp$study_design_type %||% NA_character_,
    input_data_modality                           = list(tmp$input_data_modality %||% NA_character_),
    clinical_purpose_model_output                 = list(tmp$clinical_purpose_model_output %||% NA_character_),
    model_architecture                            = tmp$model_architecture %||% NA_character_,
    dataset_origin                                = list(tmp$dataset_origin %||% NA_character_),
    funding_source                                = list(tmp$funding_source %||% NA_character_),
    main_accuracy_metric                          = list(tmp$main_accuracy_metric %||% NA_character_)
  )
}

run_openai_batches <- function(df, idx, max_requests_per_batch = 50000, poll_seconds = 30, label = "run") {
  if (length(idx) == 0)
    return(list(class_df = empty_class_df(), batches = tibble()))

  chunks <- split(idx, ceiling(seq_along(idx) / max_requests_per_batch))
  all_rows <- list()
  batch_meta <- list()

  for (k in seq_along(chunks)) {
    chunk_idx <- chunks[[k]]
    message("Submitting chunk ", k, "/", length(chunks), " with ", length(chunk_idx), " requests")

    jsonl_path <- file.path(
      tempdir(),
      paste0("batch_input_", label, "_", k, "_", format(Sys.time(), "%Y%m%d_%H%M%S"), ".jsonl")
    )
    write_batch_input_jsonl(df, chunk_idx, jsonl_path)

    input_file_id <- upload_batch_file(jsonl_path)
    batch <- create_batch_job(input_file_id)
    message("Batch created: ", batch$id)

    final_batch <- wait_for_batch(batch$id, poll_seconds = poll_seconds)

    batch_meta[[k]] <- tibble(
      chunk          = k,
      batch_id       = final_batch$id %||% NA_character_,
      status         = final_batch$status %||% NA_character_,
      input_file_id  = final_batch$input_file_id %||% NA_character_,
      output_file_id = final_batch$output_file_id %||% NA_character_,
      error_file_id  = final_batch$error_file_id %||% NA_character_,
      total          = final_batch$request_counts$total %||% NA_integer_,
      completed      = final_batch$request_counts$completed %||% NA_integer_,
      failed         = final_batch$request_counts$failed %||% NA_integer_
    )

    if (!is.null(final_batch$output_file_id)) {
      out_records <- parse_jsonl_text(download_file_text(final_batch$output_file_id))
      parsed_rows <- compact(lapply(out_records, parse_batch_output_record))
      all_rows[[k]] <- if (length(parsed_rows) == 0) empty_class_df() else bind_rows(parsed_rows)
    } else {
      all_rows[[k]] <- empty_class_df()
    }

    if (!is.null(final_batch$error_file_id)) {
      err_path <- file.path(
        tempdir(),
        paste0("batch_errors_", label, "_", k, "_", format(Sys.time(), "%Y%m%d_%H%M%S"), ".jsonl")
      )
      writeLines(download_file_text(final_batch$error_file_id), err_path, useBytes = TRUE)
      message("Saved error file to: ", err_path)
    }
  }

  list(class_df = bind_rows(all_rows), batches = bind_rows(batch_meta))
}

collapse_vals <- function(x) {
  x <- x[!is.na(unlist(x, use.names = FALSE))]
  if (length(x) == 0) return(NA_character_)
  paste(unique(unlist(x, use.names = FALSE)), collapse = "; ")
}

## 4) Run -------------------------------------------------------------------
stop_if_no_key()
df <- read_xlsx("Dataset 3.xlsx")

all_idx <- which(df$score >= 4)

all_res <- run_openai_batches(
  df                    = df,
  idx                   = all_idx,
  max_requests_per_batch = 50000,
  poll_seconds          = 60,
  label                 = "all"
)

df_labelled_all <- df %>%
  mutate(row_id = row_number()) %>%
  left_join(all_res$class_df, by = "row_id") %>%
  select(-row_id) %>%
  mutate(
    funding_source                = map_chr(funding_source, collapse_vals),
    clinical_purpose_model_output = map_chr(clinical_purpose_model_output, collapse_vals),
    dataset_origin                = map_chr(dataset_origin, collapse_vals),
    main_accuracy_metric          = map_chr(main_accuracy_metric, collapse_vals),
    input_data_modality           = map_chr(input_data_modality, collapse_vals)
  )

write_xlsx(df_labelled_all, "Dataset 4.xlsx")
