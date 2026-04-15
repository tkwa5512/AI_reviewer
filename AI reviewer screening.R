# AI Reviewer Screening
# Stage 1: Title/abstract screening (all papers) → score 1-5
# Stage 2: Web-search re-scoring for borderline papers (score == 3)
# Input:  Dataset 1.xlsx  (columns: Year, firstAuthor, title, Abstract, Journal, DOI)
# Output: Dataset 2.xlsx  (input columns + score)

library(readxl)
library(writexl)
library(dplyr)
library(purrr)
library(httr)
library(jsonlite)

## 1) API key ---------------------------------------------------------------
# Set API key from paid OpenAI account

## 2) Load data -------------------------------------------------------------
df <- read_xlsx("Dataset 1.xlsx")

required_cols <- c("title", "Abstract", "Year", "firstAuthor", "Journal", "DOI")
missing_cols <- setdiff(required_cols, names(df))
if (length(missing_cols) > 0)
  stop(paste("Missing required columns:", paste(missing_cols, collapse = ", ")))

## 3) Shared inclusion/exclusion criteria -----------------------------------
INCLUSION_CRITERIA <- paste0(
  "Inclusion concept:\n",
  "- Human data (any population)\n",
  "- Uses artificial intelligence / machine learning / deep learning\n",
  "- Interprets a cardiac electrogram (e.g., 12-lead ECG, single-lead ECG, ",
  "Holter/continuous ECG, intracardiac EGMs, device EGMs)\n",
  "- Any clinically meaningful outcome. Clinically meaningful outcomes could reasonably ",
  "influence diagnosis, prognosis, or management at the patient or clinician level. ",
  "Technical tasks like signal reconstruction, denoising, compression, or feature ",
  "extraction without a clinical endpoint should not be considered clinically meaningful.\n",
  "- Primary original research articles. Exclude reviews, editorials, commentaries, ",
  "letters, perspectives, protocols, case reports, and conference abstracts\n\n",
  "Exclusion concept:\n",
  "- Non-human only\n",
  "- No AI/ML\n",
  "- Uses only NON-cardiac signals (EEG, EMG, MEG, etc.)\n"
)

SCORE_RULES <- paste0(
  "Scoring rules:\n",
  "5 = definitely include\n",
  "4 = likely include\n",
  "3 = borderline / unclear\n",
  "2 = likely exclude\n",
  "1 = definitely exclude\n"
)

## 4) Stage 1: title/abstract screener -------------------------------------
build_screen_prompt <- function(title, abstract_text) {
  if (is.na(abstract_text) || trimws(abstract_text) == "")
    abstract_text <- "ABSTRACT NOT AVAILABLE."

  paste0(
    "You are a meticulous medical researcher. Determine how suitable this paper is ",
    "for inclusion in a systematic review.\n\n",
    "Use ONLY the title and abstract.\n\n",
    INCLUSION_CRITERIA, "\n",
    SCORE_RULES, "\n",
    "Return ONLY a JSON object with one field:\n\n",
    "{\n  \"score\": integer   // from 1 to 5\n}\n\n",
    "TITLE:\n", title, "\n\n",
    "ABSTRACT:\n", abstract_text
  )
}

call_openai_score <- function(title, abstract_text,
                              model = "gpt-5.2",
                              max_retries = 5) {
  prompt <- build_screen_prompt(title, abstract_text)
  attempt <- 1
  wait_sec <- 2

  repeat {
    res <- tryCatch({
      POST(
        url = "https://api.openai.com/v1/chat/completions",
        add_headers(Authorization = paste("Bearer", Sys.getenv("OPENAI_API_KEY"))),
        content_type_json(),
        encode = "json",
        body = toJSON(list(
          model = model,
          messages = list(
            list(role = "system", content = "Respond ONLY with pure JSON."),
            list(role = "user", content = prompt)
          ),
          temperature = 0,
          response_format = list(type = "json_object")
        ), auto_unbox = TRUE),
        timeout(600)
      )
    }, error = function(e) {
      message("Request error: ", e$message)
      NULL
    })

    if (is.null(res)) return(NULL)

    if (http_error(res)) {
      status <- status_code(res)
      msg <- content(res, "text", encoding = "UTF-8")
      message("HTTP error (attempt ", attempt, "): ", status, " | ", msg)

      if (status == 429 && attempt < max_retries) {
        Sys.sleep(wait_sec)
        attempt <- attempt + 1
        wait_sec <- wait_sec * 2
        next
      } else {
        return(NULL)
      }
    }

    txt <- content(res)$choices[[1]]$message$content
    out <- tryCatch(fromJSON(txt), error = function(e) {
      message("JSON parse error: ", e$message, "\nRaw: ", txt)
      NULL
    })

    if (is.null(out$score)) return(NULL)
    return(max(1L, min(5L, as.integer(out$score))))
  }
}

## 5) Stage 1 run -----------------------------------------------------------
df <- df[sample(nrow(df)), ]

chunk_size <- 500
idx_chunks <- split(seq_len(nrow(df)), ceiling(seq_len(nrow(df)) / chunk_size))
all_scores <- vector("list", length(idx_chunks))

for (i in seq_along(idx_chunks)) {
  idx <- idx_chunks[[i]]
  message("Processing chunk ", i, "/", length(idx_chunks),
          " (rows ", min(idx), "-", max(idx), ")")

  all_scores[[i]] <- tibble(
    row   = idx,
    score = map_int(idx, ~ {
      s <- call_openai_score(title = df$title[.x], abstract_text = df$Abstract[.x])
      if (is.null(s)) NA_integer_ else as.integer(s)
    })
  )

  df_partial <- df[all_scores[[i]]$row, ] %>%
    mutate(score = all_scores[[i]]$score)
  write_xlsx(df_partial, paste0("partial_chunk_", i, ".xlsx"))

  Sys.sleep(1)
}

scores_all <- bind_rows(all_scores)
df <- df %>% mutate(score = scores_all$score)

## 6) Stage 2: web-search re-scorer for borderline papers ------------------
build_prompt_web <- function(title, journal, authors) {
  paste0(
    "You are a meticulous medical researcher. Determine how suitable this paper is ",
    "for inclusion in a systematic review.\n\n",
    "Use web search to find detailed information about the following scientific article. ",
    "Identify the correct paper using the title, journal, and first author and open the ",
    "full paper from the publisher site. Do not rely solely on abstract or PubMed metadata. ",
    "Read the whole article in detail.\n\n",
    INCLUSION_CRITERIA, "\n",
    SCORE_RULES, "\n",
    "Return ONLY a JSON object with one field:\n\n",
    "{\n  \"score\": integer   // from 1 to 5\n}\n\n",
    "ARTICLE IDENTIFIERS:\n",
    "Title: ", title, "\n",
    "Journal: ", journal, "\n",
    "First author: ", authors
  )
}

parse_score <- function(txt) {
  score_val <- NULL

  if (grepl("^\\s*\\{", txt, perl = TRUE)) {
    tmp <- tryCatch(fromJSON(txt, simplifyVector = FALSE), error = function(e) NULL)
    if (!is.null(tmp$score))
      score_val <- suppressWarnings(as.integer(tmp$score))
  }

  if (is.null(score_val) || is.na(score_val)) {
    m <- regexec('"score"\\s*[:=]\\s*([1-5])', txt, perl = TRUE)
    r <- regmatches(txt, m)[[1]]
    if (length(r) >= 2) score_val <- as.integer(r[2])
  }

  if (is.null(score_val) || is.na(score_val)) {
    m <- regexec("\\b([1-5])\\b", txt, perl = TRUE)
    r <- regmatches(txt, m)[[1]]
    if (length(r) >= 2) score_val <- as.integer(r[2])
  }

  if (is.null(score_val) || is.na(score_val)) {
    message("Could not parse score from model response. Raw: ", txt)
    return(NULL)
  }

  tibble(new_score = score_val)
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

call_openai_classify_web <- function(title, journal, authors,
                                     model = "gpt-5-mini",
                                     max_retries = 4) {
  input_text <- build_prompt_web(title, journal, authors)
  attempt <- 1
  wait_sec <- 5

  repeat {
    res <- tryCatch({
      POST(
        url = "https://api.openai.com/v1/responses",
        add_headers(Authorization = paste("Bearer", Sys.getenv("OPENAI_API_KEY"))),
        content_type_json(),
        encode = "json",
        body = toJSON(list(
          model  = model,
          input  = input_text,
          tools  = list(list(type = "web_search")),
          text = list(
            format = list(
              type = "json_schema",
              name = "score_schema",
              strict = TRUE,
              schema = list(
                type = "object",
                properties = list(
                  score = list(type = "integer", enum = list(1, 2, 3, 4, 5))
                ),
                required = list("score"),
                additionalProperties = FALSE
              )
            )
          )
        ), auto_unbox = TRUE)
      )
    }, error = function(e) {
      message("Error calling API (attempt ", attempt, "): ", e$message)
      NULL
    })

    if (is.null(res)) return(NULL)

    if (http_error(res)) {
      status <- status_code(res)
      msg <- content(res, as = "text", encoding = "UTF-8")
      message("HTTP error (attempt ", attempt, "): ", status, " | ", msg)

      if (status == 429 && attempt < max_retries) {
        wait_from_msg <- NA_real_
        m <- regexec("try again in ([0-9.]+)s", msg, ignore.case = TRUE)
        r <- regmatches(msg, m)[[1]]
        if (length(r) >= 2) wait_from_msg <- as.numeric(r[2])
        if (is.na(wait_from_msg)) wait_from_msg <- wait_sec

        message("Rate limited. Waiting ", wait_from_msg, " seconds...")
        Sys.sleep(wait_from_msg)
        attempt <- attempt + 1
        wait_sec <- wait_sec * 2
        next
      } else {
        return(NULL)
      }
    }

    resp <- content(res, as = "parsed", simplifyVector = FALSE)
    txt <- extract_response_text(resp)

    if (is.null(txt)) {
      message("No text found in response.")
      return(NULL)
    }

    Sys.sleep(10)
    return(parse_score(txt))
  }
}

## 7) Stage 2 run -----------------------------------------------------------
all_idx <- which(df$score == 3)
class_list_all <- vector("list", length(all_idx))

for (i in seq_along(all_idx)) {
  idx <- all_idx[i]
  class_list_all[[i]] <- call_openai_classify_web(
    title   = df$title[idx],
    journal = df$Journal[idx],
    authors = df$firstAuthor[idx]
  )

  if (i %% 25 == 0) {
    saveRDS(class_list_all, "checkpoint_class_list.rds")
    message("Checkpoint saved at i = ", i)
  }
}

# Retry any failed calls
failed <- which(sapply(class_list_all, is.null))
if (length(failed) > 0) {
  for (i in failed) {
    idx <- all_idx[i]
    class_list_all[[i]] <- call_openai_classify_web(
      title   = df$title[idx],
      journal = df$Journal[idx],
      authors = df$firstAuthor[idx]
    )
  }
}

class_df_all <- bind_cols(
  tibble(row_id = all_idx),
  bind_rows(class_list_all)
)

df <- df %>%
  mutate(row_id = row_number()) %>%
  left_join(class_df_all, by = "row_id") %>%
  select(-row_id)

write_xlsx(df, "Dataset 2.xlsx")