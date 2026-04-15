# Filter by Score
#   Requires input from score and new_score (Stage 1 from AI reviewer screening). Then Final_score should be the final manually allocated score.
#   Adds a publisher URL resolved from each paper's DOI.
# Input:  Dataset 2.xlsx  (columns include: score, new_score, Final_score, DOI)
# Output: Dataset 3.xlsx  (filtered rows + URL column)

library(readxl)
library(writexl)
library(dplyr)
library(stringr)
library(httr)

## 1) Load data and resolve scores ------------------------------------------
df <- read_xlsx("Dataset 2.xlsx")

# Apply Stage 2 and manual scores where Stage 1 left a borderline (3)
df$score[df$score == 3] <- df$new_score[df$score == 3]
df$score[df$score == 3] <- df$Final_score[df$score == 3]

df <- df[df$score > 3, ]

## 2) Resolve DOI to publisher URL ------------------------------------------
resolve_doi_to_url <- function(doi, timeout_sec = 60) {
  if (is.na(doi) || str_trim(doi) == "") return(NA_character_)

  doi <- str_remove(str_trim(doi), "^https?://(dx\\.)?doi\\.org/")
  resp <- tryCatch(
    GET(
      paste0("https://doi.org/", doi),
      config(followlocation = FALSE),
      timeout(timeout_sec),
      user_agent("DOI-Resolver/1.0")
    ),
    error = function(e) NULL
  )

  if (is.null(resp)) return(NA_character_)
  loc <- headers(resp)[["location"]]
  if (is.null(loc) || loc == "") return(NA_character_)
  loc
}

df$URL <- vapply(df$DOI, resolve_doi_to_url, FUN.VALUE = character(1))

# Inspect papers with missing URLs and add them manually before proceeding
missing_url <- df[is.na(df$URL), c("firstAuthor", "title", "DOI")]
if (nrow(missing_url) > 0) {
  message(nrow(missing_url), " paper(s) with missing URL:")
  print(missing_url)
}

# Add any missing URLs manually

write_xlsx(df, "Dataset 3.xlsx")
