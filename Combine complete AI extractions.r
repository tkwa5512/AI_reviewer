# Combine Complete AI Extractions
# Merge the three independent AI reviewer outputs into a single dataset using rule-based logic. Where reviewers disagree, precedence and majority-vote rules are applied.
# Filtering at the end also removes non-eligible papers.
# Input:  Dataset 4.xlsx, Dataset 5.xlsx, Dataset 6.xlsx
# Output: Dataset 7.xlsx

library(readxl)
library(writexl)
library(dplyr)

df1 <- read_xlsx("Dataset 4.xlsx")
df2 <- read_xlsx("Dataset 5.xlsx")
df3 <- read_xlsx("Dataset 6.xlsx")

extracted_cols <- c(
  "country_of_first_affiliation_of_first_author",
  "study_design_type",
  "input_data_modality",
  "clinical_purpose_model_output",
  "model_architecture",
  "dataset_origin",
  "funding_source",
  "main_accuracy_metric"
)

stopifnot(
  nrow(df1) == nrow(df2),
  nrow(df1) == nrow(df3),
  all(extracted_cols %in% names(df1)),
  all(extracted_cols %in% names(df2)),
  all(extracted_cols %in% names(df3))
)

n <- nrow(df1)
all_cols <- unique(c(names(df1), names(df2), names(df3)))

## Helper functions ---------------------------------------------------------
na_unclear <- function(x) {
  x <- trimws(as.character(x))
  x[x == "" | tolower(x) == "unclear"] <- NA_character_
  x
}

is_missing_loose <- function(x) {
  if (is.character(x)) is.na(x) | trimws(x) == "" else is.na(x)
}

split_terms <- function(x) {
  x <- na_unclear(x)
  if (length(x) != 1L) stop("split_terms expects length-1 input.")
  if (is.na(x)) return(character(0))
  parts <- trimws(unlist(strsplit(x, ";", fixed = TRUE), use.names = FALSE))
  parts[parts != ""]
}

collapse_terms <- function(terms) {
  terms <- trimws(terms[!is.na(terms)])
  terms <- terms[terms != ""]
  if (length(terms) == 0) return(NA_character_)
  paste(terms, collapse = "; ")
}

canonical_multivalue <- function(x) {
  terms <- split_terms(x)
  if (length(terms) == 0) return(NA_character_)
  paste(sort(unique(tolower(terms))), collapse = "|")
}

equal_ignore_order <- function(a, b) {
  ca <- canonical_multivalue(a)
  cb <- canonical_multivalue(b)
  !is.na(ca) && !is.na(cb) && identical(ca, cb)
}

is_single_other <- function(x) {
  terms <- split_terms(x)
  length(terms) == 1 && tolower(terms[1]) == "other"
}

remove_other_if_mixed <- function(x) {
  terms <- unique(split_terms(x))
  if (length(terms) == 0) return(NA_character_)
  lower <- tolower(terms)
  if ("other" %in% lower && length(terms) > 1)
    terms <- terms[lower != "other"]
  collapse_terms(unique(terms))
}

harmonize_clinical <- function(vec) {
  vec <- na_unclear(vec)
  vapply(vec, function(x) {
    if (is.na(x)) return(NA_character_)
    terms <- unique(split_terms(x))
    lower <- tolower(terms)

    pair1 <- c("atrial arrhythmia diagnosis", "ventricular arrhythmia diagnosis")
    pair2 <- c("atrial mechanism/substrate/localisation", "ventricular mechanism/substrate/localisation")

    if (all(pair1 %in% lower)) {
      terms <- c(terms[!(lower %in% pair1)], "Atrial and ventricular arrhythmia diagnosis")
      lower <- tolower(terms)
    }
    if (all(pair2 %in% lower)) {
      terms <- c(terms[!(lower %in% pair2)], "Atrial and ventricular mechanism/substrate/localisation")
    }

    collapse_terms(unique(terms))
  }, character(1))
}

combine_union_terms <- function(a, b, c) {
  terms <- unique(c(split_terms(a), split_terms(b), split_terms(c)))
  if (length(terms) == 0) return(NA_character_)
  collapse_terms(terms)
}

combine_dataset_origin <- function(a, b, c) {
  terms <- unique(c(split_terms(a), split_terms(b), split_terms(c)))
  if (length(terms) == 0) return(NA_character_)

  lower <- tolower(terms)
  if ((("mit-bih database" %in% lower) || ("ptb" %in% lower)) && !("physionet" %in% lower)) {
    terms <- c(terms, "Physionet")
    lower <- tolower(terms)
  }
  if (any(c("mit-bih database", "ptb", "physionet") %in% lower) && !("other public database" %in% lower))
    terms <- c(terms, "Other public database")

  collapse_terms(unique(terms))
}

combine_funding <- function(a, b, c) {
  terms <- unique(c(split_terms(a), split_terms(b), split_terms(c)))
  if (length(terms) == 0) return(NA_character_)
  lower <- tolower(terms)
  if (length(terms) > 1) terms <- terms[lower != "none"]
  collapse_terms(unique(terms))
}

first_term <- function(x) {
  terms <- split_terms(x)
  if (length(terms) == 0) return(NA_character_)
  terms[1]
}

mode_with_tiebreak <- function(v1, v2, v3) {
  vals <- c(df1 = v1, df2 = v2, df3 = v3)
  vals <- vals[!is.na(vals)]
  if (length(vals) == 0) return(NA_character_)
  tab <- table(vals)
  top_n <- max(tab)
  candidates <- names(tab)[tab == top_n]
  if (length(candidates) == 1) return(candidates)
  for (src in c("df1", "df3", "df2")) {
    v <- vals[src]
    if (!is.na(v) && v %in% candidates) return(as.character(v))
  }
  candidates[1]
}

## Combine columns ----------------------------------------------------------

# 0) Non-extracted columns: prefer df1, fill gaps from df2 then df3
df <- setNames(vector("list", length(all_cols)), all_cols)

for (col in setdiff(all_cols, extracted_cols)) {
  res <- if (col %in% names(df1)) df1[[col]] else if (col %in% names(df2)) df2[[col]] else df3[[col]]
  if (col %in% names(df2)) { idx <- is_missing_loose(res); res[idx] <- df2[[col]][idx] }
  if (col %in% names(df3)) { idx <- is_missing_loose(res); res[idx] <- df3[[col]][idx] }
  df[[col]] <- res
}

# 1) country_of_first_affiliation_of_first_author: df2 -> df1 -> df3
c1 <- na_unclear(df1$country_of_first_affiliation_of_first_author)
c2 <- na_unclear(df2$country_of_first_affiliation_of_first_author)
c3 <- na_unclear(df3$country_of_first_affiliation_of_first_author)

res_country <- c2
idx <- is.na(res_country); res_country[idx] <- c1[idx]
idx <- is.na(res_country); res_country[idx] <- c3[idx]
df$country_of_first_affiliation_of_first_author <- res_country

# 2) study_design_type
s1 <- na_unclear(df1$study_design_type)
s2 <- na_unclear(df2$study_design_type)
s3 <- na_unclear(df3$study_design_type)

res_study <- s1
base_lbl <- "Observational study/model development"
ext_lbl  <- "Observational study/model development with external validation"

idx <- !is.na(res_study) &
  res_study == base_lbl &
  ((!is.na(s2) & s2 == ext_lbl) | (!is.na(s3) & s3 == ext_lbl))
res_study[idx] <- ext_lbl

idx <- (is.na(res_study) | res_study == "Conference paper abstract only") & !is.na(s3)
res_study[idx] <- s3[idx]
idx <- (is.na(res_study) | res_study == "Conference paper abstract only") & !is.na(s2)
res_study[idx] <- s2[idx]

df$study_design_type <- res_study

# 3) input_data_modality
i1 <- na_unclear(df1$input_data_modality)
i2 <- na_unclear(df2$input_data_modality)
i3 <- na_unclear(df3$input_data_modality)

res_input <- rep(NA_character_, n)
idx <- !is.na(i2) & !is.na(i3) & mapply(equal_ignore_order, i2, i3)
res_input[idx] <- i2[idx]
idx <- is.na(res_input)
res_input[idx] <- i1[idx]
idx <- (is.na(res_input) | vapply(res_input, is_single_other, logical(1))) & !is.na(i3)
res_input[idx] <- i3[idx]
idx <- (is.na(res_input) | vapply(res_input, is_single_other, logical(1))) & !is.na(i2)
res_input[idx] <- i2[idx]

df$input_data_modality <- res_input

# 4) clinical_purpose_model_output: harmonize each reviewer first, then combine
p1 <- harmonize_clinical(df1$clinical_purpose_model_output)
p2 <- harmonize_clinical(df2$clinical_purpose_model_output)
p3 <- harmonize_clinical(df3$clinical_purpose_model_output)

res_purpose <- rep(NA_character_, n)
idx <- !is.na(p2) & !is.na(p3) & mapply(equal_ignore_order, p2, p3)
res_purpose[idx] <- p2[idx]
idx <- is.na(res_purpose)
res_purpose[idx] <- p1[idx]
idx <- (is.na(res_purpose) | vapply(res_purpose, is_single_other, logical(1))) & !is.na(p3)
res_purpose[idx] <- p3[idx]
idx <- (is.na(res_purpose) | vapply(res_purpose, is_single_other, logical(1))) & !is.na(p2)
res_purpose[idx] <- p2[idx]

df$clinical_purpose_model_output <- res_purpose

# 5) model_architecture
a1 <- na_unclear(df1$model_architecture)
a2 <- na_unclear(df2$model_architecture)
a3 <- na_unclear(df3$model_architecture)

res_arch <- rep(NA_character_, n)
idx <- !is.na(a2) & !is.na(a3) & mapply(equal_ignore_order, a2, a3)
res_arch[idx] <- a2[idx]
idx <- is.na(res_arch)
res_arch[idx] <- a1[idx]
idx <- (is.na(res_arch) | vapply(res_arch, is_single_other, logical(1))) & !is.na(a3)
res_arch[idx] <- a3[idx]
idx <- (is.na(res_arch) | vapply(res_arch, is_single_other, logical(1))) & !is.na(a2)
res_arch[idx] <- a2[idx]

df$model_architecture <- res_arch

# 6) dataset_origin
o1 <- na_unclear(df1$dataset_origin)
o2 <- na_unclear(df2$dataset_origin)
o3 <- na_unclear(df3$dataset_origin)

df$dataset_origin <- vapply(
  seq_len(n),
  function(i) combine_dataset_origin(o1[i], o2[i], o3[i]),
  character(1)
)

# 7) funding_source
f1 <- na_unclear(df1$funding_source)
f2 <- na_unclear(df2$funding_source)
f3 <- na_unclear(df3$funding_source)

df$funding_source <- vapply(
  seq_len(n),
  function(i) combine_funding(f1[i], f2[i], f3[i]),
  character(1)
)

# 8) main_accuracy_metric
m1 <- na_unclear(df1$main_accuracy_metric)
m2 <- na_unclear(df2$main_accuracy_metric)
m3 <- na_unclear(df3$main_accuracy_metric)

first1 <- vapply(m1, first_term, character(1))
first2 <- vapply(m2, first_term, character(1))
first3 <- vapply(m3, first_term, character(1))

df$single_metric <- vapply(
  seq_len(n),
  function(i) mode_with_tiebreak(first1[i], first2[i], first3[i]),
  character(1)
)

df$main_accuracy_metric <- vapply(
  seq_len(n),
  function(i) combine_union_terms(m1[i], m2[i], m3[i]),
  character(1)
)

df <- as.data.frame(df, stringsAsFactors = FALSE, check.names = FALSE)

## Post-processing ----------------------------------------------------------

# If funding metadata is absent from the dataset and only one reviewer reports
# a funding source, set funding_source to NA to avoid false positives
F1 <- na_unclear(df1$funding_source)
F2 <- na_unclear(df2$funding_source)
F3 <- na_unclear(df3$funding_source)
X  <- na_unclear(df2$Funding.Details)

idx <- which(is.na(F1) & is.na(F3) & !is.na(F2) & is.na(X))
df$funding_source[idx] <- NA

# Retain only the first country from multi-value entries
df$country_of_first_affiliation_of_first_author <-
  sub(";.*$", "", df$country_of_first_affiliation_of_first_author)

# Remove bot-blocked or unresolvable country strings
df$country_of_first_affiliation_of_first_author[
  df$country_of_first_affiliation_of_first_author ==
    "Unable to determine (manuscript page at medRxiv returned 403 Forbidden; affiliations/funding/acknowledgements could not be inspected there)."
] <- NA

# Collapse co-occurring atrial + ventricular diagnoses into combined label
combined_label    <- "Atrial and ventricular arrhythmia diagnosis"
atrial_label      <- "Atrial arrhythmia diagnosis"
ventricular_label <- "Ventricular arrhythmia diagnosis"

add_combo_label <- function(x) {
  if (is.na(x) || trimws(x) == "") return(x)
  cleaned <- unique(trimws(strsplit(x, ";\\s*")[[1]]))
  cleaned <- cleaned[cleaned != ""]
  has_atrial      <- atrial_label      %in% cleaned
  has_ventricular <- ventricular_label %in% cleaned
  has_combo       <- combined_label    %in% cleaned
  if (has_combo || (has_atrial && has_ventricular)) {
    cleaned <- c(cleaned[!cleaned %in% c(atrial_label, ventricular_label, combined_label)], combined_label)
  }
  paste(unique(cleaned), collapse = "; ")
}

df$clinical_purpose_model_output <- vapply(df$clinical_purpose_model_output, add_combo_label, character(1))

# Set unclassifiable study design types to NA
df$study_design_type[df$study_design_type == "other"] <- NA

## Final filtering ----------------------------------------------------------
df <- filter(df, !study_design_type %in% c("Book chapter", "Conference paper abstract only",
                                            "Review article", "Editorial/letter"))
df <- filter(df, !(input_data_modality == "other" | is.na(input_data_modality)))
df <- df[!vapply(df$input_data_modality, function(x) {
  if (is.na(x)) return(FALSE)
  terms <- tolower(trimws(unlist(strsplit(as.character(x), ";", fixed = TRUE))))
  terms <- terms[terms != ""]
  length(terms) > 0 && all(terms %in% c("other", "unclear"))
}, logical(1)), ]
df <- filter(df, model_architecture != "machine learning/AI not used" | is.na(model_architecture))

write_xlsx(df, "Dataset 7.xlsx")
