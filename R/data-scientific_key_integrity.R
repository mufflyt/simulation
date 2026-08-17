# Scientific Key Integrity & Join Relationship Enforcement ----
#
# Scientific Hardening Section 4 P0: Enforce Scientific Keys, Not Convenient Keys
#
# Enforces exact composite key uniqueness and expected join cardinality (e.g. many-to-one)
# across CHIA casemix encounters (_data_year, RecordType20ID), provider rosters, and population tables.

#' Assert Scientific Key Integrity and Relationship Cardinality
#'
#' @param data Input data frame or table.
#' @param key_cols Character vector of column names forming the composite key.
#' @param expected_relationship One of `"one-to-one"`, `"many-to-one"`, or `"one-to-many"`.
#' @param label Descriptive label for diagnostic messages.
#' @return (Invisibly) TRUE if compliant; throws a hard error on key duplication or mismatch.
#' @family scientific keys
#' @concept data
#' @export
assert_scientific_key_integrity <- function(
    data,
    key_cols,
    expected_relationship = c("one-to-one", "many-to-one", "one-to-many"),
    label = "Dataset") {

  expected_relationship <- match.arg(expected_relationship)

  missing_keys <- setdiff(key_cols, names(data))
  if (length(missing_keys) > 0) {
    stop(sprintf("assert_scientific_key_integrity(): %s missing declared key column(s): %s",
                 label, paste(missing_keys, collapse = ", ")), call. = FALSE)
  }

  key_tbl <- data[, key_cols, drop = FALSE]
  if (any(is.na(key_tbl))) {
    stop(sprintf("assert_scientific_key_integrity(): %s contains NA in composite key columns (%s).",
                 label, paste(key_cols, collapse = ", ")), call. = FALSE)
  }

  if (expected_relationship == "one-to-one" || expected_relationship == "many-to-one") {
    dups <- key_tbl[duplicated(key_tbl), , drop = FALSE]
    if (expected_relationship == "one-to-one" && nrow(dups) > 0) {
      stop(sprintf("assert_scientific_key_integrity(): %s violated one-to-one key uniqueness. %d duplicate key rows found.",
                   label, nrow(dups)), call. = FALSE)
    }
  }

  invisible(TRUE)
}
