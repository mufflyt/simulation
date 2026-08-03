# 39-severity_sandvik.R -------------------------------------------------
# Grades the SWAN incontinence panel with a published severity
# instrument, and exposes the severity threshold as the seam through
# which surgical candidacy enters the demand chain.

#' Score the Sandvik Incontinence Severity Index on a SWAN panel
#'
#' Applies the Sandvik Incontinence Severity Index — the product of a
#' four-level leakage frequency and a three-level leakage amount — to a
#' panel produced by [build_swan_incontinence_panel()], and flags which
#' participant-visits cross a configurable severity threshold for
#' surgical candidacy.
#'
#' @details
#' The index takes only eight attainable values (1, 2, 3, 4, 6, 8, 9,
#' 12) and is cut into four published categories: 1-2 slight, 3-6
#' moderate, 8-9 severe, 12 very severe. Those cutpoints are fixed by
#' the instrument and are locked by regression tests; they are not
#' tunable here.
#'
#' **The SWAN amount item does not match Sandvik's scale.** SWAN's
#' `AMTLEAK` format runs over four levels (`1: Drop` through `4: Wet
#' floor`) where Sandvik specifies three (drops, small amounts, large
#' amounts). The default crosswalk collapses "wet floor" onto Sandvik's
#' "large amounts". That collapse is a judgement, not a published
#' mapping, so it is exposed as an argument, recorded in the returned
#' provenance as `derived_by_analogy`, and should be varied as a
#' sensitivity. It is the single largest source of non-sampling
#' uncertainty in the severity distribution this function returns.
#'
#' **The frequency item is assumed to be scale-compatible.** SWAN's
#' `DAYSLEAK` format is treated as already ordered 1-4 in the Sandvik
#' direction. Any code outside 1-4 is an error rather than an `NA`.
#'
#' Continent participant-visits (`leakage_ever` is `FALSE`) score `NA`,
#' not zero: they are outside the instrument's domain, and a zero would
#' silently join the "slight" end of a severity distribution. Their
#' `surgical_threshold_met` is `FALSE`.
#'
#' Where the source visit measured amount for stress leakage only
#' (`amount_scope == "stress_specific"`, true at SWAN visits 12, 13 and
#' 15), the resulting index understates severity for urgency-predominant
#' women and a warning is emitted.
#'
#' @param swan_incontinence_panel A data frame with one row per
#'   participant-visit, as returned by [build_swan_incontinence_panel()].
#'   Must contain `swan_id`, `visit`, `leakage_ever`, `frequency_level`,
#'   `amount_level` and `amount_scope`.
#' @param amount_crosswalk A named integer vector mapping SWAN
#'   `AMTLEAK` codes onto Sandvik's three amount levels. Names are the
#'   SWAN codes as strings; values must lie in 1-3. Defaults to
#'   `c("1" = 1L, "2" = 2L, "3" = 3L, "4" = 3L)`. Any observed amount
#'   code absent from the crosswalk is an error, never a silent `NA`.
#' @param surgical_threshold A length-one character giving the lowest
#'   Sandvik category counted as a surgical candidate. One of
#'   `"slight"`, `"moderate"`, `"severe"` (the default) or
#'   `"very_severe"`.
#' @param verbose A length-one logical. When `TRUE` (the default) the
#'   function narrates its inputs, the crosswalk in force, the resulting
#'   severity distribution, and the threshold-crossing count to the
#'   console via `base::message()`. Nothing is written to disk.
#'
#' @return The input panel with four columns appended:
#'   `sandvik_amount_level` (integer, 1-3 after the crosswalk),
#'   `sandvik_index` (integer, one of the eight attainable products),
#'   `sandvik_category` (an ordered factor with levels `slight <
#'   moderate < severe < very_severe`) and `surgical_threshold_met`
#'   (logical). A `severity_provenance` attribute records the
#'   instrument, the crosswalk in force, and the calibration tier of
#'   each.
#'
#' @examples
#' # ---------------------------------------------------------------
#' # Example 1 - default scoring at the severe threshold
#' # ---------------------------------------------------------------
#' swan_incontinence_panel <- tibble::tibble(
#'   swan_id = c(101L, 102L, 103L, 104L),
#'   visit = 0L,
#'   leakage_ever = c(TRUE, TRUE, TRUE, FALSE),
#'   frequency_level = c(1L, 3L, 4L, NA_integer_),
#'   amount_level = c(1L, 3L, 4L, NA_integer_),
#'   urge_amount_level = NA_integer_,
#'   amount_scope = "overall"
#' )
#'
#' severity_scored_panel <- score_sandvik_severity(
#'   swan_incontinence_panel = swan_incontinence_panel,
#'   amount_crosswalk = c("1" = 1L, "2" = 2L, "3" = 3L, "4" = 3L),
#'   surgical_threshold = "severe",
#'   verbose = FALSE
#' )
#' severity_scored_panel$sandvik_index
#' #> [1]  1  9 12 NA
#' as.character(severity_scored_panel$sandvik_category)
#' #> [1] "slight"      "severe"      "very_severe" NA
#' severity_scored_panel$surgical_threshold_met
#' #> [1] FALSE  TRUE  TRUE FALSE
#'
#' # ---------------------------------------------------------------
#' # Example 2 - sensitivity on the wet-floor collapse, narrated
#' # ---------------------------------------------------------------
#' # Treating "wet floor" as its own tier rather than folding it into
#' # "large amounts" is the conservative reading; it moves women out
#' # of the surgical pool.
#' conservative_panel <- score_sandvik_severity(
#'   swan_incontinence_panel = swan_incontinence_panel,
#'   amount_crosswalk = c("1" = 1L, "2" = 2L, "3" = 2L, "4" = 3L),
#'   surgical_threshold = "severe",
#'   verbose = TRUE
#' )
#' #> [sandvik] input: 4 participant-visits, 3 with leakage
#' #> [sandvik] amount crosswalk: 1->1, 2->2, 3->2, 4->3
#' #> [sandvik] index computed for 3 of 4 rows
#' #> [sandvik] distribution: slight 1, moderate 1, severe 0,
#' #>           very_severe 1
#' #> [sandvik] threshold 'severe': 1 of 4 rows met
#' conservative_panel$sandvik_index
#' #> [1]  1  6 12 NA
#'
#' # ---------------------------------------------------------------
#' # Example 3 - a permissive threshold and the provenance record
#' # ---------------------------------------------------------------
#' # Moderate-or-worse is the wider reading of who might reach an
#' # operating room; it roughly doubles the candidate pool here.
#' permissive_panel <- score_sandvik_severity(
#'   swan_incontinence_panel = swan_incontinence_panel,
#'   amount_crosswalk = c("1" = 1L, "2" = 2L, "3" = 3L, "4" = 3L),
#'   surgical_threshold = "moderate",
#'   verbose = FALSE
#' )
#' sum(permissive_panel$surgical_threshold_met)
#' #> [1] 2
#'
#' severity_provenance <- attr(permissive_panel, "severity_provenance")
#' severity_provenance$instrument
#' #> [1] "Sandvik Incontinence Severity Index"
#' severity_provenance$instrument_tier
#' #> [1] "calibrated"
#' severity_provenance$amount_crosswalk_tier
#' #> [1] "derived_by_analogy"
#'
#' @importFrom assertthat assert_that
#' @importFrom dplyr mutate
#' @importFrom rlang .data
#' @export
score_sandvik_severity <- function(swan_incontinence_panel,
                                   amount_crosswalk = c(
                                     "1" = 1L, "2" = 2L,
                                     "3" = 3L, "4" = 3L
                                   ),
                                   surgical_threshold = "severe",
                                   verbose = TRUE) {
  assertthat::assert_that(is.data.frame(swan_incontinence_panel))
  assertthat::assert_that(is.logical(verbose))
  assertthat::assert_that(length(verbose) == 1L)
  assertthat::assert_that(is.character(surgical_threshold))
  assertthat::assert_that(length(surgical_threshold) == 1L)
  assertthat::assert_that(
    surgical_threshold %in% sandvik_category_levels()
  )

  assert_severity_input_columns(swan_incontinence_panel)
  validated_crosswalk <- validate_amount_crosswalk(amount_crosswalk)

  announce_severity_step(
    verbose,
    sprintf("input: %d participant-visits, %d with leakage",
            nrow(swan_incontinence_panel),
            sum(swan_incontinence_panel$leakage_ever, na.rm = TRUE))
  )
  announce_severity_step(
    verbose,
    paste0("amount crosswalk: ",
           paste(names(validated_crosswalk), validated_crosswalk,
                 sep = "->", collapse = ", "))
  )

  warn_on_stress_specific_scope(swan_incontinence_panel)
  assert_frequency_scale(swan_incontinence_panel$frequency_level)

  sandvik_amount_level <- apply_amount_crosswalk(
    swan_incontinence_panel$amount_level, validated_crosswalk
  )

  index_is_defined <- !is.na(swan_incontinence_panel$leakage_ever) &
    swan_incontinence_panel$leakage_ever &
    !is.na(swan_incontinence_panel$frequency_level) &
    !is.na(sandvik_amount_level)

  sandvik_index <- rep(NA_integer_, nrow(swan_incontinence_panel))
  sandvik_index[index_is_defined] <- as.integer(
    swan_incontinence_panel$frequency_level[index_is_defined] *
      sandvik_amount_level[index_is_defined]
  )

  sandvik_category <- cut_sandvik_index(sandvik_index)

  threshold_rank <- match(surgical_threshold, sandvik_category_levels())
  surgical_threshold_met <- !is.na(sandvik_category) &
    as.integer(sandvik_category) >= threshold_rank

  severity_scored_panel <- dplyr::mutate(
    swan_incontinence_panel,
    sandvik_amount_level = sandvik_amount_level,
    sandvik_index = sandvik_index,
    sandvik_category = sandvik_category,
    surgical_threshold_met = surgical_threshold_met
  )

  announce_severity_step(
    verbose,
    sprintf("index computed for %d of %d rows",
            sum(index_is_defined), nrow(severity_scored_panel))
  )
  announce_severity_step(
    verbose,
    paste0(
      "distribution: ",
      paste(sandvik_category_levels(),
            as.integer(table(
              factor(as.character(sandvik_category),
                     levels = sandvik_category_levels())
            )),
            collapse = ", ")
    )
  )
  announce_severity_step(
    verbose,
    sprintf("threshold '%s': %d of %d rows met",
            surgical_threshold, sum(surgical_threshold_met),
            nrow(severity_scored_panel))
  )

  attr(severity_scored_panel, "severity_provenance") <- list(
    instrument = "Sandvik Incontinence Severity Index",
    instrument_tier = "calibrated",
    instrument_source = paste(
      "Sandvik et al., validated frequency x amount severity index;",
      "cutpoints 1-2 slight, 3-6 moderate, 8-9 severe, 12 very severe"
    ),
    amount_crosswalk = validated_crosswalk,
    amount_crosswalk_tier = "derived_by_analogy",
    amount_crosswalk_note = paste(
      "SWAN AMTLEAK has four levels against Sandvik's three;",
      "the collapse is a judgement and should be varied"
    ),
    frequency_scale_tier = "assumed_identity",
    surgical_threshold = surgical_threshold,
    scored_at = Sys.time()
  )

  severity_scored_panel
}


# helpers ---------------------------------------------------------------

#' @noRd
announce_severity_step <- function(verbose, step_text) {
  if (isTRUE(verbose)) {
    message("[sandvik] ", step_text)
  }
  invisible(NULL)
}

#' @noRd
sandvik_category_levels <- function() {
  c("slight", "moderate", "severe", "very_severe")
}

#' @noRd
assert_severity_input_columns <- function(swan_incontinence_panel) {
  required_severity_columns <- c(
    "swan_id", "visit", "leakage_ever", "frequency_level",
    "amount_level", "amount_scope"
  )
  absent_columns <- setdiff(
    required_severity_columns, names(swan_incontinence_panel)
  )
  if (length(absent_columns) > 0L) {
    stop(
      "swan_incontinence_panel is missing column(s): ",
      paste(absent_columns, collapse = ", "),
      ". Build it with build_swan_incontinence_panel().",
      call. = FALSE
    )
  }
  invisible(TRUE)
}

#' @noRd
validate_amount_crosswalk <- function(amount_crosswalk) {
  assertthat::assert_that(is.numeric(amount_crosswalk))
  assertthat::assert_that(length(amount_crosswalk) > 0L)
  assertthat::assert_that(!is.null(names(amount_crosswalk)))
  assertthat::assert_that(all(nzchar(names(amount_crosswalk))))
  crosswalk_values <- as.integer(amount_crosswalk)
  if (any(crosswalk_values < 1L | crosswalk_values > 3L)) {
    stop(
      "amount_crosswalk must map onto Sandvik's three amount levels ",
      "(1-3); got ", paste(sort(unique(crosswalk_values)),
                           collapse = ", "), ".",
      call. = FALSE
    )
  }
  stats::setNames(crosswalk_values, names(amount_crosswalk))
}

#' @noRd
apply_amount_crosswalk <- function(amount_level, validated_crosswalk) {
  observed_codes <- unique(stats::na.omit(amount_level))
  unmapped_codes <- setdiff(
    as.character(observed_codes), names(validated_crosswalk)
  )
  if (length(unmapped_codes) > 0L) {
    stop(
      "amount_level contains unmapped SWAN code(s): ",
      paste(sort(unmapped_codes), collapse = ", "),
      ". Extend amount_crosswalk rather than letting them become NA.",
      call. = FALSE
    )
  }
  unname(validated_crosswalk[as.character(amount_level)])
}

#' @noRd
assert_frequency_scale <- function(frequency_level) {
  offending_codes <- setdiff(
    unique(stats::na.omit(frequency_level)), 1:4
  )
  if (length(offending_codes) > 0L) {
    stop(
      "frequency_level must lie on the four-level SWAN DAYSLEAK scale; ",
      "got ", paste(sort(offending_codes), collapse = ", "), ".",
      call. = FALSE
    )
  }
  invisible(TRUE)
}

#' @noRd
warn_on_stress_specific_scope <- function(swan_incontinence_panel) {
  stress_specific_rows <- sum(
    swan_incontinence_panel$amount_scope == "stress_specific",
    na.rm = TRUE
  )
  if (stress_specific_rows > 0L) {
    warning(
      stress_specific_rows,
      " row(s) have amount_scope == 'stress_specific'. At SWAN visits ",
      "12, 13 and 15 the amount item is cough-specific, so the index ",
      "understates severity for urgency-predominant women.",
      call. = FALSE
    )
  }
  invisible(TRUE)
}

#' @noRd
cut_sandvik_index <- function(sandvik_index) {
  category_label <- rep(NA_character_, length(sandvik_index))
  category_label[sandvik_index %in% c(1L, 2L)] <- "slight"
  category_label[sandvik_index %in% c(3L, 4L, 6L)] <- "moderate"
  category_label[sandvik_index %in% c(8L, 9L)] <- "severe"
  category_label[sandvik_index %in% 12L] <- "very_severe"

  unclassified_index <- setdiff(
    stats::na.omit(sandvik_index[is.na(category_label)]), numeric(0)
  )
  if (length(unclassified_index) > 0L) {
    stop(
      "Sandvik index took unattainable value(s): ",
      paste(sort(unique(unclassified_index)), collapse = ", "),
      ". The index is a product of 1-4 and 1-3.",
      call. = FALSE
    )
  }

  factor(category_label, levels = sandvik_category_levels(),
         ordered = TRUE)
}
