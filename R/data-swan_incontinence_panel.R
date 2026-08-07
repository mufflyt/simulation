# 38-swan_incontinence_panel.R ------------------------------------------
# Harmonises SWAN's per-visit incontinence items into the longitudinal
# state panel that dmdm_transition_data() (R/demand-dmdm_fit_transitions) already expects as its
# input. Nothing in the package built that panel; it lived only in
# inst/legacy/.

#' Build a harmonised SWAN incontinence state panel
#'
#' Reshapes a set of wide, per-visit SWAN frames into one tidy
#' participant-visit panel carrying the three items the Sandvik
#' Incontinence Severity Index needs: whether leakage occurred, how
#' often, and how much. SWAN renames these items between visits, so the
#' reshape is driven by an explicit, evidence-gated crosswalk rather
#' than by pasting a visit suffix onto an assumed stem.
#'
#' Three naming facts make a suffix-pasting approach wrong, and each is
#' locked by a regression test:
#'
#' * The frequency stem is `DAYSLEA` at visits 0, 5 and 10 but `LEKDAYS`
#'   at visits 7, 8, 9, 12, 13 and 15.
#' * The amount stem is `AMTLEAK` at visit 0 and `LEKAMNT` from visit 7
#'   onward, so **visit 10 pairs `DAYSLEA10` with `LEKAMNT10`** — the
#'   two stems do not move together.
#' * The leakage gate is `INVOLEA` at most visits but `LEKINVO` at
#'   visits 7 through 9.
#'
#' Visit 5 carries a frequency item and no amount item at all, so the
#' Sandvik index is not computable there; the panel records
#' `amount_scope = "none"` rather than emitting a silent `NA` that later
#' reads as "not severe". Visits 12, 13 and 15 split amount into a
#' cough-specific (`LEKAMNT`) and an urge-specific (`URGEAMT`) item, so
#' those visits are marked `amount_scope = "stress_specific"` and both
#' columns are returned.
#'
#' Visits whose variable names have not been verified against the SWAN
#' data dictionary are refused with an error. Guessing a name produces a
#' column of `NA` that propagates all the way to a severity distribution
#' without ever failing, which is the failure mode this module exists to
#' prevent.
#'
#' Negative SWAN sentinel codes (`-1`, `-7`, `-8`, `-9`) become `NA`.
#' Labelled factor values of the form `"(2) Yes"` are reduced to their
#' integer code.
#'
#' @param swan_visit_frames A named list of wide SWAN data frames, one
#'   per visit. Names must be the visit numbers as strings, for example
#'   `list("0" = swan_visit_00, "7" = swan_visit_07)`. Each frame must
#'   contain the participant identifier column and the incontinence
#'   items named for that visit. Frames may carry any number of other
#'   columns; they are ignored.
#' @param variable_map A data frame giving the visit-to-variable
#'   crosswalk, or `NULL` (the default) to use the crosswalk shipped in
#'   `inst/extdata/swan/swan_incontinence_variable_map.csv`. A supplied
#'   frame must contain the columns `visit`, `ever_variable`,
#'   `frequency_variable`, `amount_variable`, `urge_amount_variable`,
#'   `amount_scope` and `verification_status`.
#' @param participant_id_column A length-one character naming the
#'   participant identifier column in every visit frame. Defaults to
#'   `"SWANID"`, the sole participant key used across this project.
#' @param verbose A length-one logical. When `TRUE` (the default) the
#'   function narrates its inputs, each reshape step, the per-visit row
#'   counts, and a summary of the returned panel to the console via
#'   `base::message()`. No file is written and no logging package is
#'   used.
#'
#' @return A tibble with one row per participant-visit and the columns
#'   `swan_id` (integer), `visit` (integer), `leakage_ever` (logical),
#'   `frequency_level` (integer, 1-4 on the SWAN `DAYSLEAK` scale),
#'   `amount_level` (integer, 1-4 on the SWAN `AMTLEAK` scale),
#'   `urge_amount_level` (integer, `NA` unless the visit splits amount by
#'   type) and `amount_scope` (character; one of `"overall"`,
#'   `"stress_specific"` or `"none"`). The result carries a
#'   `swan_panel_provenance` attribute recording which visits were used
#'   and which variable names each was read from.
#'
#' @examples
#' # ---------------------------------------------------------------
#' # Example 1 - one visit, the shipped crosswalk, quiet
#' # ---------------------------------------------------------------
#' swan_visit_00 <- tibble::tibble(
#'   SWANID = c(101L, 102L, 103L),
#'   INVOLEA0 = c("(2) Yes", "(1) No", "(2) Yes"),
#'   DAYSLEA0 = c("(3) 1-3 times/week", "(-1) Not applicable",
#'                "(4) Every day"),
#'   AMTLEAK0 = c("(1) Drop", "(-1) Not applicable", "(4) Wet floor")
#' )
#'
#' swan_incontinence_panel <- build_swan_incontinence_panel(
#'   swan_visit_frames = list("0" = swan_visit_00),
#'   variable_map = NULL,
#'   participant_id_column = "SWANID",
#'   verbose = FALSE
#' )
#' swan_incontinence_panel
#' #> # A tibble: 3 x 7
#' #>   swan_id visit leakage_ever frequency_level amount_level
#' #>     <int> <int> <lgl>                  <int>        <int>
#' #> 1     101     0 TRUE                       3            1
#' #> 2     102     0 FALSE                     NA           NA
#' #> 3     103     0 TRUE                       4            4
#' #> # i 2 more variables: urge_amount_level <int>,
#' #> #   amount_scope <chr>
#' # Note row 2: a continent woman keeps her row and scores NA on
#' # both items, rather than being dropped or coded zero.
#'
#' # ---------------------------------------------------------------
#' # Example 2 - the visit-10 naming trap, narrated to the console
#' # ---------------------------------------------------------------
#' # DAYSLEA10 pairs with LEKAMNT10. A suffix-pasting reshape drops
#' # the amount item here and every visit-10 woman scores as mild.
#' swan_visit_10 <- tibble::tibble(
#'   SWANID = c(201L, 202L),
#'   INVOLEA10 = c("(2) Yes", "(2) Yes"),
#'   DAYSLEA10 = c("(2) 1-3 times/month", "(4) Every day"),
#'   LEKAMNT10 = c("(1) Drop", "(3) Large amount")
#' )
#'
#' visit_10_panel <- build_swan_incontinence_panel(
#'   swan_visit_frames = list("10" = swan_visit_10),
#'   variable_map = NULL,
#'   participant_id_column = "SWANID",
#'   verbose = TRUE
#' )
#' #> [swan-panel] input: 1 visit frame(s); id column = SWANID
#' #> [swan-panel] crosswalk: 16 visit rows, 9 marked verified
#' #> [swan-panel] visit 10: gate INVOLEA10, freq DAYSLEA10,
#' #>              amount LEKAMNT10 (scope overall)
#' #> [swan-panel] visit 10: 2 wide rows -> 2 panel rows
#' #> [swan-panel] output: 2 rows, 2 participants, 1 visit(s)
#' visit_10_panel$amount_level
#' #> [1] 1 3
#'
#' # ---------------------------------------------------------------
#' # Example 3 - a hand-supplied crosswalk for a renamed extract
#' # ---------------------------------------------------------------
#' # Sites that pre-harmonise their extract can override the map
#' # instead of renaming columns back to SWAN's originals.
#' renamed_extract <- tibble::tibble(
#'   participant = c(301L, 302L),
#'   leaks_yes_no = c("(2) Yes", "(2) Yes"),
#'   leak_days = c("(4) Every day", "(1) < once a month"),
#'   leak_amount = c("(3) Large amount", "(1) Drop")
#' )
#'
#' local_variable_map <- tibble::tibble(
#'   visit = 7L,
#'   ever_variable = "leaks_yes_no",
#'   frequency_variable = "leak_days",
#'   amount_variable = "leak_amount",
#'   urge_amount_variable = NA_character_,
#'   amount_scope = "overall",
#'   verification_status = "verified"
#' )
#'
#' renamed_panel <- build_swan_incontinence_panel(
#'   swan_visit_frames = list("7" = renamed_extract),
#'   variable_map = local_variable_map,
#'   participant_id_column = "participant",
#'   verbose = FALSE
#' )
#' renamed_panel$frequency_level
#' #> [1] 4 1
#' attr(renamed_panel, "swan_panel_provenance")$visits_used
#' #> [1] 7
#'
#' @importFrom assertthat assert_that
#' @importFrom dplyr arrange bind_rows mutate select
#' @importFrom readr read_csv
#' @importFrom rlang .data
#' @importFrom stringr str_match
#' @importFrom tibble tibble
#' @family swan incontinence panel
#' @concept data
#' @export
build_swan_incontinence_panel <- function(swan_visit_frames,
                                          variable_map = NULL,
                                          participant_id_column = "SWANID",
                                          verbose = TRUE) {
  assertthat::assert_that(is.logical(verbose))
  assertthat::assert_that(length(verbose) == 1L)
  assertthat::assert_that(is.list(swan_visit_frames))
  assertthat::assert_that(length(swan_visit_frames) > 0L)
  assertthat::assert_that(!is.null(names(swan_visit_frames)))
  assertthat::assert_that(is.character(participant_id_column))
  assertthat::assert_that(length(participant_id_column) == 1L)

  announce_step(
    verbose,
    sprintf("input: %d visit frame(s); id column = %s",
            length(swan_visit_frames), participant_id_column)
  )

  resolved_variable_map <- resolve_variable_map(variable_map)
  announce_step(
    verbose,
    sprintf("crosswalk: %d visit rows, %d marked verified",
            nrow(resolved_variable_map),
            sum(resolved_variable_map$verification_status == "verified"))
  )

  per_visit_panels <- lapply(
    names(swan_visit_frames),
    function(visit_label) {
      extract_one_visit(
        wide_visit_frame = swan_visit_frames[[visit_label]],
        visit_label = visit_label,
        variable_map = resolved_variable_map,
        participant_id_column = participant_id_column,
        verbose = verbose
      )
    }
  )

  swan_incontinence_panel <- dplyr::bind_rows(per_visit_panels)
  swan_incontinence_panel <- dplyr::arrange(
    swan_incontinence_panel, .data$swan_id, .data$visit
  )

  assert_no_duplicate_participant_visits(swan_incontinence_panel)

  attr(swan_incontinence_panel, "swan_panel_provenance") <- list(
    visits_used = sort(unique(swan_incontinence_panel$visit)),
    participant_id_column = participant_id_column,
    variable_map = resolved_variable_map[
      resolved_variable_map$visit %in% swan_incontinence_panel$visit,
    ],
    built_at = Sys.time()
  )

  announce_step(
    verbose,
    sprintf("output: %d rows, %d participants, %d visit(s)",
            nrow(swan_incontinence_panel),
            length(unique(swan_incontinence_panel$swan_id)),
            length(unique(swan_incontinence_panel$visit)))
  )

  swan_incontinence_panel
}


# helpers ---------------------------------------------------------------

#' @noRd
announce_step <- function(verbose, step_text) {
  if (isTRUE(verbose)) {
    message("[swan-panel] ", step_text)
  }
  invisible(NULL)
}

#' @noRd
resolve_variable_map <- function(variable_map) {
  if (is.null(variable_map)) {
    shipped_map_path <- system.file(
      "extdata", "swan", "swan_incontinence_variable_map.csv",
      package = "urpssim"
    )
    assertthat::assert_that(nzchar(shipped_map_path))
    variable_map <- readr::read_csv(
      shipped_map_path,
      na = c("", "NA"),
      show_col_types = FALSE
    )
  }
  assertthat::assert_that(is.data.frame(variable_map))
  required_map_columns <- c(
    "visit", "ever_variable", "frequency_variable", "amount_variable",
    "urge_amount_variable", "amount_scope", "verification_status"
  )
  missing_map_columns <- setdiff(required_map_columns, names(variable_map))
  if (length(missing_map_columns) > 0L) {
    stop(
      "variable_map is missing required column(s): ",
      paste(missing_map_columns, collapse = ", "),
      call. = FALSE
    )
  }
  variable_map$visit <- as.integer(variable_map$visit)
  variable_map
}

#' @noRd
extract_one_visit <- function(wide_visit_frame,
                              visit_label,
                              variable_map,
                              participant_id_column,
                              verbose) {
  assertthat::assert_that(is.data.frame(wide_visit_frame))

  visit_number <- suppressWarnings(as.integer(visit_label))
  if (is.na(visit_number)) {
    stop(
      "swan_visit_frames names must be visit numbers; got '",
      visit_label, "'.",
      call. = FALSE
    )
  }

  if (!participant_id_column %in% names(wide_visit_frame)) {
    stop(
      "Visit ", visit_number, " frame has no '", participant_id_column,
      "' column. SWANID is the sole participant key in this pipeline.",
      call. = FALSE
    )
  }

  map_row <- variable_map[variable_map$visit == visit_number, , drop = FALSE]
  if (nrow(map_row) != 1L) {
    stop(
      "Visit ", visit_number, " has ", nrow(map_row),
      " crosswalk rows; exactly one is required.",
      call. = FALSE
    )
  }
  if (!identical(map_row$verification_status[[1]], "verified")) {
    stop(
      "Visit ", visit_number, " is marked '",
      map_row$verification_status[[1]],
      "' in the crosswalk. Verify its variable names against the SWAN ",
      "data dictionary and set verification_status = 'verified', or ",
      "pass a variable_map that does. Guessed names yield an all-NA ",
      "severity column that never errors.",
      call. = FALSE
    )
  }

  gate_variable <- map_row$ever_variable[[1]]
  frequency_variable <- map_row$frequency_variable[[1]]
  amount_variable <- map_row$amount_variable[[1]]
  urge_amount_variable <- map_row$urge_amount_variable[[1]]
  amount_scope <- map_row$amount_scope[[1]]

  announce_step(
    verbose,
    sprintf("visit %d: gate %s, freq %s, amount %s (scope %s)",
            visit_number, gate_variable, frequency_variable,
            ifelse(is.na(amount_variable), "<none>", amount_variable),
            amount_scope)
  )

  assert_columns_present(
    wide_visit_frame,
    stats::na.omit(c(gate_variable, frequency_variable,
                     amount_variable, urge_amount_variable)),
    visit_number
  )

  visit_panel <- tibble::tibble(
    swan_id = as.integer(wide_visit_frame[[participant_id_column]]),
    visit = visit_number,
    leakage_ever = swan_yes_no_to_logical(
      wide_visit_frame[[gate_variable]]
    ),
    frequency_level = swan_label_to_integer(
      wide_visit_frame[[frequency_variable]]
    ),
    amount_level = pull_optional_level(wide_visit_frame, amount_variable),
    urge_amount_level = pull_optional_level(
      wide_visit_frame, urge_amount_variable
    ),
    amount_scope = amount_scope
  )

  visit_panel <- visit_panel[!is.na(visit_panel$swan_id), , drop = FALSE]

  announce_step(
    verbose,
    sprintf("visit %d: %d wide rows -> %d panel rows",
            visit_number, nrow(wide_visit_frame), nrow(visit_panel))
  )

  visit_panel
}

#' @noRd
pull_optional_level <- function(wide_visit_frame, variable_name) {
  if (is.na(variable_name) || !nzchar(variable_name)) {
    return(rep(NA_integer_, nrow(wide_visit_frame)))
  }
  swan_label_to_integer(wide_visit_frame[[variable_name]])
}

#' @noRd
assert_columns_present <- function(wide_visit_frame,
                                   expected_columns,
                                   visit_number) {
  absent_columns <- setdiff(expected_columns, names(wide_visit_frame))
  if (length(absent_columns) > 0L) {
    stop(
      "Visit ", visit_number, " frame is missing column(s) named in the ",
      "crosswalk: ", paste(absent_columns, collapse = ", "), ".",
      call. = FALSE
    )
  }
  invisible(TRUE)
}

#' @noRd
swan_label_to_integer <- function(labelled_values) {
  character_values <- as.character(labelled_values)
  leading_code <- stringr::str_match(
    character_values, "^\\s*\\(?\\s*(-?[0-9]+)\\s*\\)?"
  )[, 2]
  integer_code <- suppressWarnings(as.integer(leading_code))
  integer_code[!is.na(integer_code) & integer_code < 0L] <- NA_integer_
  integer_code
}

#' @noRd
swan_yes_no_to_logical <- function(labelled_values) {
  integer_code <- swan_label_to_integer(labelled_values)
  unexpected_codes <- setdiff(
    unique(stats::na.omit(integer_code)), c(1L, 2L)
  )
  if (length(unexpected_codes) > 0L) {
    stop(
      "SWAN YESNO item carries unexpected code(s): ",
      paste(sort(unexpected_codes), collapse = ", "),
      ". Expected 1 = No, 2 = Yes.",
      call. = FALSE
    )
  }
  ifelse(is.na(integer_code), NA, integer_code == 2L)
}

#' @noRd
assert_no_duplicate_participant_visits <- function(swan_incontinence_panel) {
  participant_visit_keys <- paste(
    swan_incontinence_panel$swan_id, swan_incontinence_panel$visit
  )
  duplicate_count <- sum(duplicated(participant_visit_keys))
  if (duplicate_count > 0L) {
    stop(
      "Reshape produced ", duplicate_count,
      " duplicate participant-visit rows. A naive join across visit ",
      "frames is the usual cause.",
      call. = FALSE
    )
  }
  invisible(TRUE)
}
