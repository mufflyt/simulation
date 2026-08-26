# CHIA + BORIM URPS capacity evidence: linkage ------------------------------
#
# Links Massachusetts CHIA case-mix physician identifiers to NPI through the
# BORIM extract, then restricts to the board-certified URPS roster and flags
# pelvic-floor diagnoses/procedures. The physician-year summary, CADR intensity
# read, evidence bundle, and resolution gate live in
# R/calibration-chia_capacity_evidence.R.
#
# Estimands stay separate: CHIA delivered workload is never relabeled as latent
# FTE capacity; adequacy is not identified here (see empirical_capacity_status()
# and the Lizeth/Rabice access inverse). External file paths come from arguments
# or environment variables only -- no machine-specific paths in library code.

.capacity_comma <- function(x) {
  base::format(
    x,
    big.mark = ",",
    trim = TRUE,
    scientific = FALSE
  )
}

.normalize_npi <- function(x) {
  value <- stringr::str_extract(
    base::as.character(x),
    "[0-9]{10}"
  )
  dplyr::if_else(
    stringr::str_detect(value, "^[0-9]{10}$"),
    value,
    NA_character_
  )
}

.normalize_identifier <- function(x) {
  value <- base::toupper(base::trimws(base::as.character(x)))
  value <- stringr::str_replace_all(value, "[^A-Z0-9]", "")
  value[value %in% c("", "NA", "NAN", "NULL", "NONE")] <- NA_character_
  value
}

#' Locate the Massachusetts BORIM-to-NPI source
#'
#' @param path Optional explicit path.
#'
#' @return Character scalar.
#' @concept calibration
#' @family CHIA capacity evidence
#' @export
find_ma_borim_npi_source <- function(path = NULL) {
  if (!base::is.null(path)) {
    if (!base::file.exists(path)) {
      base::stop("BORIM file not found: ", path, call. = FALSE)
    }
    return(base::normalizePath(path, mustWork = TRUE))
  }

  # Environment variable first, then a repo-relative staging location. No
  # machine-specific absolute paths in library code -- set MA_BORIM_CSV (or pass
  # `path`) to point at a BORIM extract elsewhere.
  env_path <- base::Sys.getenv("MA_BORIM_CSV", unset = "")
  candidate_paths <- c(
    env_path,
    base::file.path(
      "data-raw",
      "ma_casemix",
      "BORIM_STDREL_NPI_straight_from_CD.csv"
    )
  )
  candidate_paths <- candidate_paths[base::nzchar(candidate_paths)]
  found_paths <- candidate_paths[base::file.exists(candidate_paths)]

  if (base::length(found_paths) == 0L) {
    base::stop(
      base::paste(
        "BORIM-to-NPI source not found.",
        "Set MA_BORIM_CSV or pass `path`."
      ),
      call. = FALSE
    )
  }

  selected_path <- found_paths[[1L]]
  base::message("BORIM source: ", selected_path)
  base::normalizePath(selected_path, mustWork = TRUE)
}

#' Build a BORIM identifier-to-NPI bridge
#'
#' The Cadish BORIM extract contains NPI directly. This function also keeps
#' other identifier-like BORIM columns so CHIA can link even when a case-mix
#' vintage carries an internal BORIM identifier rather than the literal license
#' number.
#'
#' Ambiguous identifier values mapping to more than one NPI are dropped.
#'
#' @param path BORIM CSV path.
#'
#' @return Tibble with `key_value`, `key_source`, and `npi`.
#' @concept calibration
#' @family CHIA capacity evidence
#' @export
read_ma_borim_npi_bridge <- function(
    path = find_ma_borim_npi_source()) {
  base::message("Reading Massachusetts BORIM identifier bridge.")

  borim_records <- readr::read_csv(
    path,
    col_types = readr::cols(.default = readr::col_character()),
    show_col_types = FALSE,
    progress = FALSE
  )

  required_names <- c("NPI")
  missing_names <- base::setdiff(
    required_names,
    base::names(borim_records)
  )
  if (base::length(missing_names) > 0L) {
    base::stop(
      "BORIM file is missing NPI.",
      call. = FALSE
    )
  }

  column_names <- base::names(borim_records)
  lower_names <- base::tolower(column_names)

  identifier_like <- stringr::str_detect(
    lower_names,
    "npi|license|licnum|lic_no|licno|borim|provider.*id|phys.*id"
  )
  excluded_like <- stringr::str_detect(
    lower_names,
    "name|status|date|renew|expire|special|board|gender|degree|class"
  )
  key_columns <- column_names[identifier_like & !excluded_like]
  key_columns <- base::unique(c("NPI", key_columns))

  if ("license" %in% column_names) {
    key_columns <- base::unique(c(key_columns, "license"))
  }

  base::message(
    "BORIM identifier columns: ",
    base::paste(key_columns, collapse = ", ")
  )

  borim_keys <- borim_records |>
    dplyr::mutate(
      npi = .normalize_npi(.data$NPI)
    ) |>
    dplyr::select(
      npi,
      dplyr::all_of(base::setdiff(key_columns, "NPI")),
      NPI
    ) |>
    tidyr::pivot_longer(
      cols = -npi,
      names_to = "key_source",
      values_to = "key_raw"
    ) |>
    dplyr::mutate(
      key_value = .normalize_identifier(key_raw)
    ) |>
    dplyr::filter(
      !base::is.na(npi),
      !base::is.na(key_value),
      stringr::str_length(key_value) >= 4L
    ) |>
    dplyr::distinct(key_value, key_source, npi)

  ambiguity <- borim_keys |>
    dplyr::group_by(key_value) |>
    dplyr::summarise(
      n_npi = dplyr::n_distinct(npi),
      .groups = "drop"
    ) |>
    dplyr::filter(n_npi > 1L)

  if (base::nrow(ambiguity) > 0L) {
    base::message(
      "Dropping ",
      .capacity_comma(base::nrow(ambiguity)),
      " ambiguous BORIM identifier value(s)."
    )
  }

  bridge <- borim_keys |>
    dplyr::anti_join(ambiguity, by = "key_value") |>
    dplyr::distinct(key_value, npi, .keep_all = TRUE)

  base::message(
    "BORIM bridge: ",
    .capacity_comma(dplyr::n_distinct(bridge$key_value)),
    " unique identifier values -> ",
    .capacity_comma(dplyr::n_distinct(bridge$npi)),
    " NPIs."
  )

  bridge
}

.chia_provider_score <- function(column_name) {
  lower_name <- base::tolower(column_name)
  score <- 0L

  if (stringr::str_detect(lower_name, "npi")) {
    score <- score + 12L
  }
  if (stringr::str_detect(
      lower_name,
      "license|licnum|lic_no|licno|borim")) {
    score <- score + 10L
  }
  if (stringr::str_detect(
      lower_name,
      "phys|phy[0-9_]*$|provider|doctor|surgeon")) {
    score <- score + 5L
  }
  if (stringr::str_detect(
      lower_name,
      "attend|operat|principal|primary|servicing")) {
    score <- score + 3L
  }
  if (stringr::str_detect(
      lower_name,
      "name|special|type|status|state|city|zip|date|year")) {
    score <- score - 12L
  }

  score
}

#' Detect likely physician-identifier fields in a CHIA case-mix file
#'
#' @param column_names Character vector of column names.
#'
#' @return Tibble ranked by detection score.
#' @concept calibration
#' @family CHIA capacity evidence
#' @export
identify_chia_provider_columns <- function(column_names) {
  candidates <- tibble::tibble(
    column = column_names,
    score = base::vapply(
      column_names,
      .chia_provider_score,
      integer(1)
    )
  ) |>
    dplyr::filter(score >= 5L) |>
    dplyr::arrange(dplyr::desc(score), column)

  if (base::nrow(candidates) == 0L) {
    base::stop(
      base::paste(
        "No likely CHIA physician identifier column was detected.",
        "Pass `provider_columns` explicitly."
      ),
      call. = FALSE
    )
  }

  candidates
}

.detect_one_column <- function(
    column_names,
    patterns,
    label,
    required = FALSE) {
  lower_names <- base::tolower(column_names)
  matched <- rep(FALSE, base::length(column_names))

  for (pattern in patterns) {
    matched <- matched | stringr::str_detect(lower_names, pattern)
  }

  candidates <- column_names[matched]
  if (base::length(candidates) == 0L) {
    if (base::isTRUE(required)) {
      base::stop(
        "Could not detect CHIA ",
        label,
        " column.",
        call. = FALSE
      )
    }
    return(NULL)
  }

  candidates[[1L]]
}

.detect_chia_diagnosis_columns <- function(column_names) {
  lower_names <- base::tolower(column_names)
  column_names[
    stringr::str_detect(
      lower_names,
      "(^|_)(dx|diag|diagnosis|icd)[0-9_]*"
    ) &
      !stringr::str_detect(
        lower_names,
        "proc|procedure|px"
      )
  ]
}

.detect_chia_procedure_columns <- function(column_names) {
  lower_names <- base::tolower(column_names)
  column_names[
    stringr::str_detect(
      lower_names,
      "(^|_)(proc|procedure|px)[0-9_]*"
    )
  ]
}

#' Read only the CHIA case-mix fields needed for workload linkage
#'
#' @param path One CHIA CSV or CSV.GZ file.
#' @param provider_columns Optional physician identifier columns.
#' @param year_column Optional year column.
#' @param encounter_id_column Optional discharge/encounter key.
#' @param diagnosis_columns Optional diagnosis fields.
#' @param procedure_columns Optional procedure fields.
#'
#' @return List containing selected records and resolved schema.
#' @concept calibration
#' @family CHIA capacity evidence
#' @export
read_chia_capacity_fields <- function(
    path,
    provider_columns = NULL,
    year_column = NULL,
    encounter_id_column = NULL,
    diagnosis_columns = NULL,
    procedure_columns = NULL) {
  if (!base::file.exists(path)) {
    base::stop("CHIA file not found: ", path, call. = FALSE)
  }

  base::message("Inspecting CHIA schema: ", path)
  header <- readr::read_csv(
    path,
    n_max = 0,
    show_col_types = FALSE,
    progress = FALSE
  )
  column_names <- base::names(header)

  if (base::is.null(provider_columns)) {
    provider_candidates <- identify_chia_provider_columns(column_names)
    provider_columns <- provider_candidates$column
  }
  provider_columns <- base::intersect(
    provider_columns,
    column_names
  )
  if (base::length(provider_columns) == 0L) {
    base::stop(
      "None of the requested CHIA provider columns exists.",
      call. = FALSE
    )
  }

  if (base::is.null(year_column)) {
    year_column <- .detect_one_column(
      column_names,
      c(
        "^year$",
        "disch.*year",
        "discharge.*year",
        "admit.*year",
        "service.*year"
      ),
      label = "year"
    )
  }

  if (base::is.null(encounter_id_column)) {
    encounter_id_column <- .detect_one_column(
      column_names,
      c(
        "encounter.*id",
        "discharge.*id",
        "case.*id",
        "record.*id",
        "visit.*id",
        "discharge.*key"
      ),
      label = "encounter id"
    )
  }

  if (base::is.null(diagnosis_columns)) {
    diagnosis_columns <- .detect_chia_diagnosis_columns(
      column_names
    )
  }
  if (base::is.null(procedure_columns)) {
    procedure_columns <- .detect_chia_procedure_columns(
      column_names
    )
  }

  selected_names <- base::unique(c(
    provider_columns,
    year_column,
    encounter_id_column,
    diagnosis_columns,
    procedure_columns
  ))
  selected_names <- selected_names[
    !base::is.na(selected_names) & base::nzchar(selected_names)
  ]

  base::message(
    "CHIA physician fields: ",
    base::paste(provider_columns, collapse = ", ")
  )
  base::message(
    "CHIA diagnosis fields: ",
    .capacity_comma(base::length(diagnosis_columns))
  )
  base::message(
    "CHIA procedure fields: ",
    .capacity_comma(base::length(procedure_columns))
  )

  case_records <- readr::read_csv(
    path,
    col_select = dplyr::all_of(selected_names),
    col_types = readr::cols(.default = readr::col_character()),
    show_col_types = FALSE,
    progress = FALSE
  ) |>
    dplyr::mutate(
      .source_file = base::basename(path),
      .source_row = dplyr::row_number()
    )

  schema <- list(
    provider_columns = provider_columns,
    year_column = year_column,
    encounter_id_column = encounter_id_column,
    diagnosis_columns = diagnosis_columns,
    procedure_columns = procedure_columns
  )

  list(
    records = case_records,
    schema = schema
  )
}

.icd_clean <- function(x) {
  value <- base::toupper(base::as.character(x))
  stringr::str_replace_all(value, "[^A-Z0-9]", "")
}

.icd_pop <- function(x) {
  value <- .icd_clean(x)
  stringr::str_detect(value, "^(618|N81)")
}

.icd_ui <- function(x) {
  value <- .icd_clean(x)
  stringr::str_detect(
    value,
    "^(6256|7883|N393|N394|N3946)"
  )
}

.icd9_hysterectomy <- function(x) {
  value <- .icd_clean(x)
  stringr::str_detect(value, "^68[3-9]")
}

.row_any <- function(records, columns, predicate) {
  if (base::length(columns) == 0L) {
    return(base::rep(FALSE, base::nrow(records)))
  }

  flags <- base::lapply(
    columns,
    function(column_name) {
      predicate(records[[column_name]])
    }
  )
  base::Reduce(`|`, flags)
}

#' Link CHIA case-mix encounters to BORIM NPI and the URPS roster
#'
#' @param chia_extract List from [read_chia_capacity_fields()].
#' @param borim_bridge Tibble from [read_ma_borim_npi_bridge()].
#' @param urps_roster Board-certified URPS roster containing `npi`.
#'
#' @return Encounter-by-URPS-NPI attribution table.
#' @concept calibration
#' @family CHIA capacity evidence
#' @export
link_chia_to_urps <- function(
    chia_extract,
    borim_bridge = read_ma_borim_npi_bridge(),
    urps_roster = load_urps_roster()) {
  case_records <- chia_extract$records
  schema <- chia_extract$schema

  if (!"npi" %in% base::names(urps_roster)) {
    base::stop(
      "`urps_roster` must contain `npi`.",
      call. = FALSE
    )
  }

  roster_npis <- urps_roster |>
    dplyr::transmute(
      npi = .normalize_npi(npi)
    ) |>
    dplyr::filter(!base::is.na(npi)) |>
    dplyr::distinct(npi)

  provider_long <- case_records |>
    tidyr::pivot_longer(
      cols = dplyr::all_of(schema$provider_columns),
      names_to = "physician_role",
      values_to = "physician_identifier_raw"
    ) |>
    dplyr::mutate(
      physician_key = .normalize_identifier(
        physician_identifier_raw
      )
    ) |>
    dplyr::filter(!base::is.na(physician_key))

  direct_npi <- .normalize_npi(
    provider_long$physician_identifier_raw
  )
  provider_long <- provider_long |>
    dplyr::mutate(
      direct_npi = direct_npi
    ) |>
    dplyr::left_join(
      borim_bridge |>
        dplyr::select(key_value, borim_npi = npi),
      by = c("physician_key" = "key_value")
    ) |>
    dplyr::mutate(
      npi = dplyr::coalesce(direct_npi, borim_npi),
      npi_link_method = dplyr::case_when(
        !base::is.na(direct_npi) ~ "chia_npi_direct",
        !base::is.na(borim_npi) ~ "borim_identifier_to_npi",
        TRUE ~ "unmatched"
      )
    )

  n_provider_values <- base::nrow(provider_long)
  n_linked_values <- base::sum(
    !base::is.na(provider_long$npi)
  )
  base::message(
    "CHIA physician identifiers linked to NPI: ",
    .capacity_comma(n_linked_values),
    "/",
    .capacity_comma(n_provider_values),
    " (",
    base::sprintf(
      "%.1f%%",
      100 * n_linked_values / base::max(1, n_provider_values)
    ),
    ")."
  )

  diagnosis_columns <- schema$diagnosis_columns
  procedure_columns <- schema$procedure_columns

  pop_flag <- .row_any(
    case_records,
    diagnosis_columns,
    .icd_pop
  )
  ui_flag <- .row_any(
    case_records,
    diagnosis_columns,
    .icd_ui
  )
  hysterectomy_flag <- .row_any(
    case_records,
    procedure_columns,
    .icd9_hysterectomy
  )

  flag_table <- case_records |>
    dplyr::transmute(
      .source_file,
      .source_row,
      pop_diagnosis = pop_flag,
      ui_diagnosis = ui_flag,
      hysterectomy_procedure = hysterectomy_flag
    )

  linked_records <- provider_long |>
    dplyr::left_join(
      flag_table,
      by = c(".source_file", ".source_row")
    ) |>
    dplyr::filter(!base::is.na(npi)) |>
    dplyr::semi_join(roster_npis, by = "npi")

  if (!base::is.null(schema$year_column)) {
    linked_records <- linked_records |>
      dplyr::mutate(
        year = readr::parse_integer(
          base::as.character(
            .data[[schema$year_column]]
          )
        )
      )
  } else {
    linked_records <- linked_records |>
      dplyr::mutate(year = NA_integer_)
  }

  if (!base::is.null(schema$encounter_id_column)) {
    linked_records <- linked_records |>
      dplyr::mutate(
        encounter_key = base::paste(
          .source_file,
          .data[[schema$encounter_id_column]],
          sep = "::"
        )
      )
  } else {
    linked_records <- linked_records |>
      dplyr::mutate(
        encounter_key = base::paste(
          .source_file,
          .source_row,
          sep = "::"
        )
      )
  }

  linked_records <- linked_records |>
    dplyr::mutate(
      urps_diagnosis = pop_diagnosis | ui_diagnosis,
      pop_hysterectomy = pop_diagnosis &
        hysterectomy_procedure
    ) |>
    dplyr::distinct(encounter_key, npi, .keep_all = TRUE)

  base::message(
    "CHIA encounters attributed to board-certified URPS physicians: ",
    .capacity_comma(
      dplyr::n_distinct(linked_records$encounter_key)
    ),
    "."
  )
  base::message(
    "Unique URPS NPIs represented: ",
    .capacity_comma(dplyr::n_distinct(linked_records$npi)),
    "."
  )

  linked_records
}
