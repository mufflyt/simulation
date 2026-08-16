# Lizeth fielded URPS access anchor ----
#
# PURPOSE
#
# Lizeth is a national mystery-caller study of access to URPS care.
# It directly measures realized access:
#
#   * whether the office was reached;
#   * whether an appointment was obtainable;
#   * waiting time when an appointment was obtainable;
#   * Medicaid acceptance;
#   * clinical scenario;
#   * physician and NPI.
#
# These are fielded URPS observations. They are not a direct measurement of
# latent productive capacity. Therefore this module does NOT invert wait time
# into a required-FTE or adequacy ratio without a separately validated
# wait-time-to-capacity response function.
#
# The output is an empirical base-year ACCESS anchor that can:
#
#   1. replace borrowed evidence when describing current access;
#   2. test whether an assumed adequacy calibration is plausible;
#   3. serve as the calibration target for a future response model;
#   4. propagate sampling uncertainty by physician-level bootstrap.
#
# It must not silently change capacity_status()$resolved to TRUE.
#
# Base-R note: the package declares neither scales nor lubridate. To keep the
# dependency surface unchanged, thousands-formatting and date parsing are done
# with base R. `.lizeth_comma()` mirrors scales::comma() for message text, and
# `.lizeth_as_date()` mirrors lubridate::as_date() -- passing an explicit format
# makes base::as.Date() return NA on unparseable input rather than erroring,
# which is the forgiving behaviour the parser relies on.

.lizeth_comma <- function(x) {
  base::format(x, big.mark = ",", trim = TRUE, scientific = FALSE)
}

.lizeth_as_date <- function(x) {
  base::as.Date(base::as.character(x), format = "%Y-%m-%d")
}

# Federal-holiday-adjusted business days between call and appointment, matching
# the Lizeth study's PRIMARY wait outcome exactly: mc_business_days(..., "federal")
# in mystery_common.R delegates to mysterycall_count_business_days() over the US
# federal calendar (Mon-Fri minus the 11 federal holidays). A URPS mystery-caller
# wait is quoted in business days, not calendar days -- a raw date subtraction is
# the "two different clocks" error the access layer (URPS_ACCESS_TIME_UNIT) warns
# against. Same-day appointments are 0; an appointment before the call is NA.
.lizeth_business_days <- function(call_date, appointment_date) {
  if (!base::requireNamespace("mysterycall", quietly = TRUE)) {
    base::stop(
      "Package 'mysterycall' is required to count federal business days for ",
      "Lizeth wait times (the study's primary outcome unit). Install the commit ",
      "the study uses: remotes::install_github('mufflyt/mysterycall@098bb834'). ",
      "It is a Suggests, not pinned in Remotes, because its GLMM dependency ",
      "graph conflicts with this package's CI-pinned lme4/blme.",
      call. = FALSE
    )
  }
  base::as.numeric(mysterycall::mysterycall_count_business_days(
    base::as.Date(call_date),
    base::as.Date(appointment_date),
    mysterycall::mysterycall_us_federal_calendar()
  ))
}

#' Find the most recent Lizeth labeled REDCap export
#'
#' @param lizeth_dir Local path to the Lizeth repository.
#'
#' @return Character scalar giving the labeled REDCap CSV path.
#'
#' @concept calibration
#' @family Lizeth access anchor
#' @export
find_lizeth_redcap <- function(lizeth_dir = "../lizeth") {
  base::message("Searching for Lizeth labeled REDCap exports.")
  base::message("Lizeth repository: ", lizeth_dir)
  if (!base::dir.exists(lizeth_dir)) {
    base::stop(
      "Lizeth repository not found: ",
      lizeth_dir,
      call. = FALSE
    )
  }
  candidates <- base::list.files(
    path = lizeth_dir,
    pattern = "^ICVsPOPVsSUI_DATA_LABELS_.*\\.csv$",
    full.names = TRUE
  )
  if (base::length(candidates) == 0L) {
    base::stop(
      "No labeled Lizeth REDCap export found in ",
      lizeth_dir,
      call. = FALSE
    )
  }
  candidate_info <- base::file.info(candidates)
  selected_path <- candidates[
    base::which.max(candidate_info$mtime)
  ]
  base::message(
    "Found ",
    .lizeth_comma(base::length(candidates)),
    " labeled REDCap export(s)."
  )
  base::message("Using: ", selected_path)
  selected_path
}

#' Parse the REDCap physician information field
#'
#' @param lizeth_records Labeled REDCap records.
#'
#' @return Tibble with physician, insurance, scenario, state and NPI.
#'
#' @concept calibration
#' @family Lizeth access anchor
#' @importFrom rlang .data
#' @export
parse_lizeth_physician_information <- function(lizeth_records) {
  base::message("Parsing Lizeth physician-information field.")
  if (!base::is.data.frame(lizeth_records)) {
    base::stop(
      "`lizeth_records` must be a data frame.",
      call. = FALSE
    )
  }
  if (base::ncol(lizeth_records) < 2L) {
    base::stop(
      "Lizeth REDCap export has fewer than two columns.",
      call. = FALSE
    )
  }
  physician_column <- base::names(lizeth_records)[[2]]
  base::message(
    "Physician-information column: ",
    physician_column
  )
  parsed_records <- lizeth_records |>
    dplyr::rename(
      physician_information = dplyr::all_of(physician_column)
    ) |>
    tidyr::separate_wider_delim(
      cols = physician_information,
      delim = ",",
      names = c(
        "physician_code",
        "physician_name",
        "insurance",
        "clinical_scenario",
        "phone",
        "state",
        "npi",
        "embedded_record_id"
      ),
      too_few = "align_start",
      too_many = "merge"
    ) |>
    dplyr::mutate(
      dplyr::across(
        c(
          physician_code,
          physician_name,
          insurance,
          clinical_scenario,
          phone,
          state,
          npi,
          embedded_record_id
        ),
        stringr::str_trim
      ),
      physician_code = readr::parse_integer(
        physician_code
      ),
      npi = stringr::str_extract(
        npi,
        "\\d{10}"
      ),
      embedded_record_id = stringr::str_extract(
        embedded_record_id,
        "\\d+"
      ),
      embedded_record_id = readr::parse_integer(
        embedded_record_id
      )
    )
  base::message(
    "Parsed ",
    .lizeth_comma(base::nrow(parsed_records)),
    " call records."
  )
  base::message(
    "Unique physicians: ",
    .lizeth_comma(
      dplyr::n_distinct(
        parsed_records$physician_name,
        na.rm = TRUE
      )
    )
  )
  base::message(
    "Unique NPIs: ",
    .lizeth_comma(
      dplyr::n_distinct(
        parsed_records$npi,
        na.rm = TRUE
      )
    )
  )
  parsed_records
}

#' Build call-level Lizeth access outcomes
#'
#' @param lizeth_records Parsed labeled REDCap records.
#'
#' @return Tibble with standardized access outcomes.
#'
#' @concept calibration
#' @family Lizeth access anchor
#' @importFrom rlang .data
#' @export
prepare_lizeth_access <- function(lizeth_records) {
  base::message("Constructing Lizeth call-level access outcomes.")
  required_names <- c(
    "physician_name",
    "insurance",
    "clinical_scenario",
    "state",
    "npi"
  )
  missing_names <- base::setdiff(
    required_names,
    base::names(lizeth_records)
  )
  if (base::length(missing_names) > 0L) {
    base::stop(
      "Missing parsed Lizeth variable(s): ",
      base::paste(missing_names, collapse = ", "),
      call. = FALSE
    )
  }
  redcap_names <- base::names(lizeth_records)
  contact_1_name <- redcap_names[
    stringr::str_detect(
      redcap_names,
      stringr::regex(
        "Able to contact office on first call",
        ignore_case = TRUE
      )
    )
  ][1]
  contact_2_name <- redcap_names[
    stringr::str_detect(
      redcap_names,
      stringr::regex(
        "Able to contact office on second",
        ignore_case = TRUE
      )
    )
  ][1]
  call_date_name <- redcap_names[
    stringr::str_detect(
      redcap_names,
      stringr::regex(
        "Date and Time of FIRST",
        ignore_case = TRUE
      )
    )
  ][1]
  appointment_name <- redcap_names[
    stringr::str_detect(
      redcap_names,
      stringr::regex(
        "^Appointment Date$",
        ignore_case = TRUE
      )
    )
  ][1]
  exclusion_name <- redcap_names[
    stringr::str_detect(
      redcap_names,
      stringr::regex(
        "^Reason for exclusions$",
        ignore_case = TRUE
      )
    )
  ][1]
  medicaid_name <- redcap_names[
    stringr::str_detect(
      redcap_names,
      stringr::regex(
        "accept Medicaid",
        ignore_case = TRUE
      )
    )
  ][1]
  expected_names <- c(
    contact_1_name,
    contact_2_name,
    call_date_name,
    appointment_name,
    exclusion_name,
    medicaid_name
  )
  if (base::any(base::is.na(expected_names))) {
    base::stop(
      base::paste(
        "Could not identify all required REDCap columns.",
        "Inspect names(lizeth_records)."
      ),
      call. = FALSE
    )
  }
  base::message("Identified REDCap access fields.")
  access_records <- lizeth_records |>
    dplyr::mutate(
      contacted_first = base::as.character(
        .data[[contact_1_name]]
      ),
      contacted_second = base::as.character(
        .data[[contact_2_name]]
      ),
      call_date_text = base::as.character(
        .data[[call_date_name]]
      ),
      appointment_date_text = base::as.character(
        .data[[appointment_name]]
      ),
      exclusion_reason = base::as.character(
        .data[[exclusion_name]]
      ),
      medicaid_status_text = base::as.character(
        .data[[medicaid_name]]
      )
    ) |>
    dplyr::mutate(
      call_date = .lizeth_as_date(call_date_text),
      appointment_date = .lizeth_as_date(
        appointment_date_text
      ),
      reached_office = dplyr::case_when(
        stringr::str_detect(
          contacted_first,
          stringr::regex("^yes", ignore_case = TRUE)
        ) ~ TRUE,
        stringr::str_detect(
          contacted_second,
          stringr::regex("^yes", ignore_case = TRUE)
        ) ~ TRUE,
        TRUE ~ FALSE
      ),
      excluded = !base::is.na(exclusion_reason) &
        stringr::str_trim(exclusion_reason) != "" &
        !stringr::str_detect(
          exclusion_reason,
          stringr::regex(
            "^NA$|^none$",
            ignore_case = TRUE
          )
        ),
      appointment_obtained = !base::is.na(
        appointment_date
      ),
      wait_business_days = .lizeth_business_days(
        call_date,
        appointment_date
      ),
      call_year = base::as.integer(
        base::format(call_date, "%Y")
      )
    )
  base::message(
    "Constructed access outcomes for ",
    .lizeth_comma(base::nrow(access_records)),
    " calls."
  )
  base::message(
    "Appointments observed: ",
    .lizeth_comma(
      base::sum(
        access_records$appointment_obtained,
        na.rm = TRUE
      )
    )
  )
  access_records
}

#' Estimate the fielded Lizeth URPS access anchor
#'
#' This estimates realized national URPS appointment access. It does not
#' transform wait time into a supply/demand adequacy ratio.
#'
#' @param access_records Call-level records from prepare_lizeth_access().
#' @param exclude_ineligible Exclude records carrying an exclusion reason.
#'
#' @return List containing the empirical anchor, strata, and summary sentence.
#'
#' @concept calibration
#' @family Lizeth access anchor
#' @export
estimate_lizeth_access_anchor <- function(
    access_records,
    exclude_ineligible = TRUE) {
  base::message("Estimating fielded Lizeth URPS access anchor.")
  analytic_records <- access_records
  if (base::isTRUE(exclude_ineligible)) {
    base::message("Removing calls with a recorded exclusion.")
    analytic_records <- analytic_records |>
      dplyr::filter(!excluded)
  }
  base::message(
    "Analytic calls: ",
    .lizeth_comma(base::nrow(analytic_records))
  )
  overall_anchor <- analytic_records |>
    dplyr::summarise(
      n_calls = dplyr::n(),
      n_physicians = dplyr::n_distinct(
        npi,
        na.rm = TRUE
      ),
      reached_n = base::sum(
        reached_office,
        na.rm = TRUE
      ),
      reached_pct = base::mean(
        reached_office,
        na.rm = TRUE
      ),
      appointment_n = base::sum(
        appointment_obtained,
        na.rm = TRUE
      ),
      appointment_pct = base::mean(
        appointment_obtained,
        na.rm = TRUE
      ),
      wait_mean = base::mean(
        wait_business_days,
        na.rm = TRUE
      ),
      wait_sd = stats::sd(
        wait_business_days,
        na.rm = TRUE
      ),
      wait_median = stats::median(
        wait_business_days,
        na.rm = TRUE
      ),
      wait_p25 = stats::quantile(
        wait_business_days,
        probs = 0.25,
        na.rm = TRUE,
        names = FALSE
      ),
      wait_p75 = stats::quantile(
        wait_business_days,
        probs = 0.75,
        na.rm = TRUE,
        names = FALSE
      )
    )
  base::message("Computing insurance-specific access estimates.")
  insurance_anchor <- analytic_records |>
    dplyr::group_by(insurance) |>
    dplyr::summarise(
      n_calls = dplyr::n(),
      n_physicians = dplyr::n_distinct(
        npi,
        na.rm = TRUE
      ),
      appointment_n = base::sum(
        appointment_obtained,
        na.rm = TRUE
      ),
      appointment_pct = base::mean(
        appointment_obtained,
        na.rm = TRUE
      ),
      wait_mean = base::mean(
        wait_business_days,
        na.rm = TRUE
      ),
      wait_sd = stats::sd(
        wait_business_days,
        na.rm = TRUE
      ),
      wait_median = stats::median(
        wait_business_days,
        na.rm = TRUE
      ),
      wait_p25 = stats::quantile(
        wait_business_days,
        probs = 0.25,
        na.rm = TRUE,
        names = FALSE
      ),
      wait_p75 = stats::quantile(
        wait_business_days,
        probs = 0.75,
        na.rm = TRUE,
        names = FALSE
      ),
      .groups = "drop"
    )
  base::message("Computing clinical-scenario estimates.")
  scenario_anchor <- analytic_records |>
    dplyr::group_by(clinical_scenario) |>
    dplyr::summarise(
      n_calls = dplyr::n(),
      n_physicians = dplyr::n_distinct(
        npi,
        na.rm = TRUE
      ),
      appointment_n = base::sum(
        appointment_obtained,
        na.rm = TRUE
      ),
      appointment_pct = base::mean(
        appointment_obtained,
        na.rm = TRUE
      ),
      wait_median = stats::median(
        wait_business_days,
        na.rm = TRUE
      ),
      wait_p25 = stats::quantile(
        wait_business_days,
        probs = 0.25,
        na.rm = TRUE,
        names = FALSE
      ),
      wait_p75 = stats::quantile(
        wait_business_days,
        probs = 0.75,
        na.rm = TRUE,
        names = FALSE
      ),
      .groups = "drop"
    )
  insurance_counts <- analytic_records |>
    dplyr::filter(
      !base::is.na(insurance),
      !base::is.na(appointment_obtained)
    ) |>
    dplyr::count(
      insurance,
      appointment_obtained,
      name = "n"
    ) |>
    tidyr::pivot_wider(
      names_from = appointment_obtained,
      values_from = n,
      values_fill = 0
    )
  p_value <- NA_real_
  if (base::nrow(insurance_counts) == 2L) {
    success_name <- "TRUE"
    failure_name <- "FALSE"
    if (!success_name %in% base::names(insurance_counts)) {
      insurance_counts[[success_name]] <- 0L
    }
    if (!failure_name %in% base::names(insurance_counts)) {
      insurance_counts[[failure_name]] <- 0L
    }
    comparison <- stats::prop.test(
      x = insurance_counts[[success_name]],
      n = insurance_counts[[success_name]] +
        insurance_counts[[failure_name]]
    )
    p_value <- comparison$p.value
  }
  years <- analytic_records |>
    dplyr::filter(!base::is.na(call_year)) |>
    dplyr::pull(call_year) |>
    base::unique() |>
    base::sort()
  year_text <- if (base::length(years) == 1L) {
    base::as.character(years)
  } else {
    base::paste0(
      base::min(years),
      "-",
      base::max(years)
    )
  }
  direction_text <- "could not be compared"
  if (base::nrow(insurance_anchor) == 2L) {
    ordered_insurance <- insurance_anchor |>
      dplyr::arrange(dplyr::desc(appointment_pct))
    direction_text <- base::paste0(
      ordered_insurance$insurance[[1]],
      " had higher appointment obtainment than ",
      ordered_insurance$insurance[[2]]
    )
  }
  p_text <- if (base::is.finite(p_value)) {
    if (p_value < 0.001) {
      "<0.001"
    } else {
      base::formatC(
        p_value,
        format = "f",
        digits = 3
      )
    }
  } else {
    "not estimable"
  }
  summary_sentence <- base::sprintf(
    base::paste(
      "In the %s fielded URPS mystery-caller study, %s of %s eligible calls",
      "obtained an appointment; median wait was %.1f business days",
      "(p25 %.1f, p75 %.1f). By insurance, %s (p=%s)."
    ),
    year_text,
    .lizeth_comma(overall_anchor$appointment_n),
    .lizeth_comma(overall_anchor$n_calls),
    overall_anchor$wait_median,
    overall_anchor$wait_p25,
    overall_anchor$wait_p75,
    direction_text,
    p_text
  )
  base::message(summary_sentence)
  anchor <- base::list(
    anchor_type = "fielded_urps_access",
    identifies_capacity_adequacy = FALSE,
    wait_time_unit = "business_days",
    calibration_status =
      "measured_input_unvalidated_response",
    overall = overall_anchor,
    by_insurance = insurance_anchor,
    by_scenario = scenario_anchor,
    p_value_insurance = p_value,
    summary_sentence = summary_sentence,
    source = "Lizeth national URPS mystery-caller REDCap study",
    limitation = base::paste(
      "Observed access identifies realized congestion but not the",
      "supply/demand adequacy ratio without a validated response",
      "function linking effective URPS capacity to appointment access."
    )
  )
  base::message("Lizeth fielded access anchor complete.")
  anchor
}

#' Create simulation adequacy evidence from the Lizeth anchor
#'
#' @param lizeth_anchor Object from estimate_lizeth_access_anchor().
#'
#' @return A urps_adequacy_evidence table.
#'
#' @concept calibration
#' @family Lizeth access anchor
#' @export
lizeth_adequacy_evidence <- function(lizeth_anchor) {
  base::message(
    "Registering Lizeth as empirical URPS adequacy evidence."
  )
  anchor <- lizeth_anchor$overall
  observed_text <- base::sprintf(
    base::paste(
      "%s/%s eligible mystery calls obtained an appointment;",
      "median wait %.1f business days (p25 %.1f, p75 %.1f)."
    ),
    .lizeth_comma(anchor$appointment_n),
    .lizeth_comma(anchor$n_calls),
    anchor$wait_median,
    anchor$wait_p25,
    anchor$wait_p75
  )
  evidence_table <- adequacy_evidence_table(
    evidence = "Lizeth national URPS mystery-caller access study",
    model_implication = base::paste(
      "A base-year adequacy calibration should reproduce the",
      "observed appointment-obtainment and wait-time distribution."
    ),
    observed = observed_text,
    interpretation = "not_identifiable_from_this_evidence",
    citation = "Lizeth fielded URPS mystery-caller study",
    evidence_type = "empirical_observation"
  )
  base::message(
    "Lizeth evidence registered without assigning an adequacy level."
  )
  evidence_table
}

#' Report capacity status including the Lizeth access anchor
#'
#' @param lizeth_anchor Optional Lizeth access-anchor object.
#'
#' @return List describing base-year calibration status.
#'
#' @concept calibration
#' @family Lizeth access anchor
#' @export
capacity_status_with_lizeth <- function(lizeth_anchor = NULL) {
  base::message("Checking capacity-anchor status.")
  original_status <- capacity_status()
  if (base::is.null(lizeth_anchor)) {
    base::message("No Lizeth anchor supplied.")
    return(original_status)
  }
  updated_status <- original_status
  updated_status$resolved <- FALSE
  updated_status$urps_access_anchor_fielded <- TRUE
  updated_status$access_anchor_source <-
    "Lizeth national URPS mystery-caller study"
  updated_status$access_anchor_status <-
    "measured_input_unvalidated_response"
  updated_status$why_still_unresolved <- base::paste(
    "Lizeth directly measures URPS appointment access and wait time.",
    "It does not by itself identify latent productive capacity or",
    "the supply/demand adequacy ratio. A validated response function",
    "linking capacity to access would be required to infer that level."
  )
  updated_status$next_required_step <- base::paste(
    "Fit or externally validate a capacity-to-access response model;",
    "until then, retain adequacy as an explicit sensitivity parameter."
  )
  base::message(
    "Fielded URPS access evidence: YES; ",
    "identified capacity adequacy: NO."
  )
  updated_status
}

#' Run the complete Lizeth access-anchor workflow
#'
#' @param lizeth_dir Local path to the Lizeth repository.
#'
#' @return Named list with call records, anchor, evidence and status.
#'
#' @concept calibration
#' @family Lizeth access anchor
#' @export
build_lizeth_access_anchor <- function(lizeth_dir = "../lizeth") {
  base::message("Starting Lizeth URPS access-anchor workflow.")
  redcap_path <- find_lizeth_redcap(
    lizeth_dir = lizeth_dir
  )
  base::message("Reading labeled REDCap export.")
  redcap_records <- readr::read_csv(
    redcap_path,
    show_col_types = FALSE,
    progress = FALSE
  )
  parsed_records <- parse_lizeth_physician_information(
    lizeth_records = redcap_records
  )
  access_records <- prepare_lizeth_access(
    lizeth_records = parsed_records
  )
  lizeth_anchor <- estimate_lizeth_access_anchor(
    access_records = access_records
  )
  evidence_table <- lizeth_adequacy_evidence(
    lizeth_anchor = lizeth_anchor
  )
  status <- capacity_status_with_lizeth(
    lizeth_anchor = lizeth_anchor
  )
  base::message("Lizeth URPS access-anchor workflow complete.")
  base::list(
    redcap_path = redcap_path,
    calls = access_records,
    anchor = lizeth_anchor,
    evidence = evidence_table,
    capacity_status = status
  )
}
