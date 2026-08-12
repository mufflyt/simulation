################################################################################
# R/supply-hwsm_parameters.R
# HRSA HWSM work-effort and retirement parameters (Surgery proxy)
#
# The two supply mechanisms, kept SEPARATE the way HRSA and this engine both
# model them:
#   * work effort -> hwsm_fte = weekly professional hours / 40 (HRSA's own FTE
#     definition: 1 FTE = 40 professional hours/week). This is work effort AMONG
#     STILL-ACTIVE physicians and MUST NOT be multiplied by p_active -- retirement
#     is a separate stochastic event, not a haircut on hours.
#   * retirement -> a one-year conditional retirement probability derived from the
#     HRSA age/sex workforce-survival curve by 1 - S(a+1)/S(a), the same
#     discrete-hazard form as build_urps_exit_hazard().
#
# PROVENANCE. Weekly hours: pooled 2019 + 2022 AAMC National Sample Survey of
# Physicians, HRSA Health Workforce Simulation Model, Surgery category. Survival:
# HRSA HWSM Surgery workforce-participation curve. Surgery is the proxy for
# practising urogynecologists; OB/GYN-like alternatives belong in a sensitivity
# analysis, not as the base case.
#
# CALIBRATION TIER: derived_by_analogy. Surgery stands in for URPS; these are not
# ABOG/ABU departure micro-data, so nothing built on them is `calibrated`.
#
# FTE BASIS. hwsm_fte is on HRSA's 40 PROFESSIONAL-hours definition, which already
# includes indirect (non-patient-care) time. The rest of the engine's clinical-FTE
# path is 37.2 CLINICAL hours with indirect time grossed up separately on the
# demand side (INDIRECT_TIME_SHARE). The two definitions are NOT interchangeable:
# feeding hwsm_fte into the clinical-FTE gap without first putting demand on the
# same 40h professional basis double-counts indirect time. This module supplies
# the parameters; wiring them in as the engine default is a deliberate
# recalibration, not a column rename.
################################################################################

# HRSA HWSM Surgery weekly professional hours by age band and sex (2019+2022
# NSSP). Built once and cached; a top-level tribble would run at load time.
.hwsm_cache <- new.env(parent = emptyenv())

#' @noRd
.hwsm_hours_table <- function() {
  if (is.null(.hwsm_cache$hours)) {
    .hwsm_cache$hours <- tibble::tribble(
      ~sex_hwsm, ~age_min, ~age_max, ~weekly_hours,
      "Male",       0L,      34L,           56.0,
      "Male",      35L,      44L,           50.5,
      "Male",      45L,      54L,           49.4,
      "Male",      55L,      59L,           51.5,
      "Male",      60L,      64L,           50.5,
      "Male",      65L,      69L,           45.6,
      "Male",      70L,      74L,           40.0,
      "Male",      75L,     120L,           32.6,
      "Female",     0L,      34L,           50.4,
      "Female",    35L,      44L,           48.2,
      "Female",    45L,      54L,           47.1,
      "Female",    55L,      59L,           45.9,
      "Female",    60L,      64L,           44.9,
      "Female",    65L,      69L,           40.0,
      "Female",    70L,      74L,           34.3,
      "Female",    75L,     120L,           26.9
    )
  }
  .hwsm_cache$hours
}

# HRSA HWSM Surgery workforce-survival S(a) for ages 50..90 (P still active,
# given active at 50). Converted to a one-year conditional retirement hazard.
#' @noRd
.hwsm_survival_table <- function() {
  if (is.null(.hwsm_cache$survival)) {
    female_survival <- c(
      0.982104455, 0.982104455, 0.982104455, 0.982104455, 0.977311005,
      0.893442972, 0.888315537, 0.879422073, 0.861107072, 0.855905327,
      0.739681230, 0.681174005, 0.656650507, 0.647361675, 0.327767472,
      0.319518990, 0.285189684, 0.258739498, 0.258739498, 0.258739498,
      0.103269174, 0.103269174, 0.080972024, 0.080972024, 0.074706709,
      0.048834160, 0.043674489, 0.039810335, 0.039810335, 0.039810335,
      0.025201381, 0.025201381, 0.025201381, 0.020502394, 0.020502394,
      0.020502394, 0.020502394, 0.020502394, 0.020502394, 0.020502394, 0)
    male_survival <- c(
      0.998964056, 0.998964056, 0.996926134, 0.996926134, 0.996926134,
      0.976265759, 0.972439705, 0.969064450, 0.960926894, 0.958879905,
      0.895665643, 0.891485906, 0.862312546, 0.853101462, 0.840029596,
      0.658204884, 0.617861016, 0.538053396, 0.478171931, 0.462324733,
      0.264598344, 0.257089719, 0.210632033, 0.197687585, 0.188627363,
      0.096344518, 0.093570885, 0.091045926, 0.073748712, 0.067985635,
      0.032969524, 0.030334310, 0.028727298, 0.026843215, 0.026111795,
      0.013067703, 0.013067703, 0.012312315, 0.008931285, 0.008931285, 0)
    .hwsm_cache$survival <- dplyr::bind_rows(
      tibble::tibble(age_hwsm = 50:90, sex_hwsm = "Female", p_active = female_survival),
      tibble::tibble(age_hwsm = 50:90, sex_hwsm = "Male",   p_active = male_survival)
    ) %>%
      dplyr::group_by(.data$sex_hwsm) %>%
      dplyr::arrange(.data$age_hwsm, .by_group = TRUE) %>%
      dplyr::mutate(
        p_active_next = dplyr::lead(.data$p_active, default = 0),
        # One-year conditional retirement: 1 - S(a+1)/S(a); certain once inactive.
        p_retire_next_year = dplyr::case_when(
          .data$p_active <= 0 ~ 1,
          TRUE ~ 1 - (.data$p_active_next / .data$p_active)
        ),
        p_retire_next_year = pmax(0, pmin(1, .data$p_retire_next_year))
      ) %>%
      dplyr::ungroup()
  }
  .hwsm_cache$survival
}

#' Provenance of the HRSA HWSM supply parameters
#'
#' Every field a reader needs to decide whether these parameters fit their
#' purpose, carried with the numbers rather than in a document beside them.
#'
#' @return Named list of provenance fields.
#' @family hwsm supply
#' @concept supply
#' @export
hwsm_provenance <- function() {
  list(
    source = "HRSA Health Workforce Simulation Model (HWSM), Surgery category",
    hours_data = "Pooled 2019 + 2022 AAMC National Sample Survey of Physicians",
    fte_definition = "1.0 FTE = 40 professional hours/week (HRSA definition)",
    proxy = paste("Surgery is the proxy for practising urogynecologists;",
                  "OB/GYN-like alternatives are a sensitivity analysis, not the base case."),
    calibration_tier = "derived_by_analogy",
    retirement = paste("Age/sex workforce-survival S(a) converted to a one-year",
                       "conditional retirement probability 1 - S(a+1)/S(a)."),
    caveat = paste("hwsm_fte is work effort among STILL-ACTIVE physicians on the",
                   "40h professional basis. Do not multiply it by p_active, and do",
                   "not substitute it for the engine's 37.2 clinical-hour FTE",
                   "without putting demand on the same professional basis.")
  )
}

#' HRSA HWSM retirement hazard table (Surgery proxy)
#'
#' The HRSA Surgery workforce-survival curve as a per-age, per-sex one-year exit
#' probability, in the shape [advance_urps_agents()] consumes (`age`, `sex`,
#' `prob_exit`). Ages below `min_age` carry `prob_exit = 0` because HRSA models
#' permanent retirement only for ages 50+, and pre-50 attrition is a separate
#' career-change process here.
#'
#' @param min_age Age below which retirement probability is 0. Default 50.
#' @param max_age Highest age row emitted. Default 90.
#' @return Data frame with `age`, `sex` ("Female"/"Male"), `prob_exit`,
#'   `calibration_tier`, carrying a `provenance` attribute.
#' @family hwsm supply
#' @concept supply
#' @examples
#' \dontrun{
#' hz <- hwsm_retirement_hazard_table()
#' # Feed straight into the agent engine's annual advance:
#' advance_urps_agents(agents, exit_hazard = hz)
#' }
#' @export
hwsm_retirement_hazard_table <- function(min_age = 50L, max_age = 90L) {
  surv <- .hwsm_survival_table()
  grid <- expand.grid(age = seq.int(30L, as.integer(max_age)),
                      sex = c("Female", "Male"),
                      stringsAsFactors = FALSE)
  out <- merge(
    grid,
    data.frame(age = surv$age_hwsm, sex = surv$sex_hwsm,
               prob_exit = surv$p_retire_next_year, stringsAsFactors = FALSE),
    by = c("age", "sex"), all.x = TRUE
  )
  out$prob_exit[is.na(out$prob_exit) | out$age < min_age] <- 0
  out$calibration_tier <- "derived_by_analogy"
  out <- out[order(out$sex, out$age), c("age", "sex", "prob_exit", "calibration_tier")]
  rownames(out) <- NULL
  structure(out, provenance = hwsm_provenance())
}

#' Add HRSA HWSM work-effort and retirement parameters to a provider roster
#'
#' @description
#' Adds age- and sex-specific weekly professional hours, work-effort FTE
#' (`hwsm_fte`), probability of remaining active (`p_active`), and one-year
#' retirement probability (`p_retire_next_year`) from the HRSA HWSM Surgery
#' physician parameters. Surgery is the proxy for practising urogynecologists.
#'
#' HRSA defines one physician FTE as 40 professional hours per week, so
#' `hwsm_fte` can exceed 1.0 for high-hours cohorts.
#'
#' @details
#' `hwsm_fte` is work effort among physicians who are STILL ACTIVE. Retirement is
#' simulated separately (see [hwsm_retirement_hazard_table()]), so do NOT multiply
#' `hwsm_fte` by `p_active` -- that would charge retirement twice. `hwsm_fte` is on
#' HRSA's 40 professional-hours basis; it is not the engine's 37.2 clinical-hour
#' FTE (see [apply_hrsa_surgical_fte()] for the peak-relative clinical variant).
#'
#' @param provider_tbl A data frame with one row per physician.
#' @param age_col Name of the integer age column. Default "age".
#' @param sex_col Name of the sex column (Female/F, Male/M). Default "sex".
#' @param save_dir Optional directory in which to write the augmented roster as a
#'   timestamped CSV. `NULL` (default) writes nothing.
#' @param verbose Logical; emit progress and summary messages. Default TRUE.
#'
#' @return `provider_tbl` with HWSM columns added, carrying a `provenance`
#'   attribute from [hwsm_provenance()].
#' @importFrom dplyr mutate left_join case_when filter n join_by any_of
#' @importFrom rlang .data
#' @family hwsm supply
#' @concept supply
#' @examples
#' \dontrun{
#' roster <- data.frame(age = c(40L, 62L, 71L), sex = c("Female", "Male", "Female"))
#' add_hwsm_supply_parameters(roster)
#' }
#' @export
add_hwsm_supply_parameters <- function(provider_tbl,
                                       age_col = "age",
                                       sex_col = "sex",
                                       save_dir = NULL,
                                       verbose = TRUE) {
  if (!is.data.frame(provider_tbl))
    stop("`provider_tbl` must be a data frame.", call. = FALSE)

  missing_cols <- setdiff(c(age_col, sex_col), names(provider_tbl))
  if (length(missing_cols) > 0L)
    stop("Missing required columns: ", paste(missing_cols, collapse = ", "),
         call. = FALSE)

  if (verbose) {
    message("HWSM supply parameters: ",
            format(nrow(provider_tbl), big.mark = ","), " provider rows; ",
            "age='", age_col, "', sex='", sex_col, "'.")
  }

  hours_tbl <- .hwsm_hours_table()
  survival_tbl <- .hwsm_survival_table()

  augmented <- provider_tbl %>%
    dplyr::mutate(
      age_hwsm = as.integer(.data[[age_col]]),
      sex_hwsm = dplyr::case_when(
        tolower(trimws(as.character(.data[[sex_col]]))) %in%
          c("female", "f", "woman") ~ "Female",
        tolower(trimws(as.character(.data[[sex_col]]))) %in%
          c("male", "m", "man") ~ "Male",
        TRUE ~ NA_character_
      )
    )

  unknown_sex_n <- sum(is.na(augmented$sex_hwsm))
  if (unknown_sex_n > 0L && verbose)
    message("WARNING: ", format(unknown_sex_n, big.mark = ","),
            " row(s) have unrecognized or missing sex; HWSM columns are NA there.")

  augmented <- augmented %>%
    dplyr::left_join(
      hours_tbl,
      by = dplyr::join_by(sex_hwsm, age_hwsm >= age_min, age_hwsm <= age_max)
    ) %>%
    dplyr::mutate(hwsm_fte = .data$weekly_hours / 40) %>%
    dplyr::select(-dplyr::any_of(c("age_min", "age_max"))) %>%
    dplyr::left_join(
      survival_tbl[, c("age_hwsm", "sex_hwsm", "p_active", "p_retire_next_year")],
      by = c("age_hwsm", "sex_hwsm")
    ) %>%
    dplyr::mutate(
      # HRSA models permanent retirement for 50+; younger physicians are fully
      # active and their attrition is a separate career-change process.
      p_active = dplyr::case_when(
        is.na(.data$sex_hwsm) ~ NA_real_,
        .data$age_hwsm < 50L ~ 1,
        TRUE ~ .data$p_active
      ),
      p_retire_next_year = dplyr::case_when(
        is.na(.data$sex_hwsm) ~ NA_real_,
        .data$age_hwsm < 50L ~ 0,
        TRUE ~ .data$p_retire_next_year
      ),
      fte_source = "hrsa_hwsm_surgery",
      fte_calibration_tier = "derived_by_analogy"
    )

  if (verbose) {
    fte_total <- sum(augmented$hwsm_fte, na.rm = TRUE)
    mh <- mean(augmented$weekly_hours, na.rm = TRUE)
    message("Mean weekly professional hours: ", sprintf("%.1f", mh),
            "; total HWSM work-effort FTE: ", format(round(fte_total, 1), big.mark = ","),
            " (tier derived_by_analogy).")
  }

  if (!is.null(save_dir)) {
    if (!dir.exists(save_dir)) dir.create(save_dir, recursive = TRUE)
    save_path <- file.path(save_dir, "urogyne_hwsm_supply_parameters.csv")
    readr::write_csv(augmented, save_path)
    if (verbose) message("Saved augmented roster to: ", save_path)
  }

  structure(augmented, provenance = hwsm_provenance())
}
