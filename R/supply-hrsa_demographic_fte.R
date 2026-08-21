# HRSA Age-by-Sex Physician Professional Hours & Demographic FTE Engine ----
#
# Source:
# HRSA Health Workforce Simulation Model (HWSM) physician components,
# Exhibit V-5, reviewed December 2025.
#
# Interpretation:
# * Hours represent TOTAL professional hours worked per week.
# * On-call time when not actually working is excluded.
# * 1.0 FTE = 40 professional hours/week.
# * 40 * 52 = 2,080 professional hours/year.
# * Physician hours vary by age, sex, and broad specialty category.
#
# OB/GYN is classified by HRSA in the surgical specialty group.

HRSA_FTE_HOURS_PER_WEEK <- 40
HRSA_FTE_HOURS_PER_YEAR <- 2080

HRSA_PHYSICIAN_AGE_BREAKS <- c(
  0,
  35,
  45,
  55,
  60,
  65,
  70,
  75,
  Inf
)

HRSA_PHYSICIAN_AGE_LABELS <- c(
  "<35",
  "35-44",
  "45-54",
  "55-59",
  "60-64",
  "65-69",
  "70-74",
  "75+"
)


#' Current HRSA physician weekly-hours reference table
#'
#' @description
#' Returns the age-by-sex-by-specialty-group weekly professional hours
#' published in the current HRSA Health Workforce Simulation Model.
#'
#' These are total professional hours, not patient-care-only hours.
#'
#' @return Tibble with specialty group, gender, age group, and weekly
#'   professional hours.
#' @family provider lifecycle
#' @concept supply
#' @export
hrsa_physician_hours_table <- function() {

  age_group <- HRSA_PHYSICIAN_AGE_LABELS

  tibble::tibble(
    specialty_group = base::rep(
      c(
        "primary_care",
        "medical_specialties",
        "surgery",
        "other"
      ),
      each = 16L
    ),
    gender = base::rep(
      base::rep(
        c("male", "female"),
        each = 8L
      ),
      times = 4L
    ),
    age_group = base::rep(
      age_group,
      times = 8L
    ),
    weekly_hours = c(

      # Primary care
      46.7, 47.6, 46.4, 49.0,
      46.0, 42.5, 35.7, 31.7,

      47.4, 42.9, 43.5, 44.6,
      41.6, 38.1, 31.3, 27.3,

      # Medical specialties
      49.9, 48.8, 49.2, 50.3,
      48.2, 45.6, 38.5, 27.5,

      46.1, 46.8, 46.7, 46.5,
      44.4, 41.8, 34.7, 23.7,

      # Surgery
      56.0, 50.5, 49.4, 51.5,
      50.5, 45.6, 40.0, 32.6,

      50.4, 48.2, 47.1, 45.9,
      44.9, 40.0, 34.3, 26.9,

      # Other physician specialties
      49.4, 48.1, 46.6, 46.8,
      44.3, 40.9, 38.4, 34.6,

      46.4, 43.8, 43.4, 43.4,
      40.8, 37.4, 35.0, 31.1
    )
  )
}


#' Normalize physician gender for the HRSA hours model
#'
#' @param gender Character vector.
#'
#' @return Character vector containing `male` or `female`.
#' @keywords internal
.normalize_hrsa_gender <- function(gender) {

  gender_chr <- base::tolower(
    stringr::str_trim(
      base::as.character(gender)
    )
  )

  gender_chr <- dplyr::case_when(
    gender_chr %in% c(
      "female",
      "f",
      "woman",
      "women"
    ) ~ "female",
    gender_chr %in% c(
      "male",
      "m",
      "man",
      "men"
    ) ~ "male",
    TRUE ~ NA_character_
  )

  gender_chr
}


#' Assign HRSA physician age groups
#'
#' @param age Numeric physician age.
#'
#' @return Character HRSA age group.
#' @keywords internal
.hrsa_physician_age_group <- function(age) {

  age_num <- base::as.numeric(age)

  base::as.character(
    base::cut(
      age_num,
      breaks = HRSA_PHYSICIAN_AGE_BREAKS,
      labels = HRSA_PHYSICIAN_AGE_LABELS,
      right = FALSE,
      include.lowest = TRUE
    )
  )
}


#' Predict HRSA demographic weekly physician hours
#'
#' @description
#' Predicts expected weekly professional hours from the current HRSA
#' Health Workforce Simulation Model physician age-by-sex curves.
#'
#' The current HRSA physician model groups physicians into broad
#' specialty categories. Urogynecology should use `surgery`, because
#' HRSA classifies Obstetrics & Gynecology in its surgical group.
#'
#' @param age Numeric physician age.
#' @param gender Physician gender or sex. Common male/female aliases are
#'   accepted.
#' @param specialty_group HRSA broad physician specialty group. One of
#'   `surgery`, `primary_care`, `medical_specialties`, or `other`.
#'
#' @return Numeric expected weekly professional hours.
#' @family provider lifecycle
#' @concept supply
#' @export
predict_hrsa_demographic_hours <- function(
    age,
    gender,
    specialty_group = "surgery") {

  age_num <- base::as.numeric(age)

  if (base::length(age_num) < 1L) {
    return(numeric(0))
  }

  if (base::length(gender) == 1L) {
    gender <- base::rep(
      gender,
      base::length(age_num)
    )
  }

  if (base::length(gender) !=
      base::length(age_num)) {
    base::stop(
      "`gender` must have length 1 or the same length as `age`.",
      call. = FALSE
    )
  }

  allowed_groups <- c(
    "primary_care",
    "medical_specialties",
    "surgery",
    "other"
  )

  if (!specialty_group %in% allowed_groups) {
    base::stop(
      "`specialty_group` must be one of: ",
      base::paste(
        allowed_groups,
        collapse = ", "
      ),
      ".",
      call. = FALSE
    )
  }

  if (base::any(
    !base::is.finite(age_num)
  )) {
    base::stop(
      "`age` must contain finite numeric values.",
      call. = FALSE
    )
  }

  if (base::any(
    age_num < 20 |
      age_num > 100
  )) {
    base::warning(
      "Physician age outside 20-100 detected. ",
      "Verify the provider record.",
      call. = FALSE
    )
  }

  gender_chr <- .normalize_hrsa_gender(
    gender
  )

  if (base::any(
    base::is.na(gender_chr)
  )) {
    invalid_gender <- base::unique(
      gender[
        base::is.na(gender_chr)
      ]
    )

    base::stop(
      "Unrecognized gender value(s): ",
      base::paste(
        invalid_gender,
        collapse = ", "
      ),
      ".",
      call. = FALSE
    )
  }

  age_group_chr <- .hrsa_physician_age_group(
    age_num
  )

  spec_group <- specialty_group

  reference_tbl <- hrsa_physician_hours_table() |>
    dplyr::filter(
      .data$specialty_group == spec_group
    )

  prediction_tbl <- tibble::tibble(
    row_id = base::seq_along(age_num),
    age_group = age_group_chr,
    gender = gender_chr
  ) |>
    dplyr::left_join(
      reference_tbl |>
        dplyr::select(
          "gender",
          "age_group",
          "weekly_hours"
        ),
      by = c(
        "gender",
        "age_group"
      )
    ) |>
    dplyr::arrange(
      .data$row_id
    )

  if (base::any(
    base::is.na(
      prediction_tbl$weekly_hours
    )
  )) {
    base::stop(
      "HRSA weekly hours could not be assigned ",
      "to every physician.",
      call. = FALSE
    )
  }

  prediction_tbl$weekly_hours
}


#' Predict HRSA demographic physician FTE
#'
#' @description
#' Converts expected age-by-sex physician professional hours into HRSA
#' full-time equivalents.
#'
#' HRSA currently defines one physician FTE as 40 professional hours
#' per week:
#'
#' `FTE = weekly_hours / 40`
#'
#' Equivalently:
#'
#' `FTE = annual_hours / 2,080`
#'
#' This means an individual physician may contribute more than 1.0 FTE
#' when expected professional hours exceed 40 hours per week.
#'
#' @param age Numeric physician age.
#' @param gender Physician gender or sex.
#' @param specialty_group HRSA broad physician specialty group.
#'   Urogynecology should normally use `surgery`.
#' @param return_components Logical. When `FALSE`, return only FTE.
#'   When `TRUE`, return age, gender, hours, and FTE components.
#'
#' @return Numeric FTE vector, or a tibble when `return_components = TRUE`.
#' @family provider lifecycle
#' @concept supply
#' @export
predict_hrsa_demographic_fte <- function(
    age,
    gender,
    specialty_group = "surgery",
    return_components = FALSE) {

  base::message(
    "[hrsa-fte] Predicting demographic physician FTE."
  )

  weekly_hours <- predict_hrsa_demographic_hours(
    age = age,
    gender = gender,
    specialty_group = specialty_group
  )

  annual_hours <- weekly_hours * 52

  demographic_fte <- annual_hours /
    HRSA_FTE_HOURS_PER_YEAR

  # Mathematically equivalent check:
  direct_fte <- weekly_hours /
    HRSA_FTE_HOURS_PER_WEEK

  if (!base::isTRUE(
    base::all.equal(
      demographic_fte,
      direct_fte,
      tolerance = 1e-12
    )
  )) {
    base::stop(
      "Weekly and annual HRSA FTE calculations disagree.",
      call. = FALSE
    )
  }

  base::message(
    "[hrsa-fte] Specialty group: ",
    specialty_group,
    "."
  )

  base::message(
    "[hrsa-fte] 1.0 FTE = ",
    HRSA_FTE_HOURS_PER_WEEK,
    " hours/week = ",
    base::format(
      HRSA_FTE_HOURS_PER_YEAR,
      big.mark = ","
    ),
    " hours/year."
  )

  if (!base::isTRUE(return_components)) {
    return(
      demographic_fte
    )
  }

  gender_chr <- .normalize_hrsa_gender(
    gender
  )

  if (base::length(gender_chr) == 1L &&
      base::length(age) > 1L) {
    gender_chr <- base::rep(
      gender_chr,
      base::length(age)
    )
  }

  tibble::tibble(
    age = base::as.numeric(age),
    gender = gender_chr,
    specialty_group = specialty_group,
    age_group = .hrsa_physician_age_group(
      age
    ),
    weekly_hours = weekly_hours,
    annual_hours = annual_hours,
    demographic_fte = demographic_fte
  )
}
