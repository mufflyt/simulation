################################################################################
# R/calibration-transport_to_national.R
# Transport CHIA (Massachusetts inpatient) and CADR (national Medicare, treated)
# to national all-payer annual volumes -- or refuse, legibly.
#
# WHY THIS IS NOT A CONVERSION FACTOR
# -----------------------------------
# Both sources are missing a different piece of the national all-payer estimand,
# and neither can supply its own missing piece:
#
#   CHIA   has the right PAYERS and the wrong GEOGRAPHY and the wrong SETTING.
#          Massachusetts, inpatient only. CHIA Case Mix has no ambulatory-surgery
#          database and 957 CMR 8.00 binds acute care hospitals, so freestanding
#          ASCs never submit. Most urogynaecologic surgery is now ambulatory.
#
#   CADR   has the right GEOGRAPHY and the wrong PAYERS and a conditional
#          POPULATION. National, but Medicare and treated-cohort-conditional.
#
# So transport needs THREE factors, of which the data supply only one:
#
#   (a) age standardisation MA -> US        MEASURABLE here
#   (b) geographic transportability MA -> US   ASSUMED, and questionable
#   (c) setting share inpatient -> all         NOT AVAILABLE from either source
#
# This module computes (a), makes (b) an explicit declared multiplier rather
# than a silent 1.0, and REFUSES to emit an all-setting national volume without
# (c) supplied from a named external source. A transport model that quietly
# defaults its unknown factor to 1 is laundering, not transport.
################################################################################

#' Massachusetts age-specific inpatient rates for a procedure family
#'
#' @param db Path to chia_cadr.duckdb.
#' @param year Fiscal year. The CHIA series is strongly non-stationary
#'   (POP-hysterectomy falls ~78% across FY2004-2018 through setting migration),
#'   so a multi-year average is NOT appropriate; use the most recent year.
#' @param family One of "pop_hysterectomy", "all_hysterectomy", or "sui_sling".
#' @param min_cases Refuse to return rates below this count. Age-specific rates
#'   from a handful of cases are noise, and transporting them multiplies that
#'   noise by the US population. Default 50.
#' @return Tibble: age band, cases, women, rate per 100,000.
#' @export
chia_ma_age_specific_rates <- function(
    db = "/Volumes/MufflySamsung/DuckDB/chia_cadr.duckdb",
    year = 2018L,
    family = c("pop_hysterectomy", "all_hysterectomy", "sui_sling"),
    min_cases = 50L) {

  family <- base::match.arg(family)
  if (!base::file.exists(db)) {
    base::stop("CHIA database not found: ", db, call. = FALSE)
  }
  if (!year %in% 2015:2018) {
    base::stop("Massachusetts denominators cover 2015-2018 only; got ", year,
               ". Earlier years need a Census vintage this repo does not hold.",
               call. = FALSE)
  }

  base::message("Computing MA age-specific rates: ", family, ", FY", year)

  # Code sets follow config/chia_urps_inpatient_codes.yml, including the
  # 3-digit ICD-9 codes withdrawn in the October 2006 update (684/686/687).
  # Omitting those undercounts FY2004-2006 by ~10%.
  hyst_i9 <- "'6831','6839','6841','6849','6851','6859','6861','6869',
              '6871','6879','689','684','686','687'"
  procedure_clause <- switch(family,
    pop_hysterectomy = base::sprintf(
      "(c._data_year <= 2015 AND c.principal_procedure IN (%s))
       OR (c._data_year >= 2016 AND (c.principal_procedure LIKE '0UT9%%'
                                  OR c.principal_procedure LIKE '0UB9%%'))", hyst_i9),
    all_hysterectomy = base::sprintf(
      "(c._data_year <= 2015 AND c.principal_procedure IN (%s))
       OR (c._data_year >= 2016 AND (c.principal_procedure LIKE '0UT9%%'
                                  OR c.principal_procedure LIKE '0UB9%%'))", hyst_i9),
    sui_sling =
      "(c._data_year <= 2015 AND c.principal_procedure IN ('594','595','596','5971'))
       OR (c._data_year >= 2016 AND c.principal_procedure LIKE '0TUD%')")

  # Only pop_hysterectomy is indication-qualified.
  dx_join <- if (family == "pop_hysterectomy") {
    "JOIN dx USING (RecordType20ID, _data_year)"
  } else ""

  connection <- DBI::dbConnect(duckdb::duckdb(), db, read_only = TRUE)
  base::on.exit(DBI::dbDisconnect(connection, shutdown = TRUE), add = TRUE)

  rates <- DBI::dbGetQuery(connection, base::sprintf("
    WITH dx AS (
      SELECT DISTINCT RecordType20ID, _data_year
      FROM chia_casemix.hdd_diagnosis_long
      WHERE code LIKE '618%%' OR code LIKE 'N81%%'),
    cases AS (
      SELECT CASE WHEN c.age_capped < 50 THEN '18-49'
                  WHEN c.age_capped < 65 THEN '50-64'
                  WHEN c.age_capped < 80 THEN '65-79'
                  ELSE '80+' END AS age_band,
             count(*) AS cases
      FROM chia_casemix.v_cohort_female_adult c
      %s
      WHERE c._data_year = %d AND (%s)
      GROUP BY 1),
    pop AS (
      SELECT CASE WHEN age < 50 THEN '18-49'
                  WHEN age < 65 THEN '50-64'
                  WHEN age < 80 THEN '65-79'
                  ELSE '80+' END AS age_band,
             sum(pop_%d) AS women
      FROM ref.census_ma_pop_age_sex
      WHERE sex = 'F' AND age >= 18
      GROUP BY 1)
    SELECT p.age_band, coalesce(c.cases, 0) AS cases, p.women
    FROM pop p LEFT JOIN cases c USING (age_band)
    ORDER BY 1", dx_join, year, procedure_clause, year)) |>
    tibble::as_tibble() |>
    dplyr::mutate(
      women = base::as.numeric(women),
      rate_per_100k = 1e5 * cases / women,
      fiscal_year = base::as.integer(year),
      geography = "Massachusetts",
      setting = "hospital inpatient only")

  total_cases <- base::sum(rates$cases)
  base::message("  ", total_cases, " cases across ", base::nrow(rates),
                " age bands; crude rate ",
                base::sprintf("%.2f", 1e5 * total_cases / base::sum(rates$women)),
                " per 100,000 women")

  if (total_cases < min_cases) {
    base::stop(
      "Refusing to return age-specific rates for '", family, "' in FY", year,
      ": only ", total_cases, " inpatient cases (minimum ", min_cases, "). ",
      "Rates from this few cases are noise, and transporting them multiplies ",
      "that noise by the US female population. ",
      if (family == "sui_sling") base::paste0(
        "For sling this is not a sample-size accident: inpatient slings in ",
        "the CHIA cohort fall 155 (FY2004) -> 17 (FY2014) -> 1 (FY2017) -> 0 ",
        "(FY2018). The procedure has left the inpatient setting, so CHIA has ",
        "no sling rate to transport at any sample size. Use HCUP SASD or ",
        "Medicare Part B carrier claims."), call. = FALSE)
  }
  rates
}

#' US female population by the same age bands
#'
#' @param year Calendar year. The bundled Census file (np2023) is a PROJECTION
#'   series covering 2022-2100; it does not reach the CHIA observation window,
#'   which is itself the temporal-transport problem this function makes visible.
#' @param path Census national projection CSV.
#' @return Tibble: age band, women.
#' @export
us_female_population_bands <- function(
    year = 2023L,
    path = base::file.path("data-raw", "census", "np2023_d1_mid.csv")) {

  if (!base::file.exists(path)) {
    base::stop("Census national file not found: ", path, call. = FALSE)
  }
  census <- readr::read_csv(path, show_col_types = FALSE, progress = FALSE)

  # SEX 2 = female; ORIGIN 0 and RACE 0 are the all-groups totals.
  female <- census |>
    dplyr::filter(SEX == 2, ORIGIN == 0, RACE == 0, YEAR == year)
  if (base::nrow(female) != 1L) {
    base::stop("Expected exactly one US female row for ", year, "; got ",
               base::nrow(female),
               ". Available years: ", base::min(census$YEAR), "-",
               base::max(census$YEAR), ".", call. = FALSE)
  }

  ages <- 18:100
  counts <- base::vapply(ages, function(a) {
    column <- base::paste0("POP_", a)
    if (column %in% base::names(female)) {
      base::as.numeric(female[[column]][[1L]])
    } else NA_real_
  }, numeric(1))

  tibble::tibble(age = ages, women = counts) |>
    dplyr::filter(!base::is.na(women)) |>
    dplyr::mutate(age_band = dplyr::case_when(
      age < 50 ~ "18-49", age < 65 ~ "50-64",
      age < 80 ~ "65-79", TRUE ~ "80+")) |>
    dplyr::group_by(age_band) |>
    dplyr::summarise(women = base::sum(women), .groups = "drop") |>
    dplyr::mutate(calendar_year = base::as.integer(year),
                  geography = "United States")
}

#' Transport CHIA to a national volume
#'
#' Direct age standardisation of Massachusetts inpatient rates onto the US
#' female population, then two declared multipliers. The function will not
#' return an all-setting national volume unless `inpatient_share` is supplied
#' with a source.
#'
#' @param db Path to chia_cadr.duckdb.
#' @param chia_year CHIA fiscal year supplying the rates.
#' @param target_year Calendar year for the US denominator.
#' @param inpatient_share Fraction of all procedures performed inpatient. There
#'   is deliberately NO DEFAULT: neither CHIA nor CADR observes it, and assuming
#'   1.0 would silently report an inpatient count as a national total. Supply it
#'   from HCUP SASD/NASS or an equivalent all-setting source, with
#'   `inpatient_share_source`.
#' @param inpatient_share_source Free text naming the source. Required whenever
#'   `inpatient_share` is given.
#' @param census_path Census national projection CSV.
#' @param geographic_multiplier Adjustment for Massachusetts differing from the
#'   US. Default 1.0 means "assume transportable", which is an ASSUMPTION and
#'   almost certainly wrong in a knowable direction -- see the returned
#'   `factors` table.
#' @return Named list: rates, us_population, factors, and estimate.
#' @export
transport_chia_to_national <- function(
    db = "/Volumes/MufflySamsung/DuckDB/chia_cadr.duckdb",
    chia_year = 2018L,
    target_year = 2023L,
    inpatient_share = NULL,
    inpatient_share_source = NULL,
    geographic_multiplier = 1.0,
    census_path = base::file.path("data-raw", "census", "np2023_d1_mid.csv")) {

  base::message("========================================")
  base::message("CHIA -> NATIONAL TRANSPORT")
  base::message("========================================")

  if (!base::is.null(inpatient_share)) {
    if (base::is.null(inpatient_share_source) ||
        !base::nzchar(inpatient_share_source)) {
      base::stop("inpatient_share was supplied without ",
                 "inpatient_share_source. An unsourced setting share is the ",
                 "single largest lever on the answer and must be attributable.",
                 call. = FALSE)
    }
    if (!base::is.finite(inpatient_share) || inpatient_share <= 0 ||
        inpatient_share > 1) {
      base::stop("inpatient_share must be in (0, 1]; got ", inpatient_share,
                 call. = FALSE)
    }
  }

  rates <- chia_ma_age_specific_rates(db = db, year = chia_year)
  us_pop <- us_female_population_bands(year = target_year, path = census_path)

  joined <- dplyr::inner_join(
    rates |> dplyr::select(age_band, cases, ma_women = women, rate_per_100k),
    us_pop |> dplyr::select(age_band, us_women = women),
    by = "age_band")

  joined <- joined |>
    dplyr::mutate(expected_national_inpatient =
                    rate_per_100k / 1e5 * us_women * geographic_multiplier)

  national_inpatient <- base::sum(joined$expected_national_inpatient)

  base::message(base::sprintf(
    "Age-standardised national INPATIENT volume: %s",
    base::format(base::round(national_inpatient), big.mark = ",")))

  national_all_setting <- if (!base::is.null(inpatient_share)) {
    national_inpatient / inpatient_share
  } else NA_real_

  if (base::is.na(national_all_setting)) {
    base::message("National ALL-SETTING volume: NOT COMPUTED.")
    base::message("  inpatient_share was not supplied. Neither CHIA nor CADR ",
                  "observes it, and defaulting it to 1.0 would report an ",
                  "inpatient count as a national total.")
  } else {
    base::message(base::sprintf(
      "National ALL-SETTING volume: %s  (inpatient share %.3f, source: %s)",
      base::format(base::round(national_all_setting), big.mark = ","),
      inpatient_share, inpatient_share_source))
  }

  factors <- tibble::tribble(
    ~factor, ~value, ~evidence_tier, ~threat_to_validity,
    "MA age-specific inpatient rate", base::sum(joined$cases), "measured",
      base::paste("CHIA FY", chia_year,
                  "; the series is non-stationary (falls ~78% FY2004-2018 via",
                  "setting migration), so the rate is a point in a moving",
                  "series, not a stable parameter."),
    "US female population", base::sum(joined$us_women), "measured",
      base::paste("Census np2023 projection for", target_year,
                  "-- a PROJECTION, and it does not overlap the CHIA",
                  "observation window at all."),
    "temporal transport", base::as.numeric(target_year - chia_year), "assumed",
      base::paste("Applying FY", chia_year, "rates to", target_year,
                  "assumes setting migration stopped. The CHIA series says it",
                  "had not. This biases the inpatient estimate UPWARD."),
    "geographic transportability", geographic_multiplier, "assumed",
      base::paste("Massachusetts has near-universal coverage (post-Chapter 58",
                  "self-pay ~0.5% vs national uninsurance), unusually dense",
                  "subspecialty supply, and is highly metropolitan. All three",
                  "plausibly raise utilisation, so 1.0 likely biases the",
                  "national estimate UPWARD."),
    "inpatient share", if (base::is.null(inpatient_share)) NA_real_ else inpatient_share,
      if (base::is.null(inpatient_share)) "NOT SUPPLIED" else "external",
      base::paste("Not observable in CHIA or CADR. Dominates the answer: at a",
                  "10% inpatient share the national total is ten times the",
                  "inpatient volume."))

  # Computed before the tibble: inside tibble(), later expressions see columns
  # created by earlier ones, so `inpatient_share` would refer to the NA_real_
  # column rather than the (NULL) argument and the status would invert.
  share_supplied <- !base::is.null(inpatient_share)
  status <- if (share_supplied) "transported_with_external_setting_share" else
    "incomplete_transport_inpatient_only"

  base::list(
    rates = rates, us_population = us_pop, standardised = joined,
    factors = factors,
    estimate = tibble::tibble(
      estimand = "POP-indication hysterectomy",
      national_inpatient = national_inpatient,
      national_all_setting = national_all_setting,
      chia_year = base::as.integer(chia_year),
      target_year = base::as.integer(target_year),
      geographic_multiplier = geographic_multiplier,
      inpatient_share = if (base::is.null(inpatient_share)) NA_real_ else inpatient_share,
      inpatient_share_source = if (base::is.null(inpatient_share_source)) {
        NA_character_ } else inpatient_share_source,
      production_scalar_eligible = FALSE,
      evidence_status = status))
}

#' Transport CADR to a national all-payer volume
#'
#' CADR is already national but Medicare and treated-cohort-conditional. The
#' missing factor is the Medicare share of all procedures, which CADR cannot
#' observe -- a Medicare-only file has no denominator for the non-Medicare
#' population. Same discipline: no default.
#'
#' @param cadr_path CADR workload artifact.
#' @param pathway Pathway to transport.
#' @param year_start,year_end CADR observation window.
#' @param medicare_share Fraction of all procedures billed to Medicare. NO
#'   DEFAULT.
#' @param medicare_share_source Free text naming the source. Required with
#'   `medicare_share`.
#' @return Named list: cohort, factors, estimate.
#' @export
transport_cadr_to_national <- function(
    cadr_path = base::file.path("scripts", "cadr", "outputs",
                                "workload_per_treated_patient.csv"),
    pathway = "UI Sling",
    year_start = 2008L, year_end = 2016L,
    medicare_share = NULL,
    medicare_share_source = NULL) {

  base::message("========================================")
  base::message("CADR -> NATIONAL TRANSPORT")
  base::message("========================================")

  if (!base::is.null(medicare_share)) {
    if (base::is.null(medicare_share_source) ||
        !base::nzchar(medicare_share_source)) {
      base::stop("medicare_share was supplied without medicare_share_source.",
                 call. = FALSE)
    }
    if (!base::is.finite(medicare_share) || medicare_share <= 0 ||
        medicare_share > 1) {
      base::stop("medicare_share must be in (0, 1]; got ", medicare_share,
                 call. = FALSE)
    }
  }

  cadr <- readr::read_csv(cadr_path, show_col_types = FALSE, progress = FALSE)
  .require_columns(cadr, c("pathway", "n_treated_episodes"), "CADR artifact")

  row <- cadr |> dplyr::filter(pathway == !!pathway)
  if (base::nrow(row) != 1L) {
    base::stop("Expected one CADR row for pathway '", pathway, "'; got ",
               base::nrow(row), call. = FALSE)
  }

  n_years <- year_end - year_start + 1L
  cohort_episodes <- row$n_treated_episodes[[1L]]
  medicare_per_year <- cohort_episodes / n_years

  base::message(base::sprintf(
    "%s: %s episodes across %d-%d = %s/yr in the treated Medicare cohort",
    pathway, base::format(cohort_episodes, big.mark = ","),
    year_start, year_end, base::format(base::round(medicare_per_year),
                                       big.mark = ",")))

  national <- if (!base::is.null(medicare_share)) {
    medicare_per_year / medicare_share
  } else NA_real_

  if (base::is.na(national)) {
    base::message("National ALL-PAYER volume: NOT COMPUTED.")
    base::message("  medicare_share was not supplied. A Medicare-only file has ",
                  "no denominator for the non-Medicare population, so CADR ",
                  "cannot estimate its own share.")
  } else {
    base::message(base::sprintf(
      "National ALL-PAYER volume: %s/yr  (Medicare share %.3f, source: %s)",
      base::format(base::round(national), big.mark = ","),
      medicare_share, medicare_share_source))
  }

  factors <- tibble::tribble(
    ~factor, ~value, ~evidence_tier, ~threat_to_validity,
    "CADR treated episodes", cohort_episodes, "measured",
      base::paste("Full cohort count across", year_start, "-", year_end,
                  "-- NOT an annual figure."),
    "episodes per observed year", medicare_per_year, "derived",
      "Assumes uniform incidence across the window; CADR does not publish a per-year series here.",
    "Medicare share of all procedures", 
      if (base::is.null(medicare_share)) NA_real_ else medicare_share,
      if (base::is.null(medicare_share)) "NOT SUPPLIED" else "external",
      base::paste("Not observable in CADR. For sling this is the dominant",
                  "unknown: a large share of SUI surgery is performed in",
                  "commercially insured women under 65, whom CADR never sees."),
    "age structure", NA_real_, "assumed",
      base::paste("CADR is 65+. Transporting to all ages assumes the age",
                  "distribution of treatment, which CADR cannot observe below",
                  "65. This is a second missing factor, not a refinement."))

  share_supplied <- !base::is.null(medicare_share)
  status <- if (share_supplied) "transported_with_external_share" else
    "incomplete_transport_medicare_only"

  base::list(
    cohort = row, factors = factors,
    estimate = tibble::tibble(
      estimand = base::paste(pathway, "procedures"),
      medicare_cohort_episodes = cohort_episodes,
      medicare_per_year = medicare_per_year,
      national_all_payer_per_year = national,
      medicare_share = if (base::is.null(medicare_share)) NA_real_ else medicare_share,
      medicare_share_source = if (base::is.null(medicare_share_source)) {
        NA_character_ } else medicare_share_source,
      production_scalar_eligible = FALSE,
      evidence_status = status))
}

#' Sensitivity of the transported national volume to the missing factor
#'
#' The point of this function is that the range is wide. If it is not shown, a
#' single transported number reads as an estimate rather than as one draw from
#' a span the data cannot narrow.
#'
#' @param national_inpatient Inpatient volume from `transport_chia_to_national`.
#' @param shares Candidate inpatient shares.
#' @return Tibble of implied national all-setting volumes.
#' @export
transport_setting_share_sensitivity <- function(
    national_inpatient,
    shares = c(0.05, 0.10, 0.15, 0.20, 0.30, 0.50)) {
  tibble::tibble(
    inpatient_share = shares,
    national_all_setting = national_inpatient / shares,
    fold_change_vs_inpatient = 1 / shares)
}
