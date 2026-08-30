# Diagnostic denominator table ----
#
# DIAGNOSTIC INFRASTRUCTURE ONLY. Nothing here feeds the production pipeline,
# alters `per_entering`, or changes readiness. Its purpose is to make the shape
# of what is known and what is missing INSPECTABLE, so that a missing
# denominator is visible as a missing denominator rather than quietly replaced
# by a plausible number.
#
# WHY THIS FILE EXISTS AT ALL.
#
# Medicare Part B PUF yields 79,787 practice-new FPMRS office consultations in
# 2023 (roster-linked NPIs, HCPCS 99202-99205). The tempting next step is
#
#     per_entering <- 79787 / entering
#
# and it is wrong. `pathway_stage_entrants()` sets the conservative stage's
# `entering` to `treated[[cond]]` UNCHANGED, and `.lifecourse_treated()` builds
# `treated` as
#
#     prevalence x recognition x p_seek x p_referral x p_eligible x p_treated
#
# which is 2.8-10.7% of prevalence and already carries the very multipliers the
# canonical estimand ABSORBS. Dividing an observed consultation count by it
# double-counts recognition, seeking and referral.
#
# Note precisely what the defect is: NOT circularity. `per_entering` is applied
# after `entering` and never feeds back into `treated`, so there is no loop to
# find. The denominator is simply a different quantity from the one the
# estimand names. That distinction matters because "circular" sends a reader
# looking for feedback that does not exist, and they may conclude the concern
# was overblown.
#
# The canonical denominator is the UPSTREAM eligible prevalent stock:
#
#     prevalence x p_eligible          -- and nothing else
#
# `.assert_upstream_denominator()` enforces exactly that, and the negative
# tests in test-diagnostic-denominator-table.R prove it rejects `treated`,
# stage volumes and the rest.

#' Estimand tag required of any denominator in the diagnostic table
#'
#' A denominator is admissible only if it is explicitly declared to be the
#' upstream eligible prevalent stock. Declaration is required rather than
#' inferred: a numeric vector carries no evidence of how it was built, and the
#' whole failure mode here is a downstream quantity that looks numerically fine.
#'
#' @format A length-one character string.
#' @family diagnostic denominator
#' @concept demand
#' @export
DIAGNOSTIC_UPSTREAM_DENOMINATOR_TAG <- "upstream_eligible_prevalent_stock"

# Quantities that are DOWNSTREAM of, or entangled with, the entry process.
# Matched as substrings, case-insensitively, so `treated_national`,
# `.lifecourse_treated` and `conservative_stage_volume` are all caught without
# needing to be enumerated exactly.
.DIAGNOSTIC_DOWNSTREAM_DENOMINATORS <- c(
  "treated", "treatment_state", "care_seeking", "p_seek", "seeking",
  "referral", "p_referral", "recognition",
  "entering", "stage_entrant", "per_entering",
  "service_volume", "pathway_service_volumes", "stage_volume", "volume",
  "new_consultation", "conservative_stage"
)

#' Reject a denominator that is not an upstream eligible prevalent stock
#'
#' @param denominator_estimand Declared estimand tag. Must equal
#'   [DIAGNOSTIC_UPSTREAM_DENOMINATOR_TAG].
#' @param denominator_source Free-text provenance for the denominator.
#' @param call_context Label used in the error message.
#' @return `TRUE`, invisibly, when admissible; otherwise an error.
#' @family diagnostic denominator
#' @concept demand
#' @export
assert_upstream_denominator <- function(denominator_estimand,
                                        denominator_source,
                                        call_context = "diagnostic denominator") {
  if (!is.character(denominator_estimand) || length(denominator_estimand) != 1L) {
    stop(call_context, ": denominator_estimand must be a single string.",
         call. = FALSE)
  }
  if (!identical(denominator_estimand, DIAGNOSTIC_UPSTREAM_DENOMINATOR_TAG)) {
    stop(call_context, ": denominator_estimand is '", denominator_estimand,
         "', but only '", DIAGNOSTIC_UPSTREAM_DENOMINATOR_TAG, "' is admissible. ",
         "The canonical estimand's denominator is the eligible prevalent stock ",
         "(prevalence x p_eligible). A denominator carrying recognition, ",
         "seeking, referral or treatment double-counts the losses the entry ",
         "rate already absorbs.", call. = FALSE)
  }
  if (!is.character(denominator_source) || length(denominator_source) != 1L ||
      !nzchar(denominator_source)) {
    stop(call_context, ": denominator_source must be a non-empty string. ",
         "An undeclared provenance cannot be audited, which is the condition ",
         "under which a downstream quantity gets used by accident.", call. = FALSE)
  }
  hit <- .DIAGNOSTIC_DOWNSTREAM_DENOMINATORS[
    vapply(.DIAGNOSTIC_DOWNSTREAM_DENOMINATORS,
           function(p) grepl(p, tolower(denominator_source), fixed = TRUE),
           logical(1))
  ]
  if (length(hit)) {
    stop(call_context, ": denominator_source '", denominator_source,
         "' names a quantity downstream of the entry process ('",
         paste(hit, collapse = "', '"), "'). ",
         "pathway_stage_entrants() sets conservative-stage `entering` to ",
         "`treated` unchanged, and `treated` is prevalence x recognition x ",
         "p_seek x p_referral x p_eligible x p_treated. Using it here would ",
         "count recognition, seeking and referral twice.", call. = FALSE)
  }
  invisible(TRUE)
}

# ACS 2023 B01001 FEMALE variables, mapped onto the prevalence source's OWN age
# bands. Deliberately not re-banded into URPS_POP_AGE_BANDS: pfd_prevalence_by_band()
# publishes 20-39/40-59/60-64/65-79/80+, and interpolating those into
# 18-34/35-44/45-64/65-74/75+ would invent within-band structure the source does
# not carry. The mismatch is reported as a limitation instead of smoothed away.
.diagnostic_acs_female_bands <- function(acs_path) {
  if (!nzchar(acs_path) || !file.exists(acs_path)) {
    stop("diagnostic denominator: ACS population file not found. This function ",
         "will not substitute an estimate.", call. = FALSE)
  }
  acs <- utils::read.csv(acs_path, colClasses = "character", stringsAsFactors = FALSE)
  acs$estimate <- suppressWarnings(as.numeric(acs$estimate))
  vapply(.DIAGNOSTIC_ACS_FEMALE_BANDS, function(vars) {
    sum(acs$estimate[acs$variable %in% vars], na.rm = TRUE)
  }, numeric(1))
}

.DIAGNOSTIC_ACS_FEMALE_BANDS <- list(
  "20-39" = sprintf("B01001_%03d", 32:37),
  "40-59" = sprintf("B01001_%03d", 38:41),
  "60-64" = sprintf("B01001_%03d", 42:43),
  "65-79" = sprintf("B01001_%03d", 44:47),
  "80+"   = sprintf("B01001_%03d", 48:49)
)

#' Medicare FFS enrolment denominator, 2023
#'
#' @details
#' Women aged 65+ enrolled in Original Medicare (fee-for-service), 2023:
#' **16,542,982**. From CMS Program Statistics, Original Medicare Enrolment,
#' Table 12 -- a PUBLISHED CELL crossing sex with entitlement type, not a
#' marginal multiplied by a share. Male + female aged reconciles to the
#' published 65+ total within one person.
#'
#' Public data. No DUA. See the shipped manifest for provenance.
#'
#' `part_b_share_65plus` is published for 65+ overall but NOT by sex, so
#' applying it to the female count is an assumption and is returned separately
#' rather than folded in.
#'
#' @param path Path to the shipped CSV.
#' @return A named list of enrolment quantities.
#' @family diagnostic denominator
#' @concept demand
#' @export
cms_original_medicare_enrollment_2023 <- function(
    path = system.file("extdata", "cms_original_medicare_enrollment_2023.csv",
                       package = "urpssim")) {
  if (!nzchar(path) || !file.exists(path)) {
    stop("cms_original_medicare_enrollment_2023(): shipped CSV not found. ",
         "This function will not substitute an estimate.", call. = FALSE)
  }
  d <- utils::read.csv(path, stringsAsFactors = FALSE)
  v <- stats::setNames(as.numeric(d$value), d$metric)
  female_65 <- unname(v[["female_aged_65plus_original_medicare"]])
  recon <- unname(v[["male_aged_65plus_original_medicare"]]) + female_65
  published <- unname(v[["total_aged_65plus_original_medicare"]])
  # The reconciliation is ASSERTED, not trusted. If a future edit changes one
  # cell, this fails loudly rather than silently shifting every rate built on it.
  if (abs(recon - published) > 5) {
    stop("cms_original_medicare_enrollment_2023(): male + female aged (",
         recon, ") does not reconcile with the published 65+ total (",
         published, "). One of the vendored cells is wrong.", call. = FALSE)
  }
  list(
    female_65plus_ffs = female_65,
    total_65plus_ffs = published,
    part_b_share_65plus = unname(v[["part_b_65plus_original_medicare"]]) /
      unname(v[["part_a_or_b_65plus_original_medicare"]]),
    source = "cms_program_statistics_original_medicare_enrollment_2023_table12"
  )
}

# Prevalence at 65+ requires collapsing the source's 65-79 and 80+ bands into
# one. The weights come from the ACS female 65+ age split, because CMS does not
# publish its FFS age distribution crossed with sex. That is an ASSUMPTION --
# that FFS women 65+ have the same 65-79 vs 80+ split as all women 65+ -- and
# rows built on it are tagged ASSUMPTION rather than OK.
.diagnostic_prevalence_65plus <- function(condition_key, pop_band) {
  prev <- pfd_prevalence_by_band(condition_key)
  w79 <- pop_band[["65-79"]]; w80 <- pop_band[["80+"]]
  (prev[["65-79"]] * w79 + prev[["80+"]] * w80) / (w79 + w80)
}

#' Aggregate Medicare FFS practice-new FPMRS E/M services, 2023
#'
#' @details
#' **79,787 SERVICES, not 79,787 women.** From Medicare Part B PUF `by_service`
#' 2023: HCPCS 99202-99205 billed by 794 roster NPIs, across 1,322
#' NPI x HCPCS x place-of-service cells.
#'
#' THE COUNT CANNOT BE DEDUPLICATED TO PEOPLE. An earlier version of this
#' function reasoned that because `Tot_Srvcs` (79,787) and summed `Tot_Benes`
#' (79,785) are nearly equal, the total was "effectively one per person". That
#' inference does not hold. PUF beneficiary counts are computed WITHIN each
#' provider/service cell, so their near-equality shows only that a woman rarely
#' receives the same new-patient code twice from the same provider -- which is
#' what the billing rules already require. It says nothing about whether the
#' same woman appears in a DIFFERENT cell: another NPI, another group, or
#' another new-patient code during the year. With 1,322 cells over 794 NPIs
#' there is ample room for exactly that, and the PUF provides no key with which
#' to detect it.
#'
#' So the defensible unit is **summed practice-new FPMRS E/M services**, and any
#' rate built on it is a service rate per 1,000 population -- never a
#' probability that an individual woman entered care. Both raw quantities are
#' returned separately rather than collapsed into one "count".
#'
#' **This is an aggregate, and it stays one.** The Part B PUF carries no
#' diagnosis field of any kind, so it cannot be split into UI, POP and AI.
#' Allocating it across conditions would manufacture three condition-specific
#' numbers from one, and the canonical blocker needs three *independently
#' estimated* rates.
#'
#' **It is not a first-entry count.** New-patient CPT identifies patients new to
#' a *practice*, not to urogynecologic care. Practice switching and cross-NPI
#' duplication push it up; PUF suppression below 11 beneficiaries and
#' pelvic-floor care outside roster FPMRS push it down. Neither a lower nor an
#' upper bound.
#'
#' @return A one-row tibble with both raw quantities and their provenance.
#' @family diagnostic denominator
#' @concept demand
#' @export
medicare_ffs_practice_new_fpmrs_2023 <- function() {
  tibble::tibble(
    year = 2023L,
    payer_coverage = "medicare_ffs",
    age_band = "65+",
    # Tot_Srvcs summed across cells. The canonical quantity.
    practice_new_fpmrs_services = 79787L,
    # Tot_Benes summed across cells -- NOT distinct people. Kept because it is
    # the raw datum, and because its near-equality with services is exactly the
    # coincidence that previously invited the wrong inference.
    summed_bene_cells = 79785L,
    n_cells = 1322L,
    n_roster_npis_billing = 794L,
    beneficiary_deduplication_possible = FALSE,
    deduplication_blocked_by = "puf_bene_counts_are_within_provider_service_cells",
    numerator_estimand = "practice_new_fpmrs_em_services",
    numerator_source = paste0(
      "medicare_part_b_puf_by_service_2023;hcpcs=99202-99205;",
      "roster=urps_roster_2026-07-22"
    ),
    condition_split_available = FALSE,
    condition_split_blocked_by = "part_b_puf_carries_no_diagnosis_field"
  )
}

#' Named Medicare FFS practice-new service rate, 2023
#'
#' @details
#' Deliberately **not** `per_entering` or `annual_first_urps_entry_rate`. It is
#' a practice-new E/M **service rate** in one payer stratum.
#'
#' Four denominators, ordered from most to least interpretable:
#'
#' \describe{
#'   \item{`all_ffs_women_65plus`}{every woman 65+ in Original Medicare. Assumption
#'     free, but its denominator includes women without Part B while the
#'     numerator can only arise under Part B.}
#'   \item{`all_part_b_female_65plus`}{**the primary interpretable rate.**
#'     Numerator and denominator are on the same coverage footing, and no
#'     disease definition is imposed -- which matters because the numerator
#'     carries no diagnosis. Requires only that the published 65+ Part B share
#'     applies to women.}
#'   \item{`disease_stock_aligned_coverage_unrestricted`}{EXPLORATORY. Imposes
#'     any-PFD prevalence but leaves the denominator coverage-unrestricted, so
#'     numerator and denominator sit on different coverage universes.}
#'   \item{`coverage_aligned_partb_disease`}{EXPLORATORY. Both restrictions at
#'     once.}
#' }
#'
#' **The disease-conditioned rows are exploratory, not estimand-aligned.** The
#' numerator has no diagnosis, so conditioning its denominator on disease
#' assumes every practice-new FPMRS visit arises from a prevalent PFD case --
#' which is neither established nor testable here.
#'
#' On the any-PFD universe: at 65+ the value comes from
#' `mufflyaccess::pfd_prevalence()`, and it is a genuine union rather than a
#' sum -- 0.368 at 65-79 against UI + POP + FI = 0.473 -- consistent with
#' Nygaard's "at least one of UI, FI or POP". It is NOT the
#' `.PFD_PREVALENCE_BY_BAND` constant in `R/data-urps_population.R`, whose
#' "UI + POP combined" comment describes a different local fallback on a
#' different band scheme that is not used at 65+.
#'
#' Every rate here is **services per 1,000 population**, never a per-woman
#' probability: the numerator cannot be deduplicated to people.
#'
#' @param acs_path Path to the ACS population file, for the 65-79/80+ weights.
#' @return A tibble, one row per denominator definition.
#' @family diagnostic denominator
#' @concept demand
#' @export
medicare_ffs_practice_new_fpmrs_ratio_65plus_2023 <- function(
    acs_path = system.file("extdata", "acs5_2023_sex_by_age_state.csv",
                           package = "urpssim")) {
  num <- medicare_ffs_practice_new_fpmrs_2023()
  enr <- cms_original_medicare_enrollment_2023()
  pop_band <- .diagnostic_acs_female_bands(acs_path)
  p_any <- .diagnostic_prevalence_65plus("any_PFD", pop_band)

  d_all   <- enr$female_65plus_ffs
  d_partb <- d_all * enr$part_b_share_65plus
  d_pfd   <- d_all * p_any
  d_both  <- d_partb * p_any
  denoms  <- c(d_all, d_partb, d_pfd, d_both)
  svc <- as.numeric(num$practice_new_fpmrs_services)

  tibble::tibble(
    denominator_definition = c("all_ffs_women_65plus",
                               "all_part_b_female_65plus",
                               "disease_stock_aligned_coverage_unrestricted",
                               "coverage_aligned_partb_disease"),
    year = 2023L,
    payer_coverage = "medicare_ffs",
    age_band = "65+",
    practice_new_fpmrs_services = svc,
    denominator_n = denoms,
    services_per_1000 = 1000 * svc / denoms,
    interpretation = c("crude", "primary", "exploratory", "exploratory"),
    numerator_source = num$numerator_source,
    denominator_source = enr$source,
    numerator_estimand = num$numerator_estimand,
    denominator_estimand = DIAGNOSTIC_UPSTREAM_DENOMINATOR_TAG,
    status = c("OK", "ASSUMPTION", "ASSUMPTION", "ASSUMPTION"),
    assumption = c(
      NA_character_,
      "part_b_share_not_published_by_sex",
      "ffs_age_split_matches_acs;numerator_has_no_diagnosis_so_disease_conditioning_is_unverified",
      "part_b_share_not_published_by_sex;ffs_age_split_matches_acs;numerator_has_no_diagnosis_so_disease_conditioning_is_unverified"
    )
  )
}

#' Diagnostic denominator table: condition x year x age band x payer/coverage
#'
#' @details
#' Populates only what is genuinely derivable and marks everything else `NA`
#' with a machine-readable `missing_reason`. The Medicare FFS denominator now
#' comes from CMS Program Statistics (public, no DUA); it is never manufactured
#' from a national population, a Medicare share, a treated count or a service
#' volume. Condition-level FFS ratios remain `NA` regardless, because the
#' numerator has no diagnosis field.
#'
#' `eligible_prevalent_n` is always `population_n * prevalence * p_eligible` —
#' the upstream stock, with no recognition, seeking, referral or treatment term.
#' Every row declares its `denominator_estimand`, and each is checked by
#' [assert_upstream_denominator()] before the table is returned.
#'
#' @param year Calendar year. Only 2023 has a shipped ACS population file.
#' @param acs_path Path to the ACS 5-year sex-by-age state file.
#' @return A tibble, one row per condition x age band x payer/coverage, plus one
#'   aggregate row carrying the Medicare numerator that has no condition split.
#' @family diagnostic denominator
#' @concept demand
#' @export
build_diagnostic_denominator_table <- function(
    year = 2023L,
    acs_path = system.file("extdata", "acs5_2023_sex_by_age_state.csv",
                           package = "urpssim")) {

  stopifnot(length(year) == 1L)
  if (!identical(as.integer(year), 2023L)) {
    stop("build_diagnostic_denominator_table(): only 2023 is supported; the ",
         "shipped ACS file is acs5_2023. Another year needs its own population ",
         "file rather than an extrapolation.", call. = FALSE)
  }
  if (!nzchar(acs_path) || !file.exists(acs_path)) {
    stop("build_diagnostic_denominator_table(): ACS population file not found. ",
         "Without it population_n cannot be built, and this function will not ",
         "substitute an estimate.", call. = FALSE)
  }

  pop_band <- .diagnostic_acs_female_bands(acs_path)
  enr <- cms_original_medicare_enrollment_2023()

  p_eligible <- lifecourse_eligibility_params()$p_eligible
  conditions <- c(ui = "UI", pop = "POP", ai = "FI")
  bands <- names(.DIAGNOSTIC_ACS_FEMALE_BANDS)
  payers <- c("all_payers", "medicare_ffs")

  rows <- list()
  for (cond in names(conditions)) {
    prev <- pfd_prevalence_by_band(conditions[[cond]])
    for (b in bands) {
      for (pay in payers) {
        # MEDICARE FFS rows are emitted separately below, at 65+, from the CMS
        # enrolment cell. They are never filled from the all-payer population --
        # that would answer a different question while looking like an answer to
        # this one.
        is_ffs <- identical(pay, "medicare_ffs")
        cond_specific <- b %in% c("65-79", "80+")

        # Medicare FFS rows exist only for 65+. Below 65 the FFS population is
        # the disability-eligible cohort, which is a different population with
        # different pelvic-floor epidemiology and is not what the 65+ numerator
        # describes. Emitting under-65 FFS rows produced nine more BLOCKED lines
        # that could never be populated by the CMS enrolment file this table is
        # waiting for -- burying the six rows that are actually pending behind
        # rows that are not pending at all, merely inapplicable.
        if (is_ffs) next   # FFS rows are emitted separately, at 65+ only

        # Past that `next`, is_ffs is always FALSE. The branches below were
        # written when FFS rows came through this loop with NA everywhere; they
        # are now unreachable and are removed rather than left to read as though
        # the FFS case were still handled here.
        population_n <- unname(pop_band[[b]])
        prevalent_n  <- population_n * unname(prev[[b]])
        eligible_n   <- prevalent_n * p_eligible[[cond]]

        reason <- if (!cond_specific) {
          # 20-39 / 40-59 / 60-64 carry the any-PFD value for all three
          # conditions -- identical across ui/pop/ai. Recorded, not hidden.
          "prevalence_not_condition_specific_below_65"
        } else {
          NA_character_
        }
        status <- if (!cond_specific) "PARTIAL" else "OK"

        rows[[length(rows) + 1L]] <- tibble::tibble(
          condition = cond,
          year = as.integer(year),
          age_band = b,
          payer_coverage = pay,
          population_n = population_n,
          prevalent_n = prevalent_n,
          eligible_prevalent_n = eligible_n,
          practice_new_fpmrs_n = NA_real_,
          practice_new_ratio = NA_real_,
          numerator_source = NA_character_,
          denominator_source =
            paste0("acs5_2023_sex_by_age_state;pfd_prevalence_by_band(",
                   conditions[[cond]], ");p_eligible"),
          numerator_estimand = NA_character_,
          denominator_estimand = DIAGNOSTIC_UPSTREAM_DENOMINATOR_TAG,
          status = status,
          missing_reason = reason
        )
      }
    }
  }

  # MEDICARE FFS, 65+ -- now populated. Emitted at 65+ only, and as a single
  # band, because that is the granularity CMS publishes crossed with sex.
  # Splitting it into 65-79/80+ would invent a split the source does not carry,
  # which is the same error as filling it from the all-payer population.
  for (cond in names(conditions)) {
    p65 <- .diagnostic_prevalence_65plus(conditions[[cond]], pop_band)
    prevalent_n <- enr$female_65plus_ffs * p65
    rows[[length(rows) + 1L]] <- tibble::tibble(
      condition = cond,
      year = 2023L,
      age_band = "65+",
      payer_coverage = "medicare_ffs",
      population_n = enr$female_65plus_ffs,
      prevalent_n = prevalent_n,
      eligible_prevalent_n = prevalent_n * p_eligible[[cond]],
      # STILL NA, and this is the load-bearing part. The numerator has no
      # condition split (Part B PUF carries no diagnosis), so a condition-level
      # ratio cannot be formed. The denominator arriving does not change that.
      practice_new_fpmrs_n = NA_real_,
      practice_new_ratio = NA_real_,
      numerator_source = NA_character_,
      denominator_source = paste0(enr$source, ";pfd_prevalence_by_band(",
                                  conditions[[cond]], ");p_eligible"),
      numerator_estimand = NA_character_,
      denominator_estimand = DIAGNOSTIC_UPSTREAM_DENOMINATOR_TAG,
      status = "ASSUMPTION",
      missing_reason = "numerator_has_no_condition_split;denominator_assumes_ffs_age_split_matches_acs"
    )
  }

  # The aggregate Medicare numerator, now with a denominator. One row,
  # condition = NA, because the source cannot split it. It deliberately does
  # NOT join to the condition rows: a join is exactly how 79,787 would end up
  # allocated across three conditions.
  agg <- medicare_ffs_practice_new_fpmrs_2023()
  p_any65 <- .diagnostic_prevalence_65plus("any_PFD", pop_band)
  agg_denom <- enr$female_65plus_ffs * p_any65
  rows[[length(rows) + 1L]] <- tibble::tibble(
    condition = NA_character_,
    year = 2023L,
    age_band = "65+",
    payer_coverage = "medicare_ffs",
    population_n = enr$female_65plus_ffs,
    prevalent_n = agg_denom,
    eligible_prevalent_n = agg_denom,
    practice_new_fpmrs_n = as.numeric(agg$practice_new_fpmrs_services),
    practice_new_ratio = as.numeric(agg$practice_new_fpmrs_services) / agg_denom,
    numerator_source = agg$numerator_source,
    denominator_source = paste0(enr$source, ";pfd_prevalence_by_band(any_PFD)"),
    numerator_estimand = agg$numerator_estimand,
    denominator_estimand = DIAGNOSTIC_UPSTREAM_DENOMINATOR_TAG,
    status = "ASSUMPTION",
    missing_reason = "assumes_ffs_female_65plus_age_split_matches_acs"
  )

  out <- dplyr::bind_rows(rows)

  # Every declared denominator is checked, including the NA-denominator rows:
  # the tag must still be the upstream one, so a future edit cannot introduce a
  # downstream source alongside a populated value.
  for (i in seq_len(nrow(out))) {
    src <- out$denominator_source[[i]]
    if (is.na(src)) {
      if (!identical(out$denominator_estimand[[i]], DIAGNOSTIC_UPSTREAM_DENOMINATOR_TAG)) {
        stop("build_diagnostic_denominator_table(): row ", i,
             " has no denominator_source but a non-upstream estimand tag.",
             call. = FALSE)
      }
      next
    }
    assert_upstream_denominator(out$denominator_estimand[[i]], src,
                               call_context = paste0("diagnostic row ", i))
  }

  attr(out, "diagnostic_only") <- TRUE
  attr(out, "not_a_calibration_input") <- paste(
    "Diagnostic only. Does not feed the production pipeline, does not alter",
    "per_entering, and does not change scientific-readiness, which remains",
    "BLOCKED."
  )
  out
}
