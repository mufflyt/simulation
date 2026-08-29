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
.DIAGNOSTIC_ACS_FEMALE_BANDS <- list(
  "20-39" = sprintf("B01001_%03d", 32:37),
  "40-59" = sprintf("B01001_%03d", 38:41),
  "60-64" = sprintf("B01001_%03d", 42:43),
  "65-79" = sprintf("B01001_%03d", 44:47),
  "80+"   = sprintf("B01001_%03d", 48:49)
)

#' Aggregate Medicare FFS practice-new FPMRS consultations, 2023
#'
#' @details
#' 79,787 consultations, from Medicare Part B PUF `by_service` 2023: HCPCS
#' 99202-99205 billed by 794 NPIs on the URPS/FPMRS roster. `Tot_Srvcs` and
#' `Tot_Benes` agree to within 0.01% at every code, so these are effectively
#' one-per-person.
#'
#' **This is an aggregate, and it stays one.** The Part B PUF carries no
#' diagnosis field of any kind, so the count cannot be split into UI, POP and
#' AI. Allocating it across conditions by prevalence share would manufacture
#' three condition-specific numbers from one, and the canonical blocker needs
#' three *independently estimated* rates.
#'
#' **It is not a first-entry count.** New-patient CPT identifies patients new to
#' a *practice*, not to urogynecologic care. Its biases run in both directions:
#' practice switching overcounts true first entry; PUF suppression of cells
#' below 11 beneficiaries undercounts it; pelvic-floor care delivered outside
#' roster FPMRS is invisible. It is therefore neither a lower nor an upper
#' bound, and must not be described as either.
#'
#' @return A one-row tibble with the count and its provenance.
#' @family diagnostic denominator
#' @concept demand
#' @export
medicare_ffs_practice_new_fpmrs_2023 <- function() {
  tibble::tibble(
    year = 2023L,
    payer_coverage = "medicare_ffs",
    age_band = "65+",
    practice_new_fpmrs_n = 79787L,
    n_roster_npis_billing = 794L,
    numerator_estimand = "practice_new_fpmrs_office_consultations",
    numerator_source = paste0(
      "medicare_part_b_puf_by_service_2023;hcpcs=99202-99205;",
      "roster=urps_roster_2026-07-22"
    ),
    condition_split_available = FALSE,
    condition_split_blocked_by = "part_b_puf_carries_no_diagnosis_field"
  )
}

#' Named Medicare FFS practice-new ratio for 2023 — currently unresolvable
#'
#' @details
#' Deliberately **not** called `per_entering` or `annual_first_urps_entry_rate`.
#' Even once computable it would be a practice-new consultation ratio in one
#' payer stratum, not either canonical parameter.
#'
#' It returns `NA` today because the matched denominator does not exist. It
#' requires the count of **Medicare FFS-enrolled women 65+ with the condition**,
#' and nothing on hand supplies it:
#'
#' - MCBS 2022 is a weighted survey PUF for the wrong year, needing its own
#'   transport argument;
#' - Part B PUF beneficiary counts are **users**, so using them would make the
#'   denominator a function of the numerator — the one genuine circularity
#'   available here;
#' - no CMS Medicare enrollment file is present.
#'
#' Returning `NA` with a machine-readable reason is the point. A national 65+
#' denominator would silently answer a different question.
#'
#' @return A one-row tibble; `ratio` is `NA_real_` until CMS enrollment data
#'   exist.
#' @family diagnostic denominator
#' @concept demand
#' @export
medicare_ffs_practice_new_fpmrs_ratio_65plus_2023 <- function() {
  num <- medicare_ffs_practice_new_fpmrs_2023()
  tibble::tibble(
    year = 2023L,
    payer_coverage = "medicare_ffs",
    age_band = "65+",
    practice_new_fpmrs_n = num$practice_new_fpmrs_n,
    eligible_prevalent_n = NA_real_,
    ratio = NA_real_,
    numerator_source = num$numerator_source,
    denominator_source = NA_character_,
    status = "BLOCKED",
    missing_reason = "missing_cms_ffs_enrollment_denominator"
  )
}

#' Diagnostic denominator table: condition x year x age band x payer/coverage
#'
#' @details
#' Populates only what is genuinely derivable and marks everything else `NA`
#' with a machine-readable `missing_reason`. In particular the Medicare FFS
#' stratum's denominator stays empty rather than being manufactured from a
#' national population, a Medicare share, a treated count or a service volume.
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

  acs <- utils::read.csv(acs_path, colClasses = "character",
                         stringsAsFactors = FALSE)
  acs$estimate <- suppressWarnings(as.numeric(acs$estimate))
  pop_band <- vapply(.DIAGNOSTIC_ACS_FEMALE_BANDS, function(vars) {
    sum(acs$estimate[acs$variable %in% vars], na.rm = TRUE)
  }, numeric(1))

  p_eligible <- lifecourse_eligibility_params()$p_eligible
  conditions <- c(ui = "UI", pop = "POP", ai = "FI")
  bands <- names(.DIAGNOSTIC_ACS_FEMALE_BANDS)
  payers <- c("all_payers", "medicare_ffs")

  rows <- list()
  for (cond in names(conditions)) {
    prev <- pfd_prevalence_by_band(conditions[[cond]])
    for (b in bands) {
      for (pay in payers) {
        # MEDICARE FFS: no enrolment denominator exists. Everything downstream
        # of population_n therefore stays NA. It is NOT filled from the
        # all-payer population -- that would answer a different question while
        # looking like an answer to this one.
        is_ffs <- identical(pay, "medicare_ffs")
        cond_specific <- b %in% c("65-79", "80+")

        # Medicare FFS rows exist only for 65+. Below 65 the FFS population is
        # the disability-eligible cohort, which is a different population with
        # different pelvic-floor epidemiology and is not what the 65+ numerator
        # describes. Emitting under-65 FFS rows produced nine more BLOCKED lines
        # that could never be populated by the CMS enrolment file this table is
        # waiting for -- burying the six rows that are actually pending behind
        # rows that are not pending at all, merely inapplicable.
        if (is_ffs && !cond_specific) next

        population_n <- if (is_ffs) NA_real_ else unname(pop_band[[b]])
        prevalence   <- unname(prev[[b]])
        prevalent_n  <- if (is.na(population_n)) NA_real_ else population_n * prevalence
        eligible_n   <- if (is.na(prevalent_n)) NA_real_ else
          prevalent_n * p_eligible[[cond]]

        reason <- if (is_ffs) {
          "missing_cms_ffs_enrollment_denominator"
        } else if (!cond_specific) {
          # 20-39 / 40-59 / 60-64 carry the any-PFD value for all three
          # conditions -- identical across ui/pop/ai. Recorded, not hidden.
          "prevalence_not_condition_specific_below_65"
        } else {
          NA_character_
        }
        status <- if (is_ffs) "BLOCKED" else if (!cond_specific) "PARTIAL" else "OK"

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
          denominator_source = if (is_ffs) NA_character_ else
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

  # The aggregate Medicare numerator. One row, condition = NA, because the
  # source cannot split it -- see medicare_ffs_practice_new_fpmrs_2023(). It
  # deliberately does NOT join to the condition rows above: a join is exactly
  # how 79,787 would end up allocated across three conditions.
  agg <- medicare_ffs_practice_new_fpmrs_2023()
  rows[[length(rows) + 1L]] <- tibble::tibble(
    condition = NA_character_,
    year = 2023L,
    age_band = "65+",
    payer_coverage = "medicare_ffs",
    population_n = NA_real_,
    prevalent_n = NA_real_,
    eligible_prevalent_n = NA_real_,
    practice_new_fpmrs_n = as.numeric(agg$practice_new_fpmrs_n),
    practice_new_ratio = NA_real_,
    numerator_source = agg$numerator_source,
    denominator_source = NA_character_,
    numerator_estimand = agg$numerator_estimand,
    denominator_estimand = DIAGNOSTIC_UPSTREAM_DENOMINATOR_TAG,
    status = "BLOCKED",
    missing_reason = "missing_cms_ffs_enrollment_denominator"
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
