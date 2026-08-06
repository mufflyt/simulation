# Tiered supply-capacity reporting ----
#
# A provider COUNT is not the same as CARE AVAILABILITY. This reporting layer
# makes the descent explicit so a reader cannot treat headcount as capacity:
#
#   Tier 1  Active headcount           providers on the roster
#   Tier 2  Clinical FTE               x participation & hours (age/sex)   [attrition]
#   Tier 3  Effective service capacity x productivity -> annual work RVUs  [unit change]
#   Tier 4  Accessible capacity        x geographic reach x insurance      [attrition]
#
# This is a PURE TRANSFORM over quantities the engines already produce -- it does
# not re-run the microsimulation. Tiers 1-2 come from run_workforce_microsimulation()
# (supply_headcount, supply_clinical_fte); the tier 2->3 productivity denominator
# is WRVU_PER_FTE_BENCHMARK / calibrate_wrvu_per_fte() (R/supply-workload_to_fte); the tier 3->4
# geographic reach is summarize_access() / spatial_access_ratio() (R/geography-spatial_access_e2sfca). Nothing
# here duplicates those engines.
#
# The `provider_equivalent` column re-expresses every tier back in provider units
# so the four tiers are directly comparable ("1,000 on the roster -> 780 clinical
# FTE -> 650 accessible-equivalent"); tier 3 keeps its native work-RVU value. The
# 2->3 step is a UNIT change, not attrition, so provider_equivalent is unchanged
# there by design.

#' Tiered supply-capacity hierarchy (headcount -> clinical FTE -> wRVU -> accessible)
#'
#' Assembles the four supply tiers for one group (e.g. a pathway, a year, or the
#' national total) into a tidy table with the between-tier conversion made
#' explicit. Map it over pathways/years and `dplyr::bind_rows()` the results; pass
#' `group` to label each call. Report ABU and ABOG separately (a scope assumption
#' that should not be buried in one pooled stock) by calling once per pathway.
#'
#' @param headcount Active provider headcount (tier 1). Length 1, >= 0.
#' @param clinical_fte Clinical FTE (tier 2), already reflecting participation and
#'   age/sex hours. Length 1, >= 0.
#' @param wrvu_per_fte Annual work RVUs per clinical FTE (tier 2->3 productivity).
#'   Default the median of `WRVU_PER_FTE_BENCHMARK`; prefer a value from
#'   [calibrate_wrvu_per_fte()] solved on a base-year anchor.
#' @param accessible_fraction Share of capacity within geographic reach of the
#'   population (tier 3->4), e.g. from [spatial_access_ratio()]. In [0, 1].
#' @param insurance_fraction Share of capacity accepting the relevant insurance
#'   (tier 3->4). In [0, 1]. Default 1 (no restriction modelled).
#' @param group Optional scalar label stamped on every row for easy binding.
#' @return A 4-row tibble: `group`, `tier` (1-4), `label`, `value`, `unit`,
#'   `provider_equivalent`, `retained_vs_headcount`.
#' @export
supply_capacity_hierarchy <- function(headcount, clinical_fte,
                                      wrvu_per_fte = WRVU_PER_FTE_BENCHMARK[["median"]],
                                      accessible_fraction = 1,
                                      insurance_fraction = 1,
                                      group = NA_character_) {
  if (!is.numeric(headcount) || !is.numeric(clinical_fte) || !is.numeric(wrvu_per_fte))
    stop("supply_capacity_hierarchy(): headcount, clinical_fte, wrvu_per_fte must be numeric.",
         call. = FALSE)
  stopifnot(
    length(headcount) == 1L, length(clinical_fte) == 1L, length(wrvu_per_fte) == 1L,
    is.finite(headcount), is.finite(clinical_fte), is.finite(wrvu_per_fte),
    headcount >= 0, clinical_fte >= 0, wrvu_per_fte > 0,
    is.finite(accessible_fraction), accessible_fraction >= 0, accessible_fraction <= 1,
    is.finite(insurance_fraction),  insurance_fraction  >= 0, insurance_fraction  <= 1
  )

  reach          <- accessible_fraction * insurance_fraction
  wrvu_capacity  <- clinical_fte * wrvu_per_fte
  pe_accessible  <- clinical_fte * reach                     # provider-equivalent, tier 4
  pe             <- c(headcount, clinical_fte, clinical_fte, pe_accessible)

  tibble::tibble(
    group = group,
    tier  = 1:4,
    label = c("active_headcount", "clinical_fte",
              "effective_wrvu_capacity", "accessible_capacity"),
    value = c(headcount, clinical_fte, wrvu_capacity, wrvu_capacity * reach),
    unit  = c("providers", "fte", "annual_wrvu", "annual_wrvu"),
    provider_equivalent   = pe,
    # share of the tier-1 roster still represented, in provider-equivalent terms;
    # NA when there is no roster to divide by.
    retained_vs_headcount = if (headcount > 0) pe / headcount else rep(NA_real_, 4L)
  )
}
