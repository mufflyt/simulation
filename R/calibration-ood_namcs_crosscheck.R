################################################################################
# R/calibration-ood_namcs_crosscheck.R
# Compares chia_casemix.ood_urogynecology_service_volume_2004_2018-derived
# payer shares (per docs/superpowers/plans/2026-08-28-chia-ood-outpatient-urps-service-events.md,
# Step 5) against namcs_urps_payer_mix()'s national ambulatory shares.
#
# REPORT, NOT GATE: OOD is a hospital-based observation-status sample; NAMCS
# is a national ambulatory-visit survey. They are expected to disagree --
# OOD structurally cannot see the office-based volume NAMCS captures, and
# NAMCS cannot see anything about which hospital-affiliated subset OOD
# selects. This function's job is to make the comparison visible, not to
# reconcile it. Per this session's own newly-adopted meta-rule (an audit/
# comparison result capable of changing the study's frame needs independent
# confirmation before being trusted): read the output, do not conclude
# either source is "wrong" from a raw disagreement alone.
################################################################################

# OOD's payer_group vocabulary (Medicare/Medicaid/Commercial/Other-Public,
# see R/data-chia_ood_urogynecology_service_events.R) is NOT the same set as
# NAMCS's payer_tier vocabulary (Medicare/Medicaid/Private/Uninsured, see
# namcs_urps_payer_mix()). Two real, irreducible mismatches, not just a
# naming difference:
#   - OOD has no Self-pay/Uninsured code at all (documented in
#     .chia_ood_classify_source_of_payment()'s roxygen) -- NAMCS's
#     "Uninsured" tier has no OOD counterpart to compare against.
#   - OOD's "Other/Public" (Worker's Comp, CommCare, Free Care, etc.) has no
#     NAMCS "payer_tier" counterpart either.
# The label map below only aligns the two tiers that DO have a defensible
# correspondence (Medicare<->Medicare, Medicaid<->Medicaid,
# Commercial<->Private); "Other/Public" and "Uninsured" are reported
# separately, never forced into alignment.
.OOD_TO_NAMCS_PAYER_LABEL <- c(
  "Medicare" = "Medicare", "Medicaid" = "Medicaid", "Commercial" = "Private"
)

#' Compare OOD-derived payer shares to NAMCS ambulatory payer shares
#'
#' @param ood_volume A tibble in `ood_urogynecology_service_volume_2004_2018`'s
#'   schema (`year, service, payer_group, setting, service_events`), e.g. read
#'   via `DBI::dbReadTable()`.
#' @param namcs_mix Output of [namcs_urps_payer_mix()]. Default calls it with
#'   no arguments (reads the vendored aggregate if the source-tree-only
#'   microdata isn't available).
#' @return A tibble: `payer_tier`, `ood_share` (OOD's share among the three
#'   directly-comparable tiers only, i.e. excluding `Other/Public` and
#'   `Unknown`), `namcs_share` (NAMCS's share among Medicare/Medicaid/Private
#'   only, i.e. excluding `Uninsured`), `abs_diff`. Attribute `not_comparable`
#'   lists the OOD `Other/Public` and `Unknown` shares and the NAMCS
#'   `Uninsured` share separately, since none of those have a counterpart in
#'   the other source.
#' @family chia physician attribution
#' @concept supply
#' @export
compare_ood_to_namcs_service_shares <- function(
    ood_volume, namcs_mix = namcs_urps_payer_mix()) {
  ood_totals <- ood_volume |>
    dplyr::group_by(.data$payer_group) |>
    dplyr::summarise(n = sum(.data$service_events), .groups = "drop")

  .share_of <- function(group) {
    n <- ood_totals$n[ood_totals$payer_group == group]
    (if (length(n) == 0) 0 else n) / sum(ood_totals$n)
  }
  ood_other_public_share <- .share_of("Other/Public")
  ood_unknown_share <- .share_of("Unknown")

  ood_comparable <- ood_totals |>
    dplyr::filter(.data$payer_group %in% names(.OOD_TO_NAMCS_PAYER_LABEL)) |>
    dplyr::mutate(payer_tier = unname(.OOD_TO_NAMCS_PAYER_LABEL[.data$payer_group])) |>
    dplyr::mutate(ood_share = .data$n / sum(.data$n)) |>
    dplyr::select("payer_tier", "ood_share")

  namcs_uninsured_share <- namcs_mix$share[namcs_mix$payer_tier == "Uninsured"] |>
    (\(x) if (length(x) == 0) 0 else x)()
  namcs_comparable <- namcs_mix |>
    dplyr::filter(.data$payer_tier != "Uninsured") |>
    dplyr::mutate(namcs_share = .data$share / sum(.data$share)) |>
    dplyr::select("payer_tier", "namcs_share")

  out <- dplyr::full_join(ood_comparable, namcs_comparable, by = "payer_tier") |>
    dplyr::mutate(
      ood_share = tidyr::replace_na(.data$ood_share, 0),
      namcs_share = tidyr::replace_na(.data$namcs_share, 0),
      abs_diff = abs(.data$ood_share - .data$namcs_share)
    )

  attr(out, "not_comparable") <- tibble::tibble(
    source = c("OOD", "OOD", "NAMCS"),
    tier = c("Other/Public", "Unknown", "Uninsured"),
    share_of_that_source = c(ood_other_public_share, ood_unknown_share, namcs_uninsured_share)
  )
  out
}
