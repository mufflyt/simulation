# Medicare realized care: what was billed, not what was needed ----
#
# WHAT THIS IS AND IS NOT. Medicare Physician & Other Practitioners data reports
# services that were BILLED to fee-for-service Medicare. It is a realized-care
# estimand, and three separate restrictions stand between it and the latent
# all-payer need this package models:
#
#   1. PAYER.    Fee-for-service Medicare only. Medicare Advantage is absent, and
#                so is every commercial, Medicaid, and uninsured encounter.
#   2. ACCESS.   A service appears only if the patient reached a provider who
#                billed for it. Unmet need is invisible by construction -- the
#                people who never got care produce no claim.
#   3. CODING.   Only the mapped procedure codes are counted, and the mapping is
#                deliberately incomplete (see below).
#
# Every row this module emits therefore carries an `estimand` string saying so.
# The comparison function refuses to emit anything named like a latent-demand
# scalar: it returns a CALIBRATION RATIO between two realized quantities, which
# is a statement about the model's reproduction of billing, not about need.
#
# WHY THE DEFAULT CROSSWALK DROPS E/M CODES. The office-visit codes 99202-99215
# are not specific to urogynecology -- a 99213 is a 99213 whoever bills it. A
# crosswalk that included them would sweep in every unrelated visit a
# gynecologist billed and inflate "urogynecologic services" with general office
# work. They are available behind `include_non_specific = TRUE` for a caller who
# has already restricted the claims to a URPS roster, where the ambiguity is
# resolved by the denominator instead.

#' HCPCS crosswalk for urogynecologic services in Medicare claims
#'
#' Derived from `URPS_CPT_BASKET`, the package's existing service-to-CPT SSOT, so
#' the codes this module counts are the same ones the workload model prices.
#' (Written as code rather than a link: the constant is exported but carries no
#' Rd topic, and an unresolvable cross-reference is a `R CMD check` WARNING.)
#'
#' @param include_non_specific Include evaluation-and-management codes (99xxx).
#'   `FALSE` by default: they are not specific to urogynecology and would count
#'   unrelated office visits as urogynecologic services.
#' @return Tibble of `service`, `hcpcs`, `mix`, `specificity`.
#' @export
urps_medicare_service_crosswalk <- function(include_non_specific = FALSE) {
  x <- tibble::as_tibble(URPS_CPT_BASKET)
  x$specificity <- ifelse(grepl("^99", x$hcpcs), "non_specific", "urogynecologic")
  if (!isTRUE(include_non_specific)) {
    x <- x[x$specificity != "non_specific", , drop = FALSE]
  }
  x
}

#' Aggregate Medicare claims into billed urogynecologic services
#'
#' Maps HCPCS codes to services through [urps_medicare_service_crosswalk()] and
#' sums billed service counts. Codes outside the crosswalk are DROPPED, and the
#' share of services they represent is returned as the
#' `unmapped_service_fraction` attribute rather than discarded silently -- a high
#' fraction means the crosswalk is missing the procedures this population
#' actually receives, which is a finding, not a rounding error.
#'
#' @param claims Data frame of Medicare Physician & Other Practitioners rows.
#'   Needs `HCPCS_Cd` and `Tot_Srvcs`; `year`, `Rndrng_Prvdr_State_Abrvtn`,
#'   `Rndrng_Prvdr_Type`, `Place_Of_Srvc`, and `Rndrng_NPI` are used when present.
#' @param include_non_specific Passed to the crosswalk.
#' @return Tibble of `year`, `service`, `state`, `provider_type`,
#'   `place_of_service`, `billed_services`, `billing_npis`, `estimand`, with an
#'   `unmapped_service_fraction` attribute.
#' @export
aggregate_medicare_realized_care <- function(claims,
                                             include_non_specific = FALSE) {
  need <- c("HCPCS_Cd", "Tot_Srvcs")
  missing <- setdiff(need, names(claims))
  if (length(missing)) {
    stop("aggregate_medicare_realized_care(): claims is missing column(s): ",
         paste(missing, collapse = ", "), call. = FALSE)
  }
  claims <- tibble::as_tibble(claims)
  if (!is.numeric(claims$Tot_Srvcs) || any(!is.finite(claims$Tot_Srvcs)) ||
      any(claims$Tot_Srvcs < 0)) {
    stop("aggregate_medicare_realized_care(): Tot_Srvcs must be finite and ",
         "non-negative.", call. = FALSE)
  }

  # Optional dimensions are filled rather than dropped, so the output shape does
  # not depend on which columns a particular extract happened to carry.
  col <- function(nm, default) {
    if (nm %in% names(claims)) as.character(claims[[nm]]) else rep(default, nrow(claims))
  }
  flat <- tibble::tibble(
    year             = if ("year" %in% names(claims)) claims$year else NA_integer_,
    hcpcs            = as.character(claims$HCPCS_Cd),
    services         = as.numeric(claims$Tot_Srvcs),
    state            = col("Rndrng_Prvdr_State_Abrvtn", NA_character_),
    provider_type    = col("Rndrng_Prvdr_Type", NA_character_),
    place_of_service = col("Place_Of_Srvc", NA_character_),
    npi              = col("Rndrng_NPI", NA_character_)
  )

  cw <- urps_medicare_service_crosswalk(include_non_specific)
  flat$service <- cw$service[match(flat$hcpcs, cw$hcpcs)]

  total <- sum(flat$services)
  unmapped <- sum(flat$services[is.na(flat$service)])
  mapped <- flat[!is.na(flat$service), , drop = FALSE]

  out <- mapped %>%
    dplyr::group_by(.data$year, .data$service, .data$state, .data$provider_type,
                    .data$place_of_service) %>%
    dplyr::summarise(
      billed_services = sum(.data$services),
      billing_npis    = dplyr::n_distinct(.data$npi),
      .groups = "drop")

  out$estimand <- paste(
    "Medicare fee-for-service BILLED services (realized care):",
    "not latent all-payer demand, and not unmet need -- a service appears only",
    "if the patient reached a provider who billed for it.")

  attr(out, "unmapped_service_fraction") <-
    if (total > 0) unmapped / total else NA_real_
  attr(out, "include_non_specific") <- isTRUE(include_non_specific)
  out
}

#' Compare modelled service volumes with Medicare realized care
#'
#' Returns a per-service CALIBRATION RATIO, observed over predicted. Deliberately
#' does NOT return a latent-demand scalar: both sides are realized quantities, so
#' their ratio says how well the model reproduces billing under the same access
#' and payer restrictions -- not what share of need is met. Rescaling modelled
#' demand by this ratio would import Medicare's access and payer truncation into
#' an all-payer estimand.
#'
#' @param predicted Data frame of `service` and `predicted_services`.
#' @param observed Data frame of `service` and `billed_services`, typically from
#'   [aggregate_medicare_realized_care()].
#' @return Tibble of `service`, `predicted_services`, `billed_services`,
#'   `calibration_ratio`, `comparison_estimand`.
#' @export
compare_medicare_realized_care <- function(predicted, observed) {
  if (!all(c("service", "predicted_services") %in% names(predicted))) {
    stop("compare_medicare_realized_care(): `predicted` needs `service` and ",
         "`predicted_services`.", call. = FALSE)
  }
  if (!all(c("service", "billed_services") %in% names(observed))) {
    stop("compare_medicare_realized_care(): `observed` needs `service` and ",
         "`billed_services`.", call. = FALSE)
  }

  obs <- observed %>%
    dplyr::group_by(.data$service) %>%
    dplyr::summarise(billed_services = sum(.data$billed_services), .groups = "drop")

  out <- dplyr::left_join(tibble::as_tibble(predicted), obs, by = "service")
  # A zero prediction makes the ratio undefined rather than infinite: reporting
  # Inf would read as "infinitely under-modelled" when it means "no denominator".
  out$calibration_ratio <- ifelse(
    is.finite(out$predicted_services) & out$predicted_services > 0,
    out$billed_services / out$predicted_services, NA_real_)

  out$comparison_estimand <- paste(
    "observed BILLED Medicare FFS services over modelled services:",
    "a calibration ratio between two realized quantities, not a latent",
    "demand scalar -- do not rescale all-payer demand by it.")
  out
}
