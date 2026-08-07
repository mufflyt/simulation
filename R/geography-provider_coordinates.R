# Provider Point Locations ----
#
# The first of the three inputs `docs/DEMAND_METHODS.md` names for production
# geographic access, and the first step of the ordering that
# `geographic_access_status()` insists on: coordinates BEFORE isochrones, and
# isochrones before the access layer is wired.
#
# COVERAGE IS THE HEADLINE, NOT THE COUNT. The geocoded source covers 1,176 URPS
# providers, and joins to 964 of the 1,339 in the model baseline -- 72%. That
# number alone would be reassuring and misleading, because the missingness is
# not random:
#
#     ABOG (OB/GYN)   1,031 roster  ->  964 geocoded  (93.5%)
#     ABU  (urology)    308 roster  ->  268 geocoded  (87.0%)   [was 0.0%]
#
# The ABU pathway was at ZERO until a second source was found: the same repo
# geocodes it separately in data/abu_urology/, which the primary run does not
# include. Merging it moved overall coverage 72% -> 92% and closed the hole.
# What remains is a roughly uniform ~8% shortfall rather than a missing
# pathway, which is a different and much less damaging kind of gap.
#
# The geocoding run covered the ABOG pathway only. Every urology-pathway URPS
# provider is absent -- 23% of the workforce, concentrated in whichever markets
# they serve. An access surface built on this would run, produce entirely
# plausible ratios, and understate access wherever ABU providers practise. That
# is the ordering trap in its second form: not wrong geometry, but a wrong
# denominator that looks complete.
#
# Coverage by certification era is the same story from another angle: 75% for
# 2015 and earlier, 23% for 2021 onward, because the run predates them.
#
# So this module reports the coordinates AND refuses to let the coverage gap
# travel separately from them.

# Minimum share of the model baseline that must carry a coordinate before the
# access layer may be built. Not a statistical threshold -- a judgement that a
# surface missing more than one provider in twenty is not an access surface.
COORD_COVERAGE_MIN <- 0.95

#' Load URPS provider point locations
#'
#' Physician names are excluded, as in [load_urps_roster()]: the access
#' calculation needs a point and an identifier, never a name.
#'
#' @param path CSV path.
#' @return Tibble with `npi`, `lat`, `lon`, and provenance columns.
#' @export
load_urps_provider_coordinates <- function(
    path = "data-raw/urps_roster/urps_provider_coordinates_2026-08-02.csv") {
  if (!file.exists(path)) {
    stop("Provider coordinates not found at '", path, "'. They are derived in ",
         "mufflyt/isochrones (artifacts/<run>/step_2.3_providers_geocoded_",
         "with_retirement.csv), filtered to URPS and stripped of names.",
         call. = FALSE)
  }
  d <- utils::read.csv(path, colClasses = c(npi = "character"), stringsAsFactors = FALSE)
  # Provenance must survive the load, not just the coordinates. Merging several
  # geocoding runs with rbind coerced `retrieved_on` to Date and silently NA'd
  # 364 of 1,540 rows; source_run and the points were untouched, so nothing
  # downstream would have noticed a quarter of the file losing its date.
  for (col in c("source_run", "retrieved_on")) {
    if (!col %in% names(d)) stop("provider coordinates are missing the `", col,
                                 "` provenance column.", call. = FALSE)
    n_bad <- sum(is.na(d[[col]]) | !nzchar(as.character(d[[col]])))
    if (n_bad > 0) {
      stop(sprintf(paste("%d of %d coordinate rows have no `%s`. A point whose",
                         "origin is unrecorded cannot be audited; repair the",
                         "extract rather than loading it."),
                   n_bad, nrow(d), col), call. = FALSE)
    }
  }

  bad <- !is.finite(d$lat) | !is.finite(d$lon) |
    d$lat < 17 | d$lat > 72 | d$lon < -180 | d$lon > -65
  if (any(bad)) {
    .msg_warn(sprintf(paste("%d coordinate row(s) fall outside plausible US",
                            "bounds and are dropped."), sum(bad)))
    d <- d[!bad, , drop = FALSE]
  }
  tibble::as_tibble(d)
}

#' Coordinate coverage of the model baseline, by pathway
#'
#' The number that decides whether the access layer may be wired. A share is not
#' enough on its own: coverage that is 93% in one pathway and 0% in another is
#' not 72% coverage, it is a missing pathway.
#'
#' @param roster Roster tibble; loaded when NULL.
#' @param coords Coordinate tibble; loaded when NULL.
#' @return List with overall and per-pathway coverage, and `usable_for_access`.
#' @export
provider_coordinate_coverage <- function(roster = NULL, coords = NULL) {
  if (is.null(roster)) roster <- load_urps_roster()
  if (is.null(coords)) coords <- load_urps_provider_coordinates()

  roster$has_coord <- roster$npi %in% coords$npi
  by_path <- stats::aggregate(has_coord ~ pathway, roster, function(x)
    c(n = length(x), with_coord = sum(x)))
  tab <- data.frame(pathway = by_path$pathway,
                    n = by_path$has_coord[, "n"],
                    with_coord = by_path$has_coord[, "with_coord"],
                    stringsAsFactors = FALSE)
  tab$share <- tab$with_coord / tab$n

  # A pathway at zero is a structural hole, not a low rate. Any threshold on the
  # OVERALL share alone would pass this dataset at 72%.
  empty <- tab$pathway[tab$with_coord == 0]

  list(
    n_roster = nrow(roster),
    n_with_coordinates = sum(roster$has_coord),
    overall_share = mean(roster$has_coord),
    by_pathway = tab,
    pathways_absent = empty,
    usable_for_access = length(empty) == 0 && mean(roster$has_coord) >= COORD_COVERAGE_MIN,
    # The blocker must name WHICHEVER condition failed. Reporting
    # usable_for_access = FALSE with blocker = NA -- which this did while the
    # pathway hole was closed but the overall share sat below threshold -- tells
    # a caller they are blocked and not why.
    blocker = if (length(empty)) {
      sprintf(paste("pathway(s) %s have NO geocoded provider, so an access surface",
                    "built on these coordinates omits them entirely and understates",
                    "access wherever they practise"), paste(empty, collapse = ", "))
    } else if (mean(roster$has_coord) < COORD_COVERAGE_MIN) {
      sprintf(paste("coverage is %.1f%% against a %.0f%% floor; the gap is spread",
                    "across pathways rather than concentrated in one, so it",
                    "understates access roughly uniformly rather than in a",
                    "particular market"),
              100 * mean(roster$has_coord), 100 * COORD_COVERAGE_MIN)
    } else NA_character_
  )
}
