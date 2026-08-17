# Provider Point Locations ----
#
# The first of the three inputs `docs/DEMAND_METHODS.md` names for production
# geographic access, and the first step of the ordering that
# `geographic_access_status()` insists on: coordinates BEFORE isochrones, and
# isochrones before the access layer is wired.
#
# COVERAGE IS THE HEADLINE, NOT THE COUNT. The primary geocoding run covers
# 1,176 URPS providers and joins to 964 of the 1,339 in the model baseline --
# 72%. That number alone would be reassuring and misleading, because the
# missingness was not random. Five merges later:
#
#                      roster   geocoded    share
#     ABOG (OB/GYN)     1,031      1,030    99.9%     (from 93.5%)
#     ABU  (urology)      308        306    99.4%     (from  0.0%)
#     overall           1,339      1,336    99.8%     (from 72.0%)
#
# The ABU pathway was at ZERO until a second source was found: the same repo
# geocodes it separately in data/abu_urology/, which the primary run does not
# include. That single merge moved overall coverage 72% -> 92% and closed the
# hole. An access surface built before it would have run, produced entirely
# plausible ratios, and understated access wherever ABU providers practise --
# the ordering trap in its second form: not wrong geometry, but a wrong
# denominator that looks complete.
#
# So this module reports the coordinates AND refuses to let the coverage gap
# travel separately from them.
#
# TWO WAYS A MERGE GOES WRONG, and they need different guards:
#
#   * The bind damages a column nobody computes with -- see safe_rbind().
#   * The bind is clean and a POINT IS WRONG -- see screen_new_coordinates().
#     One candidate recovered for the last 15 sat 131 km from its own recorded
#     address, in the wrong state, with finite coordinates inside the US box.
#     Every structural check passed it.

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
#' @family provider coordinates
#' @concept geography
#' @export
load_urps_provider_coordinates <- function(
    path = "data-raw/urps_roster/urps_provider_coordinates_2026-08-02.csv") {
  if (!file.exists(path)) {
    iso_dir <- tryCatch(isochrone_source_dir(), error = function(e) NA_character_)
    candidate_paths <- c(
      file.path(getwd(), "..", "isochrones", "data", "derived", "canonical_abog_npi_LATEST.csv"),
      file.path("/Users/tmuffly/isochrones", "data", "derived", "canonical_abog_npi_LATEST.csv"),
      if (!is.na(iso_dir)) file.path(iso_dir, "canonical_abog_npi_LATEST.csv") else NULL
    )
    found <- candidate_paths[!is.null(candidate_paths) & file.exists(candidate_paths)]
    if (length(found) > 0) {
      path <- found[1]
    } else {
      stop("Provider coordinates not found at '", path, "'. They are derived in ",
           "mufflyt/isochrones (artifacts/<run>/step_2.3_providers_geocoded_",
           "with_retirement.csv), filtered to URPS and stripped of names.",
           call. = FALSE)
    }
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
#' @family provider coordinates
#' @concept geography
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

# ---- Merging geocoding runs without losing columns --------------------------

#' Row-bind tables while refusing silent coercion
#'
#' THE DEFECT THIS EXISTS FOR, observed rather than imagined. Merging five
#' geocoding runs with `rbind()` coerced `retrieved_on` from character to Date
#' because one input had parsed it as a date, and silently NA'd 364 of 1,540
#' rows. The coordinates and `source_run` were untouched, so every downstream
#' number stayed correct while a quarter of the file lost its provenance. The
#' repair attempt then did it again in reverse -- assigning a character into the
#' now-Date column NA'd all 1,540.
#'
#' Nothing about that is specific to coordinates. Any rbind of independently
#' read CSVs can do it, and the damage is invisible precisely because it lands
#' in a column nobody computes with.
#'
#' The rule: a column that was complete in every input must be complete in the
#' output, and a type change that loses information is an error rather than a
#' coercion.
#'
#' @param parts List of data frames to bind.
#' @param no_new_missing Columns in which the bind must not CREATE missing
#'   values. An NA already present in an input is data and passes; an NA that
#'   appears only after binding is coercion damage and errors. Named for what it
#'   guarantees: it is not a completeness check, and the loader does that
#'   separately.
#' @return A single data frame.
#' @keywords internal
safe_rbind <- function(parts, no_new_missing = character()) {
  parts <- Filter(function(p) !is.null(p) && nrow(p) > 0, parts)
  if (!length(parts)) stop("safe_rbind(): nothing to bind.", call. = FALSE)

  # Harmonise to character BEFORE binding for any column whose class differs
  # across inputs. Coercing deliberately, in one direction, beats letting rbind
  # pick -- rbind's choice depends on argument order, so the same merge in a
  # different order silently produces different NAs.
  all_cols <- unique(unlist(lapply(parts, names)))
  for (col in all_cols) {
    classes <- unique(unlist(lapply(parts, function(p)
      if (col %in% names(p)) class(p[[col]])[1] else NULL)))
    if (length(classes) > 1L) {
      for (i in seq_along(parts)) {
        if (col %in% names(parts[[i]])) parts[[i]][[col]] <- as.character(parts[[i]][[col]])
      }
    }
  }

  before <- vapply(all_cols, function(col) {
    vals <- unlist(lapply(parts, function(p) if (col %in% names(p)) p[[col]] else NULL))
    if (is.null(vals)) 0L else sum(is.na(vals))
  }, integer(1))

  out <- dplyr::bind_rows(parts)

  for (col in intersect(no_new_missing, names(out))) {
    n_bad <- sum(is.na(out[[col]]) | !nzchar(as.character(out[[col]])))
    if (n_bad > before[[col]]) {
      stop(sprintf(paste("safe_rbind(): binding introduced %d missing value(s)",
                         "in `%s` (%d before, %d after). That is a silent type",
                         "coercion, not data: harmonise the column across inputs",
                         "before binding."),
                   n_bad - before[[col]], col, before[[col]], n_bad), call. = FALSE)
    }
  }
  out
}

# ---- Screening a candidate point against its own recorded address -----------

# How far a geocoded point may sit from the centroid of the ZIP its own source
# recorded before the point stops being evidence about that address. ZIP
# centroids are coarse and rural ZIPs are large, so this is deliberately loose:
# it is built to catch a point in the wrong CITY, not to audit precision. The
# observed separation was total -- eleven good candidates within 7.4 km, the bad
# one at 130.7 km -- so nothing in the decision turns on where in that gap the
# threshold sits.
COORD_ADDRESS_MAX_KM <- 25

#' Screen candidate coordinates against the address their source recorded
#'
#' THE DEFECT THIS EXISTS FOR. Recovering the last 15 uncoordinated providers
#' turned up 13 candidates in other repositories. Twelve were right. One --
#' recorded address Glen Dale, WV 26038 -- was geocoded to 40.95, -81.54, which
#' is Ohio, 131 km away. It had finite coordinates inside the US bounding box,
#' a valid NPI, complete provenance, and a real address. [safe_rbind()] would
#' have bound it without complaint and the loader would have accepted it,
#' because nothing about it is structurally wrong. It was the only candidate
#' matched on name rather than identifier.
#'
#' A wrong point does not announce itself downstream either: it moves one
#' provider between markets, which changes an access surface by an amount no
#' summary statistic would flag.
#'
#' DO NOT SUBSTITUTE THE `state` COLUMN FOR THIS CHECK. It is the certifying
#' board's mailing state, and in the source that carries both, 1,481 of 7,208
#' physicians (20.5%) practise in a different state from the one they receive
#' mail in. Screening on state agreement rejects roughly one correct point in
#' eight -- and the temptation, once it fires, is to loosen the check or edit
#' the state rather than conclude the check was wrong.
#'
#' @param new Data frame of candidate rows, with `lat`, `lon` and the ZIP its
#'   source recorded for the same provider.
#' @param zip_col Name of the ZIP column in `new`.
#' @param max_km Rejection threshold; see `COORD_ADDRESS_MAX_KM`.
#' @return `new` with `address_km` and `address_ok` columns added. Rows whose
#'   ZIP is unknown get `NA` distance and are NOT marked ok: an unverifiable
#'   point is not a verified one.
#' @keywords internal
screen_new_coordinates <- function(new, zip_col = "zip5",
                                   max_km = COORD_ADDRESS_MAX_KM) {
  if (!zip_col %in% names(new)) {
    stop("screen_new_coordinates(): `new` has no `", zip_col, "` column. A ",
         "candidate point with no recorded address cannot be screened against ",
         "one; source it from a run that carries the address.", call. = FALSE)
  }
  if (!requireNamespace("zipcodeR", quietly = TRUE)) {
    stop("screen_new_coordinates() needs the zipcodeR package for ZIP ",
         "centroids. It is a Suggests: install it before merging new ",
         "coordinates, rather than merging them unscreened.", call. = FALSE)
  }
  z <- zipcodeR::zip_code_db
  z <- z[!is.na(z$lat) & !is.na(z$lng), c("zipcode", "lat", "lng")]
  names(z) <- c(".zip", ".zlat", ".zlon")

  new$.zip <- sprintf("%05d", suppressWarnings(as.integer(new[[zip_col]])))
  idx <- match(new$.zip, z$.zip)
  new$address_km <- haversine_km(new$lat, new$lon, z$.zlat[idx], z$.zlon[idx])
  # NA distance is a FAILED screen, not a passed one.
  new$address_ok <- !is.na(new$address_km) & new$address_km <= max_km
  new$.zip <- NULL
  new
}
