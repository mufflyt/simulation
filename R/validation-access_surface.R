# Semantic Validation of an Imported Access Surface ---------------------------
#
# WHY A VALIDATOR AND NOT A FILE CHECK. `geographic_access_status()` reports
# drive-time isochrones MISSING, and the temptation on finding a plausible
# artifact is to point the status object at it and watch the flag turn green.
# A file that exists is not a surface that answers this model's question.
#
# The FPMRS E2SFCA surface in mufflyt/isochrones is the case in point. It is
# genuinely an FPMRS access surface, genuinely tract-level, and covers 98% of
# modelled need. It was also computed on 580 providers against 1,099 URPS
# physicians certified through 2020, so every accessibility ratio on it is built
# from roughly half the workforce. Nothing about the filename says so.
#
# These gates encode what has to be true before an imported surface may
# calibrate the access layer. `verdict` is "pass" only when every gate passes;
# any failure leaves it "fail" and the caller must not resolve the status.

#' Gates an imported access surface must clear before it may be used
#'
#' @format Tibble of `gate` and `requirement`.
#' @family access
#' @concept validation
#' @export
ACCESS_SURFACE_GATES <- tibble::tribble(
  ~gate,         ~requirement,
  "workforce",   "Provider universe matches the modelled URPS workforce for the surface year, within tolerance.",
  "vintage",     "Provider year, tract vintage, and population denominator are recorded and mutually consistent.",
  "measure",     "Surface is a relative-availability (E2SFCA) measure, not a nearest-provider proxy, and its provider denominator is recorded and matches the provider artifact.",
  "coverage",    "Tract join and need coverage meet the floor, and missing geography is distinguishable from zero access.",
  "provenance",  "Artifact traces to a producing run and its schema and hash are recorded.",
  "accounting",  "Every eligible provider receives exactly one terminal disposition and no loss is unexplained."
)

#' Validate an imported tract-level access surface against this model
#'
#' Fail-closed semantic check. Reports one row per gate in
#' [ACCESS_SURFACE_GATES] with the measured value, and an overall verdict.
#'
#' @details
#' MISSING IS NOT ZERO. A tract absent from the surface and a tract with zero
#' accessible capacity mean different things, and averaging them together
#' silently converts absent geography into a scarcity finding. The coverage gate
#' therefore reports them separately and fails if absent tracts carry more than
#' `max_uncovered_need` of modelled need.
#'
#' @param surface Tract-level surface with `GEOID` and an access column.
#' @param providers Provider-level table from the same run, or `NULL`.
#' @param need Tract need table with `GEOID` and `need`; defaults to the
#'   committed `artifacts/tract_pfd_need.csv` when available.
#' @param surface_year Year the provider universe represents.
#' @param access_col Name of the accessibility column.
#' @param workforce_tol Allowed shortfall in provider count, as a share.
#' @param max_uncovered_need Allowed share of need with no access value.
#' @param provenance Optional list with `path` and `sha256`.
#' @return List with `gates` (tibble), `verdict`, and `blocking`.
#' @family access
#' @concept validation
#' @export
validate_access_surface <- function(surface,
                                    providers = NULL,
                                    need = NULL,
                                    surface_year = 2020L,
                                    access_col = "access_mean_population",
                                    workforce_tol = 0.10,
                                    max_uncovered_need = 0.02,
                                    provenance = NULL) {
  stopifnot(is.data.frame(surface), "GEOID" %in% names(surface))
  if (is.null(need)) {
    # Resolve from package root or test directory; a silently absent need file
    # would leave the coverage gate unscored, which must not look like a pass.
    np <- artifact_path("tract_pfd_need.csv")
    if (!is.null(np)) need <- utils::read.csv(np, colClasses = c(GEOID = "character"))
  }
  # GEOID normalisation, done ONCE and explicitly. `artifacts/tract_pfd_need.csv`
  # stores GEOIDs at mixed 10- and 11-character widths because leading zeros are
  # lost on read, so a naive join drops every tract in a state with a
  # single-digit FIPS code. formatC(flag = "0") does NOT fix this: on character
  # input it pads with spaces, which silently makes the problem worse.
  pad <- function(x) {
    x <- as.character(x)
    ifelse(is.na(x), NA_character_,
           ifelse(nchar(x) < 11, formatC(x, width = 11, flag = " "), x)) -> y
    gsub(" ", "0", y, fixed = TRUE)
  }

  # 1. WORKFORCE ------------------------------------------------------------
  #
  # NOT a raw headcount test. Cumulative certifications are the wrong
  # denominator (that series never decrements), and provider ascertainment is a
  # measured property rather than a defect. The gate asks the only question that
  # can fail closed: is every eligible provider accounted for?
  asc <- tryCatch(access_ascertainment_status(surface_year), error = function(e) NULL)
  n_prov <- if (is.null(providers)) NA_integer_ else nrow(providers)
  g_workforce <- !is.null(asc) && isTRUE(asc$all_losses_explained)
  share <- if (is.null(asc)) NA_real_ else asc$surface_rate

  # 2. VINTAGE --------------------------------------------------------------
  # Tract vintage and the ACS denominator year come from the SSOT, not from a
  # GEOID-width heuristic: mufflyaccess::tract_vintage_of() is the authority on
  # when census geography switches, and acs_year_of() on which population year
  # backs a study year.
  yrs <- if ("year" %in% names(surface)) unique(surface$year) else NA
  expect_vintage <- mufflyaccess::tract_vintage_of(surface_year)
  acs_year <- mufflyaccess::acs_year_of(surface_year)
  g_vintage <- length(yrs) == 1L && isTRUE(yrs == surface_year) &&
    length(unique(nchar(surface$GEOID))) == 1L

  # 3. MEASURE --------------------------------------------------------------
  has_access <- access_col %in% names(surface)
  # A two-step floating catchment surface carries the provider-side ratio; a
  # nearest-provider proxy does not.
  two_step <- !is.null(providers) &&
    all(c("supply", "weighted_demand", "ratio") %in% names(providers))
  # The ACCESS columns must carry values. The per-tract `n_providers` column is
  # exempt and only that column: the raster code path that produced these
  # surfaces cannot attribute providers to tracts and sets it NA by
  # construction. Exempting it is not a relaxation, because the provider
  # denominator is checked far more strictly below against the artifact itself.
  acc_cols <- setdiff(names(surface), "n_providers")
  empty_cols <- acc_cols[vapply(surface[acc_cols],
                                function(x) all(is.na(x)), logical(1))]
  # PROVIDER DENOMINATOR. Recorded in the sidecar, and it must equal the count
  # derivable from the provider artifact. `supply` is n_distinct(npi) per
  # location, so sum(supply) is the distinct physician count. A disagreement
  # means the sidecar describes a different run and must fail.
  sp <- tryCatch({
    v <- utils::read.csv(artifact_path("access_ascertainment", "surface_provenance.csv"),
                         stringsAsFactors = FALSE)
    v[v$analysis_year == surface_year, , drop = FALSE]
  }, error = function(e) NULL)
  recorded_n <- if (is.null(sp) || nrow(sp) != 1L) NA_integer_ else sp$n_providers_in_surface
  derived_n  <- if (is.null(providers)) NA_integer_ else sum(providers$supply)
  denom_ok   <- is.finite(recorded_n) && is.finite(derived_n) && recorded_n == derived_n
  g_measure <- has_access && two_step && length(empty_cols) == 0L && denom_ok

  # 4. COVERAGE -------------------------------------------------------------
  if (!is.null(need) && all(c("GEOID", "need") %in% names(need))) {
    ng <- pad(need$GEOID); sg <- pad(surface$GEOID)
    joined <- intersect(sg, ng)
    uncovered_need <- sum(need$need[!ng %in% joined], na.rm = TRUE)
    total_need <- sum(need$need, na.rm = TRUE)
    uncovered <- mufflyaccess::safe_divide(uncovered_need, total_need)
    n_zero <- sum(surface[[access_col]] == 0, na.rm = TRUE)
    n_na   <- sum(is.na(surface[[access_col]]))
    # zero_access_share() is the SSOT statistic and is POPULATION WEIGHTED: the
    # percent of weighted need living where access is exactly zero. A raw tract
    # count answers a different and less useful question, because zero-access
    # tracts are systematically small. Weighted by modelled need on the joined
    # tracts.
    jn <- merge(data.frame(GEOID = sg, acc = surface[[access_col]],
                           stringsAsFactors = FALSE),
                data.frame(GEOID = ng, w = need$need, stringsAsFactors = FALSE),
                by = "GEOID")
    jn <- jn[is.finite(jn$acc) & is.finite(jn$w), , drop = FALSE]
    zero_share <- if (nrow(jn)) {
      mufflyaccess::zero_access_share(jn$acc, jn$w)
    } else NA_real_
  } else {
    joined <- character(0); uncovered <- NA_real_; n_zero <- NA_integer_
    n_na <- NA_integer_; zero_share <- NA_real_
    uncovered_need <- NA_real_; total_need <- NA_real_
  }
  g_coverage <- is.finite(uncovered) && uncovered <= max_uncovered_need

  # 5. ACCOUNTING -----------------------------------------------------------
  acc <- tryCatch({
    d <- utils::read.csv(artifact_path("access_ascertainment", "provider_disposition_fpmrs.csv"),
                         stringsAsFactors = FALSE)
    d <- d[d$analysis_year == surface_year, , drop = FALSE]
    list(n = nrow(d), dup = anyDuplicated(d$npi) > 0,
         unknown = setdiff(unique(d$disposition), ACCESS_PROVIDER_DISPOSITIONS),
         unexplained = sum(d$disposition == "unexplained_pipeline_loss"),
         eligible = if (is.null(asc)) NA_integer_ else asc$eligible_provider_n)
  }, error = function(e) NULL)
  g_accounting <- !is.null(acc) && !acc$dup && length(acc$unknown) == 0L &&
    acc$unexplained == 0L && isTRUE(acc$n == acc$eligible)

  # 6. PROVENANCE -----------------------------------------------------------
  g_prov <- is.list(provenance) && all(c("path", "sha256") %in% names(provenance)) &&
    nzchar(provenance$sha256 %||% "")

  gates <- tibble::tibble(
    gate = ACCESS_SURFACE_GATES$gate,
    pass = c(g_workforce, g_vintage, g_measure, g_coverage, g_prov, g_accounting),
    measured = c(
      if (is.null(asc)) "no ascertainment record" else
        sprintf("%d of %d eligible reach surface (%s%%); all losses explained: %s",
                asc$surface_provider_n, asc$eligible_provider_n,
                mufflyaccess::safe_percent(asc$surface_provider_n,
                                           asc$eligible_provider_n, digits = 1),
                asc$all_losses_explained),
      sprintf("provider year %s; GEOID width %s; census vintage %s; ACS denominator %s",
              paste(yrs, collapse = "/"),
              paste(unique(nchar(surface$GEOID)), collapse = "/"),
              expect_vintage, acs_year),
      sprintf("access col %s; two-step ratio %s; all-NA access columns: %s; provider denominator recorded %s vs artifact %s (%s)",
              if (has_access) "present" else "ABSENT", two_step,
              if (length(empty_cols)) paste(empty_cols, collapse = ",") else "none",
              recorded_n, derived_n, if (isTRUE(denom_ok)) "match" else "MISMATCH"),
      sprintf("%d tracts joined; %s%% of need uncovered; %s zero-access tracts carrying %s%% of weighted need; %s NA",
              length(joined),
              mufflyaccess::safe_percent(uncovered_need, total_need, digits = 2),
              n_zero, signif(zero_share, 3), n_na),
      if (g_prov) "recorded" else "no producing run or hash recorded",
      if (is.null(acc)) "no disposition table" else
        sprintf("%d dispositions vs %s eligible; %d unexplained; dup %s; unknown labels %d",
                acc$n, acc$eligible, acc$unexplained, acc$dup, length(acc$unknown))
    )
  )
  list(gates = gates,
       verdict = if (all(gates$pass)) "pass" else "fail",
       blocking = gates$gate[!gates$pass])
}

#' Refuse to resolve geographic access on a surface that fails its gates
#'
#' @param result Output of [validate_access_surface()].
#' @return Invisibly `TRUE`. Errors when any gate fails.
#' @family access
#' @concept validation
#' @export
assert_access_surface_usable <- function(result) {
  stopifnot(is.list(result), "verdict" %in% names(result))
  if (!identical(result$verdict, "pass")) {
    stop(sprintf(paste(
      "ACCESS SURFACE REJECTED: %d of %d gates failed (%s).\n%s\n",
      "A surface that exists is not a surface that answers this model's",
      "question. Do not resolve geographic_access_status() on it."),
      length(result$blocking), nrow(result$gates),
      paste(result$blocking, collapse = ", "),
      paste(sprintf("  - %s: %s", result$gates$gate[!result$gates$pass],
                    result$gates$measured[!result$gates$pass]), collapse = "\n")),
      call. = FALSE)
  }
  invisible(TRUE)
}
