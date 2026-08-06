# Partial-Pooling Age-Band Departure Hazards ----
#
# Port of cliff/scripts/hierarchical_hazard_partial_pooling.R.
#
# Departure hazards are estimated per (subspecialty x age-band), but many cells
# are sparse (URPS 65-69 has ~53 person-years; the 70+ cell is 0 events / 16 PY).
# Estimating each subspecialty independently (UNPOOLED) makes sparse cells noisy
# or degenerate (0 events -> hazard 0); forcing one shared curve (fully POOLED)
# erases real subspecialty differences. PARTIAL POOLING is the middle ground: a
# binomial discrete-time hazard with a shared age-band shape (fixed effect) and a
# per-subspecialty random intercept, so sparse cells borrow strength from the
# pooled pattern while well-observed cells keep their own signal.
#
# blme (NOT plain lme4) is required: with only 2-3 subspecialties, lme4::glmer
# collapses the between-subspecialty variance to exactly 0 (i.e. full pooling);
# blme::bglmer puts a weakly-informative gamma prior on that variance so it stays
# positive and the pool is genuine. blme + lme4 are in Suggests (optional): the
# fit function requires them at call time, everything else works without them.

#' Reshape the wide pooled-hazard file into long (subspecialty, band, py, ev)
#'
#' Accepts the `hazard_by_band_pooled_vs_unpooled.csv` layout (columns
#' `band`, `<sub>_events`, `<sub>_py` for each subspecialty) and returns the long
#' person-year / event table the fit consumes.
#'
#' @param wide Data frame with `band` and `<sub>_events` / `<sub>_py` columns.
#' @param subspecialties Subspecialty prefixes to extract (default go/urps/migs).
#' @return Tibble `subspecialty`, `band`, `py`, `ev` (cells with py > 0).
#' @export
hazard_pooled_long <- function(wide, subspecialties = c("go", "urps", "migs")) {
  assertthat::assert_that("band" %in% names(wide))
  rows <- lapply(subspecialties, function(s) {
    ev_col <- paste0(s, "_events")
    py_col <- paste0(s, "_py")
    if (!all(c(ev_col, py_col) %in% names(wide))) return(NULL)
    tibble::tibble(subspecialty = toupper(s), band = as.character(wide$band),
                   py = as.numeric(wide[[py_col]]), ev = as.numeric(wide[[ev_col]]))
  })
  out <- dplyr::bind_rows(rows)
  dplyr::filter(out, .data$py > 0)
}

#' Fit unpooled / pooled / partial-pooled age-band departure hazards
#'
#' @param agg Data frame with `subspecialty`, `band`, `py`, `ev`.
#' @param band_levels Optional ordered band levels (default: appearance order).
#' @param prior_shape,prior_rate Gamma prior on the between-subspecialty variance
#'   (cliff uses shape = 2, rate = 0.5).
#' @return List: `hazards` (tibble with `unpooled`, `pooled`, `partial` columns),
#'   `fit` (the bglmer object), `status` ("fitted").
#' @export
fit_partial_pooled_hazards <- function(agg, band_levels = NULL,
                                       prior_shape = 2, prior_rate = 0.5) {
  assertthat::assert_that(all(c("subspecialty", "band", "py", "ev") %in% names(agg)))
  for (pkg in c("blme", "lme4")) {
    if (!requireNamespace(pkg, quietly = TRUE)) {
      stop(sprintf("fit_partial_pooled_hazards() needs the '%s' package (Suggests). Install it to fit.", pkg),
           call. = FALSE)
    }
  }

  if (is.null(band_levels)) band_levels <- unique(as.character(agg$band))
  d <- agg
  d$band <- factor(as.character(d$band), levels = band_levels)
  d$subspecialty <- factor(as.character(d$subspecialty))

  # UNPOOLED: ev/py per (subspecialty, band).
  d$unpooled <- d$ev / d$py

  # POOLED: ev/py per band (summed across subspecialties).
  pooled_tot <- d %>%
    dplyr::group_by(.data$band) %>%
    dplyr::summarise(py = sum(.data$py), ev = sum(.data$ev), .groups = "drop") %>%
    dplyr::mutate(pooled = .data$ev / .data$py)
  d <- safe_left_join(d, dplyr::select(pooled_tot, "band", "pooled"), by = "band")

  # PARTIAL-POOLED: shared band fixed effect + per-subspecialty random intercept,
  # with a weakly-informative gamma prior on the group variance (blme::bglmer).
  # blme evaluates `cov.prior` by non-standard evaluation (`gamma` is defined in
  # blme's env, not called here), so the numeric literals are substituted into
  # the call with bquote() -- passing a variable would not resolve in blme's env.
  fit <- eval(bquote(blme::bglmer(
    cbind(ev, py - ev) ~ band + (1 | subspecialty),
    data = d, family = stats::binomial(),
    cov.prior = gamma(shape = .(prior_shape), rate = .(prior_rate)),
    control = lme4::glmerControl(optimizer = "bobyqa")
  )))
  d$partial <- as.numeric(stats::predict(fit, newdata = d, type = "response"))

  hazards <- tibble::as_tibble(d) %>%
    dplyr::mutate(band = as.character(.data$band), subspecialty = as.character(.data$subspecialty)) %>%
    dplyr::select("subspecialty", "band", "py", "ev", "unpooled", "pooled", "partial")

  list(hazards = hazards, fit = fit, status = "fitted")
}

#' Between-subspecialty spread of hazards, by pooling method
#'
#' Shrinkage is visible as a smaller between-subspecialty spread under partial
#' pooling than under unpooled estimation. This summarises that per band.
#'
#' @param hazards The `hazards` tibble from [fit_partial_pooled_hazards()].
#' @return Tibble `band`, `sd_unpooled`, `sd_partial` (SD across subspecialties).
#' @export
hazard_shrinkage_summary <- function(hazards) {
  hazards %>%
    dplyr::group_by(.data$band) %>%
    dplyr::summarise(
      sd_unpooled = stats::sd(.data$unpooled),
      sd_partial = stats::sd(.data$partial),
      .groups = "drop"
    )
}

#' Partial-pooled URPS hazards from the canonical pooled-hazard source
#'
#' Convenience wrapper: resolves the canonical `urps_hazard_pooled` file (wired
#' by the calibration harvest), reshapes it, and fits the partial-pooled model.
#' Requires that calibration source to be registered/present.
#'
#' @param band_levels Optional ordered band levels.
#' @param mode Reproducibility mode.
#' @return The [fit_partial_pooled_hazards()] result.
#' @keywords internal
urps_partial_pooled_hazards <- function(band_levels = NULL,
                                        mode = resolve_reproducibility_mode()) {
  path <- resolve_canonical("urps_hazard_pooled", mode = mode)
  wide <- utils::read.csv(path, stringsAsFactors = FALSE)
  agg <- hazard_pooled_long(wide)
  if (is.null(band_levels)) {
    band_levels <- c("<45", "45-49", "50-54", "55-59", "60-64", "65-69", "70+")
    band_levels <- band_levels[band_levels %in% agg$band]
  }
  fit_partial_pooled_hazards(agg, band_levels = band_levels)
}
