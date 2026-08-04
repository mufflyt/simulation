# MEPS-Estimated Care Seeking (two-part model) ----
#
# The demand chain previously went straight from prevalence to service volume,
# with the step in between supplied by two hard-coded constants:
#
#   CARE_SEEKING_BY_INSURANCE <- c(Insured = 1.00, Uninsured = 0.58, Unknown = 0.80)
#   CARE_SEEKING_BY_INCOME    <- ...
#
# Those are assumptions with a citation, not estimates, and every prevalent case
# they touch is assumed to generate demand at a rate nobody measured in these
# data. This module estimates the step instead, from MEPS, as the two-part
# (hurdle) structure the quantity actually has:
#
#   Part 1  P(any pelvic-floor ambulatory visit | age, insurance, income,
#           race/ethnicity, comorbidity)          -- survey-weighted logistic
#   Part 2  visits | sought care                   -- survey-weighted quasi-Poisson
#
# Expected visits per woman is the product. Splitting them matters: the barrier
# to entering care and the intensity of care once in it are different processes
# with different gradients, and a single rate cannot separate "fewer women are
# seen" from "each woman is seen less often".
#
# DATA. MEPS office-based events (HC-248G for 2023) carry EVNTIDX; conditions
# (COND) carry ICD-10 and CONDIDX; CLNK links them. Joining COND -> CLNK -> OB
# yields the pelvic-floor visits, which aggregate to the person level and merge
# onto the full-year consolidated file (FYC) by DUPERSID for demographics,
# insurance, income and the survey design (VARPSU / VARSTR / PERWT<yy>F).
#
# IDENTIFIABILITY. The 2023 analytic sample is 8,123 adult women carrying 299
# care-seeking events. That supports the comorbidity and race/ethnicity
# gradients and does NOT support the insurance gradient: its standard errors are
# larger than its coefficients. `care_seeking_multipliers()` therefore returns a
# confidence interval and an `identified` flag with every multiplier, so an
# estimate the data cannot distinguish from 1.0 cannot be quietly adopted as if
# it were measured. That is the specific failure being corrected here -- swapping
# an assumed constant for an unidentified estimate would be no better.

#' ICD-10 prefixes defining a pelvic-floor condition in MEPS
#'
#' N39 (other urinary, incl. stress incontinence), N81 (female genital
#' prolapse), R32 (unspecified urinary incontinence), R15 (fecal incontinence).
#' MEPS releases ICD-10 truncated to three characters, so these are prefixes by
#' construction rather than by choice.
#'
#' @format Character vector of ICD-10 prefixes.
#' @export
MEPS_PELVIC_FLOOR_ICD10 <- c("N39", "N81", "R32", "R15")

# MEPS ships haven_labelled columns; base comparisons on them error. Internal.
.meps_unlabel <- function(d) {
  d <- as.data.frame(d, stringsAsFactors = FALSE)
  for (j in names(d)) {
    if (inherits(d[[j]], "haven_labelled")) d[[j]] <- as.vector(d[[j]])
  }
  d
}

#' Build the person-level care-seeking analytic frame from MEPS files
#'
#' @param fyc Full-year consolidated file (person level).
#' @param cond Conditions file, carrying `ICD10CDX` and `CONDIDX`.
#' @param clnk Condition-event link file.
#' @param ob Office-based events file (HC-248G for 2023).
#' @param year MEPS data year; selects the year-suffixed columns.
#' @param icd10 Pelvic-floor ICD-10 prefixes.
#' @return A person-level data frame of adult women: `sought` (0/1), `pf_visits`,
#'   `pf_spend`, `n_comorbid`, the covariates, and the survey-design columns.
#' @export
build_meps_care_seeking_panel <- function(fyc, cond, clnk, ob,
                                          year = 2023L,
                                          icd10 = MEPS_PELVIC_FLOOR_ICD10) {
  fyc <- .meps_unlabel(fyc); cond <- .meps_unlabel(cond)
  clnk <- .meps_unlabel(clnk); ob <- .meps_unlabel(ob)
  yy <- substr(as.character(year), 3, 4)

  need <- function(d, cols, what) {
    missing <- setdiff(cols, names(d))
    if (length(missing)) {
      stop(sprintf("build_meps_care_seeking_panel: %s is missing %s", what,
                   paste(missing, collapse = ", ")), call. = FALSE)
    }
  }
  wt <- paste0("PERWT", yy, "F")
  need(fyc, c("DUPERSID", "SEX", "AGELAST", "RACETHX", wt, "VARPSU", "VARSTR"), "FYC")
  need(cond, c("DUPERSID", "CONDIDX", "ICD10CDX"), "COND")
  need(clnk, c("CONDIDX", "EVNTIDX"), "CLNK")
  need(ob, "EVNTIDX", "OB")

  is_pf <- substr(cond$ICD10CDX, 1, 3) %in% icd10

  # COND -> CLNK -> OB. unique() before the join: one visit can carry the same
  # condition on several records, and counting those twice would inflate the
  # visit count for exactly the women the model is about.
  pf_events <- unique(merge(cond[is_pf, c("DUPERSID", "CONDIDX")],
                            clnk[, c("CONDIDX", "EVNTIDX")], by = "CONDIDX")[,
                            c("DUPERSID", "EVNTIDX")])
  xp <- paste0("OBXP", yy, "X")
  keep_ob <- intersect(c("EVNTIDX", xp), names(ob))
  pf_ob <- merge(pf_events, ob[, keep_ob, drop = FALSE], by = "EVNTIDX")

  agg <- function(v, f, nm) {
    if (!nrow(pf_ob)) return(stats::setNames(data.frame(character(0), numeric(0)),
                                             c("DUPERSID", nm)))
    out <- stats::aggregate(list(x = v), by = list(DUPERSID = pf_ob$DUPERSID), FUN = f)
    stats::setNames(out, c("DUPERSID", nm))
  }
  per_visits <- agg(pf_ob$EVNTIDX, length, "pf_visits")
  per_spend <- if (xp %in% names(pf_ob)) agg(pf_ob[[xp]], sum, "pf_spend") else NULL

  # Comorbidity excludes the pelvic-floor conditions themselves: including them
  # would make the covariate a proxy for the outcome.
  com <- cond[!is_pf, c("DUPERSID", "ICD10CDX")]
  com <- stats::aggregate(list(n_comorbid = com$ICD10CDX),
                          by = list(DUPERSID = com$DUPERSID),
                          FUN = function(z) length(unique(z)))

  p <- fyc[fyc$SEX == 2 & fyc$AGELAST >= 18, , drop = FALSE]
  keep <- intersect(c("DUPERSID", "AGELAST", "RACETHX", paste0("POVCAT", yy),
                      paste0("INSURC", yy), wt, "VARPSU", "VARSTR"), names(p))
  p <- p[, keep, drop = FALSE]
  p <- merge(p, per_visits, by = "DUPERSID", all.x = TRUE)
  if (!is.null(per_spend)) p <- merge(p, per_spend, by = "DUPERSID", all.x = TRUE)
  p <- merge(p, com, by = "DUPERSID", all.x = TRUE)

  zero <- function(x, z = 0) ifelse(is.na(x), z, x)
  p$pf_visits <- zero(p$pf_visits)
  if (!is.null(per_spend)) p$pf_spend <- zero(p$pf_spend)
  p$n_comorbid <- zero(p$n_comorbid)
  p$sought <- as.integer(p$pf_visits > 0)

  p$weight <- p[[wt]]
  p$age_c <- (p$AGELAST - 50) / 10
  pov <- paste0("POVCAT", yy); ins <- paste0("INSURC", yy)
  if (ins %in% names(p)) {
    p$insurance <- factor(
      ifelse(p[[ins]] %in% c(1, 2, 4, 5, 6), "Private",
             ifelse(p[[ins]] %in% c(3, 7), "Public", "Uninsured")),
      levels = c("Private", "Public", "Uninsured"))
  }
  if (pov %in% names(p)) {
    p$income <- factor(
      c("LT100FPL", "LT100FPL", "100_199FPL", "200_399FPL", "GE400FPL")[p[[pov]]],
      levels = c("GE400FPL", "200_399FPL", "100_199FPL", "LT100FPL"))
  }
  p$race <- factor(
    c("Hispanic", "NH_White", "NH_Black", "NH_Asian", "NH_Other")[p$RACETHX],
    levels = c("NH_White", "Hispanic", "NH_Black", "NH_Asian", "NH_Other"))
  p
}

#' Fit the two-part MEPS care-seeking model
#'
#' @param panel Person-level frame from [build_meps_care_seeking_panel()].
#' @param part1_terms Right-hand side for the care-seeking (hurdle) model.
#' @param part2_terms Right-hand side for the visit-count model.
#' @return An object of class `urps_care_seeking_model`.
#' @export
fit_care_seeking_model <- function(panel,
                                   part1_terms = c("age_c", "insurance", "income",
                                                   "race", "n_comorbid"),
                                   part2_terms = c("age_c", "insurance", "income",
                                                   "n_comorbid")) {
  if (!requireNamespace("survey", quietly = TRUE)) {
    stop("fit_care_seeking_model(): package 'survey' is required. The design ",
         "(VARPSU/VARSTR/PERWT) is not optional -- unweighted MEPS estimates are ",
         "not nationally representative.", call. = FALSE)
  }
  need <- c("sought", "pf_visits", "weight", "VARPSU", "VARSTR")
  missing <- setdiff(need, names(panel))
  if (length(missing)) {
    stop(sprintf("fit_care_seeking_model: panel is missing %s",
                 paste(missing, collapse = ", ")), call. = FALSE)
  }
  part1_terms <- intersect(part1_terms, names(panel))
  part2_terms <- intersect(part2_terms, names(panel))

  op <- options(survey.lonely.psu = "adjust"); on.exit(options(op), add = TRUE)
  des <- survey::svydesign(id = ~VARPSU, strata = ~VARSTR, weights = ~weight,
                           data = panel, nest = TRUE)

  f1 <- stats::as.formula(paste("sought ~", paste(part1_terms, collapse = " + ")))
  m1 <- survey::svyglm(f1, design = des, family = stats::quasibinomial())

  # Part 2 is fitted on care-seekers only, so the count model describes intensity
  # among women in care rather than a mixture of that and the zeros.
  des2 <- subset(des, sought == 1)
  f2 <- stats::as.formula(paste("pf_visits ~", paste(part2_terms, collapse = " + ")))
  m2 <- survey::svyglm(f2, design = des2, family = stats::quasipoisson())

  structure(
    list(part1 = m1, part2 = m2,
         n_persons = nrow(panel), n_events = sum(panel$sought),
         weighted_care_seeking = stats::weighted.mean(panel$sought, panel$weight),
         status = "estimated"),
    class = "urps_care_seeking_model")
}

#' Expected pelvic-floor ambulatory visits per woman
#'
#' The two parts multiplied: P(seeks care) x E\[visits | seeks care\]. Returning
#' the parts alongside the product is deliberate -- a change in expected visits
#' means something different depending on which part moved.
#'
#' @param model A [fit_care_seeking_model()] object.
#' @param newdata Covariate frame.
#' @return Data frame of `p_seek`, `visits_if_seek`, `expected_visits`.
#' @export
predict_care_seeking <- function(model, newdata) {
  stopifnot(inherits(model, "urps_care_seeking_model"))
  p <- as.numeric(stats::predict(model$part1, newdata = newdata, type = "response"))
  v <- as.numeric(stats::predict(model$part2, newdata = newdata, type = "response"))
  data.frame(p_seek = p, visits_if_seek = v, expected_visits = p * v)
}

#' Care-seeking multipliers, with the uncertainty that decides whether to use them
#'
#' Reports each level's care-seeking rate relative to the reference level, with a
#' confidence interval and an `identified` flag that is FALSE when the interval
#' covers 1.0. An unidentified multiplier is not evidence for a value: adopting a
#' point estimate the data cannot distinguish from "no effect" repeats the error
#' of the hard-coded constants it would replace.
#'
#' @param model A [fit_care_seeking_model()] object.
#' @param variable Covariate to vary (e.g. "insurance", "income", "race").
#' @param reference Covariate frame holding every other term fixed.
#' @param level Confidence level.
#' @return Data frame: `variable`, `level`, `multiplier`, `conf_low`,
#'   `conf_high`, `identified`.
#' @export
care_seeking_multipliers <- function(model, variable, reference, level = 0.95) {
  stopifnot(inherits(model, "urps_care_seeking_model"))
  xl <- model$part1$xlevels[[variable]]
  if (is.null(xl)) {
    stop(sprintf(paste("care_seeking_multipliers: '%s' is not a factor term in",
                       "the care-seeking model."), variable), call. = FALSE)
  }
  base <- as.numeric(stats::predict(model$part1, newdata = reference, type = "response"))
  z <- stats::qnorm(1 - (1 - level) / 2)

  out <- lapply(xl, function(lv) {
    nd <- reference; nd[[variable]] <- factor(lv, levels = xl)
    pr <- stats::predict(model$part1, newdata = nd, type = "response")
    est <- as.numeric(pr); se <- as.numeric(survey::SE(pr))
    # The interval is a delta-method approximation on a ratio of predicted
    # probabilities, so its lower limit can fall below zero when the estimate is
    # badly determined. A negative care-seeking multiplier is not a quantity;
    # clamp at zero and let `identified` carry the real message.
    lo <- max((est - z * se) / base, 0); hi <- (est + z * se) / base
    data.frame(variable = variable, level = lv, multiplier = est / base,
               conf_low = lo, conf_high = hi,
               identified = !(lo <= 1 && hi >= 1), stringsAsFactors = FALSE)
  })
  out <- do.call(rbind, out)
  rownames(out) <- NULL
  out
}

#' @export
print.urps_care_seeking_model <- function(x, ...) {
  cat("MEPS care-seeking model (two-part)\n")
  cat(sprintf("  persons             %d\n", x$n_persons))
  cat(sprintf("  care-seeking events %d\n", x$n_events))
  cat(sprintf("  weighted rate       %.2f%% of adult women per year\n",
              100 * x$weighted_care_seeking))
  cat(sprintf("  part 1              %s\n",
              paste(deparse(stats::formula(x$part1)), collapse = "")))
  cat(sprintf("  part 2              %s\n",
              paste(deparse(stats::formula(x$part2)), collapse = "")))
  invisible(x)
}
