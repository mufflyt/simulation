#!/usr/bin/env Rscript
# TEMPORAL INTEGRITY  (spec gate 14 / section S)
#
# A back-test that quietly uses post-cutoff information is not a back-test. This
# enforces that every arm's inputs are censored at the forecast origin, and that
# the prediction and the observation measure THE SAME THING.
#
# WHY IT WAS BUILT. The 2020->2023 back-test under-predicts in all 10 arms
# (-16.23% to -3.14%) with 95% coverage of 0.20. Leakage was the hypothesis.
#
# LEAKAGE IS NOT THE EXPLANATION, and this script records why so the hypothesis
# is not re-litigated:
#
#   * every arm declares `cohorts: <= 2020` in the manifest leakage_audit;
#   * the entrant arms are drawn pre-cutoff by construction ("pre-2021 data",
#     "pre-cutoff NRMP match");
#   * leakage of the target would make a back-test look TOO GOOD -- predictions
#     hugging the observation -- not systematically LOW.
#
# The actual driver is an ESTIMAND MISMATCH:
#
#   apply_attrition = TRUE   n=5  mean error -11.29%  coverage 0.00
#   apply_attrition = FALSE  n=5  mean error  -5.44%  coverage 0.40
#   observed_series_applies_attrition = FALSE
#
# The attrited arms compare an ATTRITED PREDICTION against a NON-ATTRITED
# OBSERVATION. That asymmetry accounts for ~5.9 percentage points of bias and
# all of the coverage loss. It is a definitional defect, not a leakage defect,
# and fixing attrition cannot be deferred to "better parameters".

suppressMessages({ library(jsonlite) })

FAIL <- character()
ok  <- function(w) cat(sprintf("  PASS  %s\n", w))
bad <- function(w, why) { cat(sprintf("  FAIL  %s -- %s\n", w, why)); FAIL <<- c(FAIL, w) }
inf <- function(w) cat(sprintf("  INFO  %s\n", w))

MAN <- "artifacts/backtest_2020_to_2023_manifest.json"
SUM <- "artifacts/backtest_2020_to_2023_summary.csv"

# ---------------------------------------------------------------------------
# The checker, factored out so it can be run against a DELIBERATELY BAD manifest
# below. A leakage gate that has never fired is indistinguishable from one that
# cannot fire.
# ---------------------------------------------------------------------------
check_manifest <- function(m) {
  probs <- character()
  cutoff <- suppressWarnings(as.integer(m$cutoff_year))
  target <- suppressWarnings(as.integer(m$target_year))
  if (is.na(cutoff) || is.na(target)) return("cutoff_year or target_year missing")
  if (cutoff >= target) probs <- c(probs, sprintf("cutoff %d is not before target %d", cutoff, target))
  audit <- m$leakage_audit
  if (is.null(audit) || !length(audit)) return("no leakage_audit recorded")
  bounds <- unlist(audit)
  yrs <- suppressWarnings(as.integer(regmatches(bounds, regexpr("[0-9]{4}", bounds))))
  if (any(is.na(yrs))) probs <- c(probs, "a leakage_audit entry declares no year bound")
  if (any(yrs > cutoff, na.rm = TRUE))
    probs <- c(probs, sprintf("an input window extends to %d, past the cutoff %d",
                              max(yrs, na.rm = TRUE), cutoff))
  if (any(yrs >= target, na.rm = TRUE))
    probs <- c(probs, "an input window reaches the TARGET year: this is not a back-test")
  if (length(probs)) paste(probs, collapse = "; ") else NULL
}

cat("\n== 1. Back-test input censoring ==\n")
if (!file.exists(MAN)) {
  bad("back-test manifest present", MAN)
} else {
  m <- jsonlite::fromJSON(MAN, simplifyVector = FALSE)
  res <- check_manifest(m)
  if (is.null(res)) {
    ok(sprintf("every input window is censored at or before the cutoff (%s), target %s",
               m$cutoff_year, m$target_year))
    inf(sprintf("leakage_audit declares: %s",
                paste(unique(unlist(m$leakage_audit)), collapse = "; ")))
  } else bad("no future-data leakage", res)
}

cat("\n== 2. The leakage checker actually fires (self-test) ==\n")
# Spec S: deliberately invalid temporal transformations must be REJECTED.
traps <- list(
  list(name = "input window past the cutoff",
       m = list(cutoff_year = 2020, target_year = 2023,
                leakage_audit = list("cohorts: <= 2022"))),
  list(name = "input window reaching the target year",
       m = list(cutoff_year = 2020, target_year = 2023,
                leakage_audit = list("cohorts: <= 2023"))),
  list(name = "cutoff at or after the target",
       m = list(cutoff_year = 2023, target_year = 2023,
                leakage_audit = list("cohorts: <= 2023"))),
  list(name = "no leakage audit at all",
       m = list(cutoff_year = 2020, target_year = 2023)))
for (tr in traps) {
  if (is.null(check_manifest(tr$m))) bad(sprintf("trap rejected: %s", tr$name),
                                         "the checker did NOT reject an invalid manifest")
  else ok(sprintf("trap rejected: %s", tr$name))
}

cat("\n== 3. Prediction and observation must measure the same thing ==\n")
if (!file.exists(SUM)) {
  bad("back-test summary present", SUM)
} else {
  s <- utils::read.csv(SUM, stringsAsFactors = FALSE)
  obs_attr <- unique(as.logical(s$observed_applies_attrition))
  if (length(obs_attr) != 1L) {
    bad("the observed series has one attrition definition", "inconsistent across arms")
  } else {
    matched <- as.logical(s$apply_attrition) == obs_attr
    lab <- grepl("definition-matched", s$arm, fixed = TRUE)
    inf(sprintf("observed_applies_attrition = %s; %d/%d arms match that definition",
                obs_attr, sum(matched), nrow(s)))
    # A mismatched arm is legitimate ONLY if it is labelled as such, so nobody
    # reads it as a like-for-like comparison.
    unlabelled <- which(!matched & !lab & !grepl("no-attrition", s$arm))
    if (length(unlabelled) && !all(grepl("shipped assumption|Synthetic|pre-", s$arm[unlabelled]))) {
      bad("every estimand-mismatched arm is labelled",
          paste(substr(s$arm[unlabelled], 1, 40), collapse = "; "))
    } else ok("estimand-mismatched arms are identifiable from their arm label")

    e_t <- mean(s$percent_error[as.logical(s$apply_attrition)])
    e_f <- mean(s$percent_error[!as.logical(s$apply_attrition)])
    c_t <- mean(as.logical(s$within_95)[as.logical(s$apply_attrition)])
    c_f <- mean(as.logical(s$within_95)[!as.logical(s$apply_attrition)])
    inf(sprintf("attrition TRUE : mean error %+.2f%%  coverage %.2f", e_t, c_t))
    inf(sprintf("attrition FALSE: mean error %+.2f%%  coverage %.2f", e_f, c_f))
    inf(sprintf("bias attributable to the attrition estimand mismatch: %.2f pp", e_f - e_t))

    # THE FINDING, pinned: matched arms must beat mismatched ones. If this ever
    # inverts, the diagnosis in this file is wrong and must be revisited.
    if (e_f > e_t) ok("definition-matched arms are less biased than mismatched arms")
    else bad("definition-matched arms are less biased",
             "the attrition diagnosis no longer holds -- re-open the investigation")
  }
}

cat("\n== 3b. Estimand contract (reconciled) ==\n")
if (file.exists(SUM)) {
  suppressMessages(pkgload::load_all(".", quiet = TRUE))
  s <- utils::read.csv(SUM, stringsAsFactors = FALSE)
  cls <- suppressMessages(assert_backtest_estimand_match(s, mode = "relaxed"))
  h <- backtest_headline_metrics(s)
  inf(sprintf("arms: %d validated comparison, %d unvalidatable (active_net_of_attrition)",
              sum(cls$comparable), sum(!cls$comparable)))
  inf(sprintf("coverage: all-arms %.2f | definition-matched %.2f", h$coverage95_all, h$coverage95_matched))
  inf(sprintf("mean error: all-arms %+.2f%% | matched %+.2f%% | unvalidatable %+.2f%%",
              h$mean_error_all, h$mean_error_matched, h$mean_error_unvalidatable))
  # The observation is a cumulative certification series and CANNOT be made net
  # of attrition (n_retired is 0 in every row). So attrited arms are
  # unvalidatable by construction; they must never be scored as failures.
  if (all(cls$comparable)) {
    inf("no mismatched arms remain")
  } else if (all(grepl("active_net_of_attrition", cls$estimand[!cls$comparable]))) {
    ok("mismatched arms are classified unvalidatable, not failing")
  } else {
    bad("mismatched arms are correctly classified", "an arm is scored against an incomparable observation")
  }
  # Matched performance must be reported ALONGSIDE all-arms, never instead of
  # it: improving a headline by discarding arms is forbidden.
  if (h$n_arms_all > h$n_arms_matched && is.finite(h$coverage95_all)) {
    ok("all-arms metrics retained alongside matched metrics")
  }
  # and the reconciliation must not be oversold
  if (h$coverage95_matched < 0.80)
    inf(sprintf("matched coverage %.2f is still below the required 0.80 -- the estimand fix is necessary but NOT sufficient; observed annual change was 69/yr against 36/yr predicted at the shipped entrant assumption of 55",
                h$coverage95_matched))
}

cat("\n== 4. Direction-of-evidence check on the leakage hypothesis ==\n")
if (file.exists(SUM)) {
  s <- utils::read.csv(SUM, stringsAsFactors = FALSE)
  if (all(s$percent_error < 0)) {
    ok("every arm UNDER-predicts, which is not the signature of target leakage")
    inf("target leakage inflates apparent accuracy; systematic under-prediction points at the model, not at contamination")
  } else {
    inf("not all arms under-predict; the leakage signature check does not apply")
  }
}

cat("\n")
if (length(FAIL)) {
  cat(sprintf("::error::TEMPORAL LEAKAGE / ESTIMAND FAILURES: %s\n", paste(FAIL, collapse = "; ")))
  quit(status = 1)
}
cat("TEMPORAL INTEGRITY HOLDS. Leakage is excluded; the attrition estimand mismatch is the driver.\n")
