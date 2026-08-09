#!/usr/bin/env Rscript
# Manuscript tables, generated from gated evidence ----
#
#   Rscript scripts/manuscript/build_tables.R
#
# WHY THIS EXISTS. Every table in docs/submission/MANUSCRIPT.docx was, until
# now, transcribed by hand from a CSV. That made hand transcription the weakest
# provenance link in a project that otherwise stamps run identity before
# computation, refuses to start from a dirty tree, and reproduces at zero
# tolerance. A rerun that moves a number leaves the manuscript silently stale,
# and nothing in the repository notices. This script makes each manuscript
# number a downstream product of the evidence chain rather than a copy of it.
#
# THE RULE IT ENFORCES. A manuscript table may not be built from a source that
# is exploratory, a fallback, failed, incomplete, or otherwise non-citable. The
# rule is enforced in code, not documented in prose, because the failure it
# guards against is somebody in a hurry finding a plausible CSV. That is not a
# hypothetical here: artifacts/validation/ currently holds eight EXPLORATORY
# runs of analysis 04 whose directory names differ from the two authoritative
# ones by a suffix, and artifacts/demand_backtest_summary.csv looks like a
# clean 3.6% MAPE result until you read its own anchors_source column and find
# `illustrative_fallback`.
#
# WHAT THIS SCRIPT DOES NOT DO -- and the boundary is deliberate. It RENDERS;
# it does not ANALYSE. Rounding and unit conversion for display are allowed.
# Anything that aggregates across rows -- a mean, a pooled rate, a ratio of two
# rows -- is not, because a statistic computed here would be a manuscript
# number with no run identity, which is the exact defect the script exists to
# remove. If the paper needs a summary statistic, an analysis must emit it into
# a gated artifact and this script must read it.

suppressPackageStartupMessages({
  if (!requireNamespace("urpssim", quietly = TRUE)) pkgload::load_all(".", quiet = TRUE) else library(urpssim)
})
source(file.path("scripts", "validation", "_provenance.R"))
source(file.path("scripts", "manuscript", "_eligibility.R"))

OUT_DIR <- file.path("docs", "submission", "tables")

# ---- Declared sources ------------------------------------------------------
#
# Every source a manuscript table may draw on, named explicitly. A table cannot
# reach an artifact that is not on this list, so adding evidence to the paper is
# a reviewable edit here rather than a path typed into a builder.
V <- file.path("artifacts", "validation")
SOURCES <- list(
  backtest = list(kind = "pinned",
                  csv = file.path("artifacts", "backtest_2020_to_2023_summary.csv"),
                  manifest = file.path("artifacts", "backtest_2020_to_2023_manifest.json")),
  temporal = list(kind = "run", dir = file.path(V, "20260808T154509_temporal_validation_1e24ac8")),
  montecarlo = list(kind = "run", dir = file.path(V, "20260808T133320_mc_convergence_1e24ac8")),
  claims   = list(kind = "run", dir = file.path(V, "20260808T193315_delegation_claims_evidence_c471388")),
  urpshare = list(kind = "run", dir = file.path(V, "20260808T215524_urps_share_partial_identification_94e961a")),
  roster   = list(kind = "run", dir = file.path(V, "20260808T215440_roster_reconciliation_94e961a")))

# ALL sources are gated before ANY table is built, and every failure is
# collected. Stopping at the first one would hide the rest behind it and turn
# one fix-and-rerun cycle into five.
cat("== gating declared sources ==\n")
gated <- lapply(names(SOURCES), function(nm) {
  s <- SOURCES[[nm]]
  g <- if (s$kind == "run") gate_run(s$dir) else gate_pinned(s$csv, s$manifest)
  cat(sprintf("  %-11s %-9s %s\n", nm, if (g$ok) "ELIGIBLE" else "REFUSED",
              if (s$kind == "run") basename(s$dir) else basename(s$csv)))
  if (!g$ok) for (p in g$problems) cat("               ", p, "\n")
  g
})
names(gated) <- names(SOURCES)

refused <- names(gated)[!vapply(gated, function(g) isTRUE(g$ok), logical(1))]
if (length(refused))
  stop("build_tables.R: ", length(refused), " source(s) are not manuscript-eligible: ",
       paste(refused, collapse = ", "),
       ". No table was written. Fix the source or remove the table -- do not ",
       "relax the gate.", call. = FALSE)

# ---- Rendering -------------------------------------------------------------

# round() BEFORE formatC(), because formatC(x, format = "d") TRUNCATES: a 95%
# lower bound of 1070.975 renders as 1,070, and a manuscript bound quietly
# moved a full unit toward the null is precisely the kind of error that is
# invisible in the output and impossible to catch by reading the table.
fmt_n   <- function(x) formatC(round(x), format = "d", big.mark = ",")
fmt_1   <- function(x) formatC(x, format = "f", digits = 1, big.mark = ",")
fmt_2   <- function(x) formatC(x, format = "f", digits = 2, big.mark = ",")
fmt_yn  <- function(x) ifelse(as.logical(x), "Yes", "No")
fmt_sgn <- function(x, d = 1) sprintf("%+.*f", d, x)

# GFM pipe table. Numeric-looking columns right-align, which is what a journal
# copy editor expects and what makes a column of magnitudes scannable.
#
# EVERY value must look numeric, not just the first. Testing only row 1 sent
# the whole Specification column right because it begins "1. Derived cohort".
md_table <- function(df, align = NULL) {
  df[] <- lapply(df, as.character)
  numericish <- function(v) all(grepl("^[-+]?[0-9][0-9,]*([.][0-9]+)?$", trimws(v)))
  if (is.null(align))
    align <- ifelse(vapply(df, numericish, logical(1)), "right", "left")
  bar <- vapply(align, function(a) if (a == "right") "---:" else "---", character(1))
  wid <- vapply(seq_along(df), function(i)
    max(nchar(c(names(df)[i], df[[i]]))), integer(1))
  pad <- function(v, i) formatC(v, width = wid[i] * if (align[i] == "right") 1 else -1)
  rows <- c(
    paste0("| ", paste(vapply(seq_along(df), function(i) pad(names(df)[i], i), character(1)),
                       collapse = " | "), " |"),
    paste0("| ", paste(bar, collapse = " | "), " |"),
    vapply(seq_len(nrow(df)), function(r)
      paste0("| ", paste(vapply(seq_along(df), function(i) pad(df[[i]][r], i), character(1)),
                         collapse = " | "), " |"), character(1)))
  paste(rows, collapse = "\n")
}

# ---- Table definitions -----------------------------------------------------
#
# Each table is one or more PANELS. A panel is a single rendered frame with its
# own CSV; a table is the unit the manuscript cites. Splitting them keeps
# "Table 3" a stable reference while its two panels stay separately machine
# readable.

bt <- gated$backtest$tables$summary
t1 <- data.frame(
  Specification            = bt$arm,
  `Entrants per year`      = fmt_1(bt$entrants_per_year),
  Attrition                = fmt_yn(bt$apply_attrition),
  `Predicted median`       = fmt_n(bt$predicted_median),
  `95% prediction interval`= sprintf("%s to %s", fmt_n(bt$pi95_lower), fmt_n(bt$pi95_upper)),
  Observed                 = fmt_n(bt$observed),
  Difference               = fmt_n(bt$absolute_error),
  `Difference (%)`         = fmt_sgn(bt$percent_error),
  `Interval contains`      = fmt_yn(bt$within_95),
  check.names = FALSE)

tv <- gated$temporal$tables$matched_origin_leakage
t2 <- data.frame(
  Origin                       = tv$origin,
  Observed                     = fmt_n(tv$observed),
  `RO absolute error (%)`      = fmt_2(tv$ro_err),
  `RO interval width`          = fmt_n(tv$ro_width),
  `RO contains`                = fmt_yn(tv$ro_cov),
  `RO interval score`          = fmt_n(tv$ro_winkler),
  `LOO absolute error (%)`     = fmt_2(tv$loo_err),
  `LOO interval width`         = fmt_n(tv$loo_width),
  `LOO contains`               = fmt_yn(tv$loo_cov),
  `LOO interval score`         = fmt_n(tv$loo_winkler),
  `Post-origin windows used by LOO` = tv$loo_future_windows,
  check.names = FALSE)

mc <- gated$montecarlo$tables$convergence
t3a <- data.frame(
  Iterations                   = fmt_n(mc$n),
  `Median across seeds`        = fmt_1(mc$median_mean),
  `Median range (%)`           = fmt_2(mc$median_range_pct),
  `Lower bound range (%)`      = fmt_2(mc$lo_range_pct),
  `Upper bound range (%)`      = fmt_2(mc$hi_range_pct),
  `Mean interval width`        = fmt_1(mc$width_mean),
  `Width range (%)`            = fmt_2(mc$width_range_pct),
  `Criterion`                  = mc$verdict,
  check.names = FALSE)

rs <- gated$montecarlo$tables$retirement_sensitivity
t3b <- data.frame(
  `Hazard uncertainty`         = rs$label,
  `Coefficient of variation`   = fmt_2(rs$hazard_cv),
  Median                       = fmt_1(rs$median),
  `95% interval`               = sprintf("%s to %s", fmt_n(rs$lo), fmt_n(rs$hi)),
  Width                        = fmt_1(rs$width),
  `Width inflation (%)`        = fmt_1(rs$width_inflation_pct),
  `Median shift (%)`           = fmt_2(rs$median_shift_pct),
  check.names = FALSE)

cm <- gated$claims$tables$provider_mix_by_service
t4a <- data.frame(
  Service                      = cm$service,
  `Provider category`          = cm$provider,
  Episodes                     = fmt_n(cm$episodes),
  `Share (%)`                  = fmt_2(cm$share_pct),
  check.names = FALSE)

pw <- gated$claims$tables$pooled_weighting
t4b <- data.frame(
  `Provider category`          = pw$provider,
  Episodes                     = fmt_n(pw$episodes),
  `Work RVU`                   = fmt_1(pw$wrvu),
  `Episode-weighted (%)`       = fmt_2(pw$episode_weighted_pct),
  `Work-RVU-weighted (%)`      = fmt_2(pw$wrvu_weighted_pct),
  check.names = FALSE)

sb <- gated$urpshare$tables$service_bounds
# CAPTURE TRAVELS WITH THE INTERVAL. Ordered second, immediately after the
# service, so a bound can never be read off this table without the fraction of
# volume it was computed from. docs/VALIDATION_RESULTS.md fixes this at
# promotion; here it is a property of the rendering rather than a convention.
t5a <- data.frame(
  Service                      = sb$service,
  `Capture (%)`                = fmt_1(100 * sb$capture),
  `Lower bound (%)`            = fmt_1(100 * sb$L),
  `Upper bound (%)`            = fmt_1(100 * sb$H),
  `Observed-cell share (%)`    = fmt_1(100 * sb$observed_cell),
  Tier                         = sb$tier,
  check.names = FALSE)

wa <- gated$urpshare$tables$wrvu_aggregates
t5b <- data.frame(
  Aggregate                    = wa$aggregate,
  Services                     = wa$services,
  `Capture (%)`                = fmt_1(wa$capture_pct),
  `Lower bound (%)`            = fmt_1(wa$L_pct),
  `Upper bound (%)`            = fmt_1(wa$H_pct),
  `Observed-cell share (%)`    = fmt_1(wa$observed_cell_pct),
  check.names = FALSE)

wf <- gated$roster$tables$waterfall
ts1a <- data.frame(Step = wf$step, n = fmt_n(wf$n), check.names = FALSE)

rd <- gated$roster$tables$roster_dispositions
ts1b <- data.frame(Disposition = rd$disposition, Rows = fmt_n(rd$N), check.names = FALSE)

TABLES <- list(
  list(id = "table_1_principal_specifications", source = "backtest",
       title = "Model specifications and agreement with the observed 2023 count",
       caption = paste("Each specification forecasts the 2023 URPS physician count from",
                       "information available at the 2020 cutoff. Attrition = Yes removes",
                       "physicians as they retire; the comparison series is a cumulative",
                       "certification count that removes no one."),
       panels = list(specifications = t1)),
  list(id = "table_2_temporal_validation", source = "temporal",
       title = "Rolling-origin versus leave-one-out validation at matched origins",
       caption = paste("Three-year-ahead forecasts at four origins evaluable under both",
                       "designs. RO = rolling origin, admitting only training windows whose",
                       "outcomes were observable at the origin. LOO = leave-one-out, which",
                       "does not; the final column counts the post-origin windows LOO used",
                       "at each origin. Lower interval scores are better."),
       panels = list(matched_origins = t2)),
  list(id = "table_3_monte_carlo_and_parameter_uncertainty", source = "montecarlo",
       title = "Simulation convergence and retirement-hazard uncertainty",
       caption = paste("Panel A: stability of the median, interval bounds and interval",
                       "width across independent seeds at each simulation size. The",
                       "criterion was declared before the multi-seed results were viewed.",
                       "Panel B: effect of treating the retirement hazard as uncertain."),
       panels = list(convergence = t3a, retirement_sensitivity = t3b)),
  list(id = "table_4_claims_provider_mix", source = "claims",
       title = "Provider mix of claims-attributed urogynecologic care",
       caption = paste("Panel A: episodes by rendering provider category and service.",
                       "Panel B: pooled across services, weighted two ways. The work-RVU",
                       "weighting is the model-relevant one; episode weighting is shown",
                       "because the two diverge."),
       panels = list(by_service = t4a, pooled = t4b)),
  list(id = "table_5_urps_share_partial_identification", source = "urpshare",
       title = "Suppression-robust bounds on the URPS share of physician-attributed workload",
       caption = paste("2024 Medicare fee-for-service. Bounds are partially identified:",
                       "cells with fewer than 11 beneficiaries are suppressed and the",
                       "deletion is not random, so the share is constrained rather than",
                       "estimated. Capture is the fraction of each service's national",
                       "volume the provider file retains and must be read with the bound.",
                       "Tier A is the anatomically female-specific basket. These bounds",
                       "constrain a Medicare fee-for-service quantity and are not",
                       "transportable to an all-payer national share."),
       panels = list(service_bounds = t5a, wrvu_aggregates = t5b)),
  list(id = "table_s1_roster_reconciliation", source = "roster",
       title = "Reconciliation of the 2024 URPS linkage roster",
       caption = paste("Every row of the canonical roster receives exactly one",
                       "disposition, evaluated in the order shown, so the dispositions sum",
                       "to the file. Activity in 2024 is deliberately not a criterion: the",
                       "numerator is formed by intersecting with services actually billed,",
                       "so a non-biller contributes zero either way, and an activity filter",
                       "could only discard a true match."),
       panels = list(waterfall = ts1a, dispositions = ts1b)))

# ---- Write -----------------------------------------------------------------

dir.create(OUT_DIR, recursive = TRUE, showWarnings = FALSE)
for (f in list.files(OUT_DIR, "^(table_|TABLE_INDEX)", full.names = TRUE)) unlink(f)

index <- list()
cat("\n== writing tables ==\n")
for (tb in TABLES) {
  g <- gated[[tb$source]]
  panel_names <- names(tb$panels)
  md <- c(sprintf("# %s", tb$title), "",
          tb$caption, "")
  for (pn in panel_names) {
    if (length(panel_names) > 1L)
      md <- c(md, sprintf("**Panel %s.** %s", LETTERS[match(pn, panel_names)],
                          gsub("_", " ", pn)), "")
    md <- c(md, md_table(tb$panels[[pn]]), "")
    csv <- file.path(OUT_DIR, sprintf("%s__%s.csv", tb$id, pn))
    utils::write.csv(tb$panels[[pn]], csv, row.names = FALSE)
  }
  md <- c(md, "---", "",
          sprintf("Source run: `%s`", g$run_id),
          sprintf("Generated by `scripts/manuscript/build_tables.R`; do not edit by hand."))
  writeLines(md, file.path(OUT_DIR, sprintf("%s.md", tb$id)))
  cat(sprintf("  %-46s %d panel(s)\n", tb$id, length(panel_names)))

  for (pn in panel_names)
    index[[length(index) + 1L]] <- data.frame(
      table = tb$id, panel = pn, rows = nrow(tb$panels[[pn]]),
      source = tb$source, run_id = g$run_id,
      source_artifact = if (SOURCES[[tb$source]]$kind == "run")
        SOURCES[[tb$source]]$dir else SOURCES[[tb$source]]$csv,
      status = "eligible: gated, not exploratory/fallback/failed/incomplete",
      input_sha256 = paste(sprintf("%s=%s", names(g$hashes), substr(g$hashes, 1, 16)),
                           collapse = "; "),
      stringsAsFactors = FALSE)
}

idx <- do.call(rbind, index)
utils::write.csv(idx, file.path(OUT_DIR, "TABLE_INDEX.csv"), row.names = FALSE)

prov <- validation_provenance(params = list(purpose = "manuscript table generation"))
writeLines(c(
  "# Manuscript table index",
  "",
  paste("Generated by `scripts/manuscript/build_tables.R`. Every file in this",
        "directory is a build product. Editing one by hand reintroduces exactly",
        "the drift this script was written to remove -- change the analysis, or",
        "change the renderer, then rebuild."),
  "",
  md_table(data.frame(
    Table = idx$table, Panel = idx$panel, Rows = idx$rows,
    `Source run` = idx$run_id, check.names = FALSE)),
  "",
  "## Source artifacts and eligibility",
  "",
  md_table(data.frame(
    Source = unique(idx$source),
    Artifact = idx$source_artifact[!duplicated(idx$source)],
    Status = idx$status[!duplicated(idx$source)],
    check.names = FALSE)),
  "",
  "## Excluded, on purpose",
  "",
  paste("These exist and look usable. They are not eligible and are recorded",
        "here so the absence reads as a decision rather than an oversight."),
  "",
  md_table(data.frame(
    Artifact = c("artifacts/demand_backtest_summary.csv",
                 "artifacts/demand_backtest_by_category.csv",
                 "artifacts/access_validation/*",
                 "artifacts/validation/*_EXPLORATORY/",
                 "artifacts/diagnostics/entrant_regime_bias_decomposition.csv",
                 "artifacts/diagnostics/interval_honesty_scorecard.csv"),
    Reason = c("its own anchors_source column reads `illustrative_fallback`",
               "same anchors; the 3.6% MAPE is against illustrative anchors",
               "belongs to a separate study, not this manuscript",
               "exploratory runs; refused by read_validation_run()",
               "no sidecar manifest and no run identity; see the note below",
               "no sidecar manifest and no run identity; see the note below"),
    check.names = FALSE)),
  "",
  paste("The last two are **the sources of the two tables currently in",
        "MANUSCRIPT.docx**. `scripts/diagnostics/` writes bare CSVs with",
        "`utils::write.csv()`; no diagnostics script calls",
        "`begin_validation_run()`, so those numbers have no model SHA, no input",
        "hashes and no COMPLETED marker. They are not wrong -- they are",
        "unattributable, which is a different problem and not one a renderer",
        "can fix. Converting the two scripts to manifest-first is what would",
        "bring the paper's existing tables under the same gate as the six",
        "above."),
  "",
  "## Build provenance",
  "",
  md_table(data.frame(
    Field = c("built", "head_sha", "model_sha", "validation_sha",
              "model tree clean", "R", "urpssim"),
    Value = c(prov$run_timestamp, substr(prov$head_sha, 1, 12),
              substr(prov$model_sha, 1, 12), substr(prov$validation_sha, 1, 12),
              as.character(prov$tree_clean_model), prov$r_version,
              prov$urpssim_version),
    check.names = FALSE))),
  file.path(OUT_DIR, "TABLE_INDEX.md"))

cat(sprintf("\nwrote %d files to %s\n", length(list.files(OUT_DIR)), OUT_DIR))
