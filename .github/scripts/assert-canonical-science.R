#!/usr/bin/env Rscript
# CANONICAL SCIENTIFIC-READINESS GATE.
#
# A DIFFERENT QUESTION FROM R CMD check, deliberately kept separate:
#
#   R CMD check          Does the package BEHAVE correctly?
#   this gate            Can the CANONICAL parameterization produce a
#                        scientifically valid run?
#
# The package can be entirely correct while the shipped configuration is not
# runnable, and conflating the two costs you both signals. R CMD check is green
# because the guards correctly refuse an invalid configuration and the tests
# assert that refusal. This gate is RED because the configuration is still
# invalid.
#
# It runs the REAL canonical pathway -- no fixture, no valid_pathway(), no
# substituted per_entering. If it ever passes without the parameter being
# sourced, something has been quietly loosened, so it also verifies that the
# refusal it expects is the one it actually got.
#
# WHAT MAKES IT GREEN: pop/conservative/new_consultation/per_entering estimated
# from longitudinal all-payer OUTPATIENT claims per the pre-registered
# estimator in docs/INCIDENT_ENTRY_ESTIMAND.md. Nothing else. Not a guessed
# incident share, not the 0.297 utilization ratio, not whatever value makes
# this script exit 0.
#
# EXIT CODES ARE DISTINCT ON PURPOSE. A permanently red job is worthless if a
# reader six months from now cannot tell "the known POP blocker" from "the
# workflow broke":
#
#   0  canonical configuration produces a valid run
#   1  KNOWN scientific blocker, identified by id  (the current, expected state)
#   2  UNEXPECTED failure -- this gate itself is broken, or the refusal is not
#      the one we documented. Treat as infrastructure, not as the POP blocker.
#
# Every blocker also emits a stable, greppable one-line marker:
#
#   ::SCIENTIFIC-BLOCKER:: id=<id> category=<category> param=<path> unblocked_by=<source>
#
# so a dashboard, a log search, or a future reader gets an unambiguous answer
# without parsing prose.
#
# IDS ARE PER LIMB, UNDER A SHARED CATEGORY. The defect is the same stock->flow
# error in UI, POP and AI, so `pop_incident_entry` as an umbrella would be false
# provenance -- it reads as "POP was the problem" when POP is merely where the
# discrepancy first became visible. Each limb gets its own id because each will
# be estimated separately and may resolve independently; `category=
# conservative_incident_entry` ties them together.

suppressMessages(pkgload::load_all(".", quiet = TRUE, export_all = TRUE))

blockers <- list()
note <- function(id, category, detail, param, unblocked_by) {
  blockers[[length(blockers) + 1L]] <<-
    list(id = id, category = category, detail = detail,
         param = param, unblocked_by = unblocked_by)
}
APCD <- "apcd_longitudinal_outpatient_claims"

cat("== canonical scientific readiness ==\n\n")

# ---------------------------------------------------------------------------
# 1. Each condition limb must produce service volumes from the canonical table.
#
# TESTED PER LIMB, not once on POP. Running only the POP cohort would have
# reported a POP-specific blocker for a defect that is independently present in
# UI and AI -- which is exactly the false provenance this gate exists to avoid.
# ---------------------------------------------------------------------------
canonical <- condition_service_pathway()          # THE REAL TABLE. Never a fixture.

for (cond in sort(unique(canonical$condition))) {
  n <- FROZEN_CARE_ENGAGED[[cond]]
  if (is.null(n) || !is.finite(n)) next
  vol <- tryCatch(
    pathway_service_volumes(treated = stats::setNames(unname(n), cond),
                            year = 2025L, pathway = canonical),
    error = function(e) conditionMessage(e))

  if (!is.character(vol)) {
    cat(sprintf("  PASS     %s limb produces service volumes\n", cond))
    next
  }
  cat(sprintf("  REFUSED  %s limb does not produce volumes\n", cond))
  cat("           ", vol, "\n", sep = "")
  # Confirm it is the EXPECTED refusal. A different error means something else
  # broke, and this gate must not absorb it as a known blocker.
  if (!grepl("NEW patient annually", vol, fixed = TRUE)) {
    cat("::UNEXPECTED-FAILURE:: the ", cond, " limb refused, but NOT by the\n", sep = "")
    cat("  documented incident-entry guard. This is infrastructure, not the\n")
    cat("  known scientific blocker. Diagnose it rather than assuming the\n")
    cat("  incident-entry parameter explains it.\n")
    quit(status = 2L)
  }
  note(paste0(cond, "_incident_entry"), "conservative_incident_entry",
       "per_entering at conservative entry is a stock, not an annual flow",
       sprintf("%s/conservative/new_consultation/per_entering", cond),
       APCD)
}
cat("\n")

# ---------------------------------------------------------------------------
# 2. No parameter the model depends on may remain declared-invalid.
# ---------------------------------------------------------------------------
invalid <- canonical[grepl("INVALID", canonical$source, fixed = TRUE), , drop = FALSE]
if (nrow(invalid) > 0L) {
  cat(sprintf("  REFUSED  %d pathway row(s) declare a parameter INVALID\n", nrow(invalid)))
  for (i in seq_len(nrow(invalid))) {
    cat(sprintf("           %s/%s/%s\n", invalid$condition[i], invalid$stage[i],
                invalid$service[i]))
  }
  cat("\n")
  note("declared_invalid_parameters", "conservative_incident_entry",
       sprintf("%d row(s) marked INVALID in the pathway table", nrow(invalid)),
       paste(sprintf("%s/%s/%s", invalid$condition, invalid$stage, invalid$service),
             collapse = ","),
       APCD)
} else {
  cat("  PASS     no pathway row declares an invalid parameter\n")
}

# ---------------------------------------------------------------------------
# 3. Verdict.
# ---------------------------------------------------------------------------
cat("\n")
if (length(blockers) == 0L) {
  cat("CANONICAL CONFIGURATION IS SCIENTIFICALLY RUNNABLE.\n")
  cat("If docs/INCIDENT_ENTRY_ESTIMAND.md still describes this as blocked, it is\n")
  cat("stale and should be updated, along with the fixtures that work around it:\n")
  cat("  .github/scripts/_pathway_fixture.R  and  valid_pathway() in helper-setup.R\n")
  quit(status = 0L)
}

cat("CANONICAL CONFIGURATION IS NOT SCIENTIFICALLY RUNNABLE.\n\n")
for (b in blockers) cat(sprintf("  %-30s %s\n", b$id, b$detail))
cat("\n")
# STABLE MACHINE-READABLE MARKERS. Grep for ::SCIENTIFIC-BLOCKER:: to answer
# "why is this red?" without reading anything else.
for (b in blockers) {
  cat(sprintf("::SCIENTIFIC-BLOCKER:: id=%s category=%s param=%s unblocked_by=%s\n",
              b$id, b$category, b$param, b$unblocked_by))
}
cat("\nThis is the DESIGNED state, not a build failure. R CMD check is green:\n")
cat("the package behaves correctly and the guards refuse the invalid\n")
cat("configuration exactly as they should. What is missing is the science.\n\n")
cat("Unblocked by: longitudinal all-payer OUTPATIENT claims, per the\n")
cat("pre-registered estimator in docs/INCIDENT_ENTRY_ESTIMAND.md.\n")
cat("NOT unblocked by: the CHIA inpatient extract, or by back-solving 0.297\n")
cat("from the utilization anchor.\n")
quit(status = 1L)
