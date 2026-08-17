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
# 3. REGISTER INTEGRITY.
#
# These are EXIT 2, not exit 1, and the distinction matters. Exit 1 means "the
# science is unresolved and we know it". A register that claims compatibility
# with no evidence behind it means THE ASSESSMENT ITSELF CANNOT BE TRUSTED --
# the same category as a broken gate. Reporting that as a known scientific
# blocker would be the most dangerous failure available here, because it would
# look routine.
#
# Every check below traces to a specific way the "false by default" discipline
# could be defeated silently.
# ---------------------------------------------------------------------------
integrity <- character(0)
flag <- function(...) integrity <<- c(integrity, paste0(...))

# Resolve config/ from the SOURCE TREE, not from the caller's working
# directory. The gate is invoked both from the repository root (CI) and from
# tests/testthat (the protocol tests), and a bare "config/..." silently
# returned NULL in the second case -- which made every integrity check pass
# vacuously. Found by the protocol tests failing, which is what they are for.
.repo_dir <- local({
  cand <- c(".", "..", file.path("..", ".."), file.path("..", "..", ".."))
  hit <- NA_character_
  for (p in cand) {
    if (file.exists(file.path(p, "DESCRIPTION")) && dir.exists(file.path(p, "config"))) {
      hit <- p; break
    }
  }
  hit
})

.read_reg <- function(rel) {
  if (is.na(.repo_dir)) return(NULL)
  p <- file.path(.repo_dir, "config", rel)
  if (!file.exists(p)) return(NULL)
  utils::read.csv(p, comment.char = "#", stringsAsFactors = FALSE)
}

# 3a. A compatible row must carry evidence. Flipping the flag is the cheapest
#     possible way to smuggle an unsourced parameter into the model.
rec <- .read_reg("recurrence_evidence.csv")
if (!is.null(rec)) {
  ok <- rec[!is.na(rec$kernel_compatible) & as.logical(rec$kernel_compatible), , drop = FALSE]
  for (i in seq_len(nrow(ok))) {
    if (!nzchar(trimws(ok$source[i])) || !nzchar(trimws(ok$endpoint[i])) ||
        is.na(ok$value[i]) || !nzchar(trimws(as.character(ok$value[i])))) {
      flag(sprintf("recurrence_evidence: %s/%s is kernel_compatible=TRUE but has no %s",
                   ok$condition[i], ok$parameter[i],
                   if (!nzchar(trimws(ok$source[i]))) "source" else
                   if (!nzchar(trimws(ok$endpoint[i]))) "endpoint" else "value"))
    }
  }
  # 3b. TWO measure types have NO conversion route to g_k, and neither may ever
  #     be marked compatible:
  #
  #       repeat_treatment_rate  -- a retreatment proportion is not recurrence
  #       unsupported_or_unknown -- by definition unconvertible
  #
  #     The second was MISSING from the first version of this check, and a
  #     negative control caught it: flipping the 0.12 row to TRUE passed,
  #     because that row HAS a source, an endpoint and a value -- they are
  #     simply the wrong ones. Checking only for empty fields therefore misses
  #     the realistic failure, which is a well-documented row whose documented
  #     content is incompatible.
  no_route <- c("repeat_treatment_rate", "unsupported_or_unknown")
  bad <- ok[ok$measure_type %in% no_route, , drop = FALSE]
  for (i in seq_len(nrow(bad))) {
    flag(sprintf(
      "recurrence_evidence: %s/%s is kernel_compatible=TRUE with measure_type '%s', which has NO conversion route to g_k",
      bad$condition[i], bad$parameter[i], bad$measure_type[i]))
  }
  # 3c. An unknown measure_type means the constraint has been bypassed.
  permitted <- c("discrete_hazard", "cumulative_incidence",
                 "first_recurrence_probability_mass", "repeat_treatment_rate",
                 "unsupported_or_unknown")
  unk <- setdiff(unique(rec$measure_type), permitted)
  if (length(unk) > 0L) {
    flag("recurrence_evidence: unrecognised measure_type(s): ",
         paste(unk, collapse = ", "))
  }
}

# 3d. Same discipline on the AI treatment register.
ai <- .read_reg("ai_treatment_evidence.csv")
if (!is.null(ai)) {
  okai <- ai[!is.na(ai$canonical_compatible) & as.logical(ai$canonical_compatible), , drop = FALSE]
  for (i in seq_len(nrow(okai))) {
    if (!nzchar(trimws(okai$denominator[i])) || is.na(okai$denominator_n[i])) {
      flag(sprintf("ai_treatment_evidence: %s is canonical_compatible=TRUE with no stated denominator",
                   okai$treatment[i]))
    }
  }
  # A derived conditional mix can never be canonical -- its denominator is the
  # wrong one for a pathway transition by construction.
  der <- ai[grepl("received SNM or sphincteroplasty", ai$quantity), , drop = FALSE]
  if (nrow(der) > 0L && any(as.logical(der$canonical_compatible), na.rm = TRUE)) {
    flag("ai_treatment_evidence: the DERIVED conditional treatment mix is marked ",
         "canonical. Its denominator is patients-treated-with-either, which is ",
         "not a pathway denominator.")
  }
}

# 3e. The claims basket must stay unwired, and anal_procedures must stay empty
#     until its composition is actually defined rather than assembled.
bp <- if (is.na(.repo_dir)) "" else file.path(.repo_dir, "config", "ai_claims_basket.yml")
if (file.exists(bp) && requireNamespace("yaml", quietly = TRUE)) {
  b <- yaml::read_yaml(bp)
  ap <- b$treatments$anal_procedures
  if (length(ap$cpt) > 0L && identical(ap$status, "NOT_DEFINED")) {
    flag("ai_claims_basket: anal_procedures has codes but is still marked ",
         "NOT_DEFINED. Either it was defined and the status is stale, or codes ",
         "were added without defining the category.")
  }
  # Every code must carry a label. An unlabelled code cannot be reviewed.
  walk_codes <- function(x, path = "") {
    if (is.list(x) && !is.null(x$code)) {
      if (is.null(x$label) || !nzchar(x$label)) {
        flag("ai_claims_basket: code ", x$code, " has no label")
      }
      return(invisible(NULL))
    }
    if (is.list(x)) for (nm in names(x)) walk_codes(x[[nm]], paste0(path, "/", nm))
    invisible(NULL)
  }
  walk_codes(b)
}

# 3f. The test/CI fixtures must never be reachable from library code. They
#     substitute a plausible value for an unresolved one, which is safe only
#     while nothing in R/ can see them.
r_files <- if (is.na(.repo_dir)) character(0) else
  list.files(file.path(.repo_dir, "R"), pattern = "[.]R$", full.names = TRUE)
for (f in r_files) {
  src <- readLines(f, warn = FALSE)
  code <- sub("#.*$", "", src)
  if (any(grepl("ci_pathway_fixture|valid_pathway\\(", code))) {
    flag("library code ", basename(f), " references a TEST FIXTURE. Fixtures ",
         "substitute plausible values for unresolved ones and must stay out of R/.")
  }
}

if (length(integrity) > 0L) {
  cat("\n")
  cat("::UNEXPECTED-FAILURE:: register/basket integrity is broken.\n")
  for (m in integrity) cat("  - ", m, "\n", sep = "")
  cat("\nThis is INFRASTRUCTURE, not a known scientific blocker: the registers\n")
  cat("are what the blocker classification is derived FROM, so a register that\n")
  cat("lies makes the whole assessment untrustworthy.\n")
  quit(status = 2L)
}
cat("  PASS     register and basket integrity\n")

# ---------------------------------------------------------------------------
# 4. Verdict.
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
