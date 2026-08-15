#!/usr/bin/env Rscript
# Assert that this package's REFUSAL gates still refuse.
#
# Ordinary tests prove code works. This proves the guardrails still say NO --
# a distinct failure mode, because a gate that silently starts passing looks
# exactly like a gate that is working. Every check below asserts a REFUSAL, and
# fails if the refusal stops happening.
#
# The repository's whole methodological stance is that unsourced numbers must
# not be presentable as results. That stance is only real if these fire.

suppressMessages(pkgload::load_all(".", quiet = TRUE))

FAILURES <- character()
ok <- function(what) cat(sprintf("  PASS  %s\n", what))
bad <- function(what, why) {
  cat(sprintf("  FAIL  %s -- %s\n", what, why))
  FAILURES <<- c(FAILURES, what)
}

# refuses(expr): TRUE when expr errors. A gate that no longer errors is the
# regression we are hunting, so "did not error" is the failure.
refuses <- function(what, expr, pattern = NULL) {
  e <- tryCatch({ suppressMessages(force(expr)); NULL },
                error = function(e) conditionMessage(e))
  if (is.null(e)) return(bad(what, "did NOT refuse"))
  if (!is.null(pattern) && !grepl(pattern, e, ignore.case = TRUE))
    return(bad(what, sprintf("refused with an unexpected message: %s", substr(e, 1, 120))))
  ok(what)
}

cat("\n== 1. Publication gates ==\n")
refuses("uncalibrated demand coefficients are not publishable",
        assert_publishable_demand_coefficients(variant = "default", mode = "strict"),
        "uncalibrated|illustrative|publish")

refuses("an unsourced condition-service pathway is not publishable",
        assert_publishable_workload(status = condition_pathway_status(),
                                    what = "condition-service pathway",
                                    mode = "strict"),
        "uncalibrated|illustrative|publish")

refuses("a bare run is not publishable",
        assert_publishable_run(list(), artifact_path = NA_character_,
                               require_artifact = FALSE, mode = "strict"),
        "not publishable")

cat("\n== 2. The publication report names both parameter layers ==\n")
rep <- suppressMessages(publishable_run_report(list(), artifact_path = NA_character_,
                                               require_artifact = FALSE))
for (chk in c("demand_coefficients_publishable", "condition_service_pathway_publishable")) {
  row <- rep[rep$check == chk, ]
  if (nrow(row) != 1L) bad(chk, "check absent from the publication report")
  else if (isTRUE(row$passed[[1]])) bad(chk, "check PASSED on shipped placeholder parameters")
  else ok(chk)
}

cat("\n== 3. Clinical-review gate ==\n")
refuses("an anchor with no clinical-review block is refused",
        assert_anchor_reviewed(list()),
        "no clinical-review specification")
refuses("an unapproved anchor is refused",
        assert_anchor_reviewed(list(clinical_review = list(
          status = "needs_clinical_review", blockers = "unit_ambiguity"))),
        "not approved")
refuses("an anonymous approval is refused",
        assert_anchor_reviewed(list(clinical_review = list(
          status = "approved", reviewer = "", date = "2026-08-15"))),
        "no named reviewer")
refuses("an undated approval is refused",
        assert_anchor_reviewed(list(clinical_review = list(
          status = "approved", reviewer = "T Muffly", date = ""))),
        "no date")

cat("\n== 4. Production-scalar provenance ==\n")
refuses("an illustrative prediction cannot produce a production scalar",
        compute_production_scalar(140762, list(
          estimand_id = "prolapse_procedure_volume", prediction = 1e5,
          model_run_id = "smoke", model_version = "test",
          artifact_path = NA_character_, artifact_sha256 = NA_character_,
          generated_utc = NA_character_, prediction_status = "illustrative")),
        "non-production prediction")

cat("\n== 5. Care-engagement Gate 4 stays RED ==\n")
p <- care_engagement_params()
for (nm in c("incident_share", "first_year_followup_rate", "annual_followup_rate")) {
  row <- p[p$parameter == nm, ]
  if (!identical(row$calibration_status, "requires_source") || !is.na(row$value))
    bad(sprintf("%s is unsourced", nm),
        "a value appeared without the gate being cleared -- see docs/, MEPS was exhausted")
  else ok(sprintf("%s remains unsourced", nm))
}

cat("\n== 6. Anchor integrity detects tampering ==\n")
d <- file.path(tempdir(), "gate_tamper")
unlink(d, recursive = TRUE)
dir.create(file.path(d, "data/anchors"), recursive = TRUE, showWarnings = FALSE)
dir.create(file.path(d, "config"), recursive = TRUE, showWarnings = FALSE)
invisible(file.copy("config/calibration_targets.yml", file.path(d, "config/")))
for (f in list.files("data/anchors", "\\.csv$", full.names = TRUE))
  invisible(file.copy(f, file.path(d, "data/anchors/")))
tf <- file.path(d, "data/anchors/prolapse_procedure_volume.csv")
x <- readLines(tf); x[2] <- sub("140762", "140763", x[2]); writeLines(x, tf)
refuses("a one-character edit to an anchor is caught",
        verify_calibration_anchors(root = d, strict = TRUE),
        "integrity failed")

cat("\n== 7. Provenance mirror is still synchronised ==\n")
wl <- yaml::read_yaml("config/service_workload.yml")
mirror <- list(
  list("indirect_time_share", wl$indirect_time_share$value, INDIRECT_TIME_SHARE),
  list("level_correction", wl$delegation_shares$level_correction,
       URPS_DELEGATION_CAPACITY_FACTOR),
  list("benchmark_median", wl$productivity_benchmark$median,
       unname(WRVU_PER_FTE_BENCHMARK[["median"]])))
for (m in mirror) {
  if (!isTRUE(all.equal(m[[2]], m[[3]])))
    bad(sprintf("mirror %s", m[[1]]),
        sprintf("YAML %s != code %s -- update both together", m[[2]], m[[3]]))
  else ok(sprintf("mirror %s", m[[1]]))
}

cat("\n== 8. The POP anchor discrepancy is still reported, not silently scaled ==\n")
pw <- condition_service_pathway()
pop <- pw[pw$condition == "pop", ]
proc <- pop[pop$stage == "procedure" & pop$service == "prolapse_procedure", ]
if (!isTRUE(all.equal(proc$per_entering[[1]], 1.0))) {
  bad("no terminal scalar on the POP procedure",
      sprintf("per_entering is %s; a terminal scalar would hide exactly here",
              proc$per_entering[[1]]))
} else ok("no terminal scalar on the POP procedure")

v <- pathway_service_volumes(treated = c(pop = unname(FROZEN_CARE_ENGAGED[["pop"]])),
                             year = 2025L, pathway = pw)
predicted <- sum(v$volume[v$service == "prolapse_procedure"])
anchor <- utils::read.csv("data/anchors/prolapse_procedure_volume.csv")$observed[[1]]
ratio <- predicted / anchor
cat(sprintf("  INFO  POP predicted %s vs anchor %s = %.2fx\n",
            format(round(predicted), big.mark = ","),
            format(anchor, big.mark = ","), ratio))
if (ratio < 1.5) {
  bad("the POP discrepancy is still openly reported",
      sprintf("ratio collapsed to %.2fx -- if a parameter was genuinely sourced this is good news, but it must be a deliberate, documented change, not a scalar", ratio))
} else ok("the POP discrepancy is still openly reported")

cat("\n")
if (length(FAILURES)) {
  cat(sprintf("::error::%d REFUSAL GATE(S) STOPPED REFUSING: %s\n",
              length(FAILURES), paste(FAILURES, collapse = "; ")))
  cat("A gate that silently starts passing is indistinguishable from a gate that works.\n")
  quit(status = 1)
}
cat("ALL REFUSAL GATES STILL REFUSE.\n")
