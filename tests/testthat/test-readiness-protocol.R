# THE READINESS GATE'S OWN PROTOCOL.
#
# assert-canonical-science.R is the one job expected to stay RED, so its exit
# semantics carry more weight than usual: they are the only thing distinguishing
# "the model correctly refuses to make an unsupported claim" from "the gate
# broke". A permanently red job whose meaning nobody can recover is worse than
# no job.
#
#   0  canonical configuration is scientifically runnable
#   1  KNOWN blocker, named by id            <- the expected state today
#   2  UNEXPECTED failure; the assessment itself cannot be trusted
#
# These tests pin that contract by RUNNING the script against constructed
# pathway files, not by reading it. A gate is only as good as its behaviour.

gate <- normalizePath(file.path("..", "..", ".github", "scripts",
                                "assert-canonical-science.R"), mustWork = FALSE)
pw_csv <- normalizePath(file.path("..", "..", "inst", "extdata", "pathway",
                                  "condition_service_pathway.csv"),
                        mustWork = FALSE)

# Run the gate against a temporarily substituted pathway table, always
# restoring the real one. The gate reads the INSTALLED extdata path, so the
# substitution has to happen on disk rather than by argument.
with_pathway <- function(tbl, code) {
  backup <- tempfile(fileext = ".csv")
  file.copy(pw_csv, backup, overwrite = TRUE)
  on.exit({
    file.copy(backup, pw_csv, overwrite = TRUE)
    unlink(backup)
  }, add = TRUE)
  utils::write.csv(tbl, pw_csv, row.names = FALSE)
  force(code)
}

run_gate <- function() {
  out <- suppressWarnings(
    system2(file.path(R.home("bin"), "Rscript"), gate,
            stdout = TRUE, stderr = TRUE))
  list(status = attr(out, "status") %||% 0L, text = paste(out, collapse = "\n"))
}

skip_if_not(file.exists(gate), "readiness gate script not present")
skip_if_not(file.exists(pw_csv), "pathway table not present (source tree absent)")

test_that("the canonical table exits 1 and names a blocker per affected limb", {
  skip_on_cran()
  r <- run_gate()
  expect_equal(r$status, 1L)
  # PER-LIMB IDS, NOT A POP UMBRELLA. The defect is independently present in
  # all three limbs; an id of pop_incident_entry alone would read as "POP was
  # the problem" when POP is only where it first became visible.
  for (cond in c("ui", "pop", "ai")) {
    expect_match(r$text, sprintf("id=%s_incident_entry", cond))
    expect_match(r$text, sprintf("param=%s/conservative/new_consultation/per_entering", cond),
                 fixed = TRUE)
  }
  expect_match(r$text, "category=conservative_incident_entry")
  expect_match(r$text, "unblocked_by=apcd_longitudinal_outpatient_claims")
  # exit 1 must NEVER be accompanied by the unexpected-failure marker
  expect_false(grepl("::UNEXPECTED-FAILURE::", r$text, fixed = TRUE))
})

test_that("the stock/flow rule is executable, not just documented", {
  skip_on_cran()
  # THE SCIENTIFIC BOUNDARY, made testable:
  #
  #   conservative + per_entering = 1.0 + entering is a STOCK -> invalid
  #   recurrence   + per_entering = 1.0 + entering is a FLOW  -> valid
  #
  # Both stages ship per_entering = 1.00 on new_consultation, so a rule of
  # "replace every 1.0" would be wrong. What separates them is what `entering`
  # counts. Asserted from the engine rather than from the CSV, so the claim is
  # about behaviour and not about a comment.
  ent <- pathway_stage_entrants(c(pop = 1000), condition_service_pathway())
  cons <- ent$entering[ent$stage == "conservative"]
  recur <- ent$entering[ent$stage == "recurrence"]
  expect_equal(cons, 1000)          # the whole prevalent cohort: a STOCK
  expect_lt(recur, cons)            # only this year's recurrences: a FLOW
  expect_gt(recur, 0)

  pw <- condition_service_pathway()
  nc <- pw[pw$service == "new_consultation", ]
  # every new_consultation row ships 1.00 -- the value alone does not identify
  # the defect
  expect_true(all(nc$per_entering == 1.00))
  # ...but only the conservative ones are declared invalid
  expect_true(all(grepl("INVALID", nc$source[nc$stage == "conservative"], fixed = TRUE)))
  expect_false(any(grepl("INVALID", nc$source[nc$stage == "recurrence"], fixed = TRUE)))
})

test_that("multiple known blockers are all reported, deterministically", {
  skip_on_cran()
  r <- run_gate()
  ids <- regmatches(r$text, gregexpr("id=[a-z_]+", r$text))[[1]]
  expect_true(all(c("id=ui_incident_entry", "id=pop_incident_entry",
                    "id=ai_incident_entry", "id=declared_invalid_parameters") %in% ids))
  # All three conservative-stage limbs carry the same stock-as-flow defect.
  # The recurrence rows do NOT: their `entering` is already an annual flow
  # (patients recurring this year), so one consultation per recurrence is
  # correct. Pinned so a future edit cannot quietly widen or narrow the set.
  expect_match(r$text, "ui/conservative/new_consultation")
  expect_match(r$text, "pop/conservative/new_consultation")
  expect_match(r$text, "ai/conservative/new_consultation")
  expect_false(grepl("recurrence/new_consultation", r$text, fixed = TRUE))
  expect_identical(run_gate()$text, r$text)   # deterministic
})

test_that("an UNRELATED defect exits 2, not 1", {
  skip_on_cran()
  # THE CONTROL THAT MATTERS. Without it, exit 1 would absorb any failure and
  # "known POP blocker" would become a synonym for "red".
  bad <- utils::read.csv(pw_csv, stringsAsFactors = FALSE)
  bad$per_entering[bad$condition == "pop" & bad$stage == "procedure"] <- -5
  r <- with_pathway(bad, run_gate())
  expect_equal(r$status, 2L)
  expect_match(r$text, "::UNEXPECTED-FAILURE::")
  expect_false(grepl("::SCIENTIFIC-BLOCKER::", r$text, fixed = TRUE))
})

test_that("a refusal with the wrong message exits 2, not 1", {
  skip_on_cran()
  # Distinct from the case above: here the run still fails, but for a reason
  # that is not the documented guard. The gate must not pattern-match loosely
  # enough to call that the known blocker.
  bad <- utils::read.csv(pw_csv, stringsAsFactors = FALSE)
  bad$p_advance[bad$condition == "pop" & bad$stage == "conservative"] <- 99
  r <- with_pathway(bad, run_gate())
  expect_equal(r$status, 2L)
  expect_match(r$text, "::UNEXPECTED-FAILURE::")
})

test_that("a valid miniature configuration exits 0", {
  skip_on_cran()
  # Proves the gate is a gate and not a wall. Without this, a script that
  # always exited 1 would be indistinguishable from a working one, and would
  # stay "correctly red" forever after the science was actually fixed.
  ok <- utils::read.csv(pw_csv, stringsAsFactors = FALSE)
  ok$per_entering[ok$service == "new_consultation" &
                    ok$stage == "conservative"] <- 0.25
  ok$source <- gsub("; per_entering INVALID", "", ok$source, fixed = TRUE)
  r <- with_pathway(ok, run_gate())
  expect_equal(r$status, 0L)
  expect_match(r$text, "SCIENTIFICALLY RUNNABLE")
  expect_false(grepl("::SCIENTIFIC-BLOCKER::", r$text, fixed = TRUE))
})

test_that("the real pathway table is intact after these tests", {
  skip_on_cran()
  # with_pathway() restores on exit, but a restore that silently failed would
  # leave a mutated canonical table in the working tree -- the worst possible
  # side effect of a test file. Assert it rather than trusting on.exit().
  pw <- utils::read.csv(pw_csv, stringsAsFactors = FALSE)
  cons <- pw[pw$service == "new_consultation" & pw$stage == "conservative", ]
  expect_true(all(cons$per_entering == 1.00))
  expect_equal(sum(grepl("per_entering INVALID", pw$source, fixed = TRUE)), 3L)
})

test_that("no workflow step can swallow a gate's exit code through a pipe", {
  skip_on_cran()
  # THE BUG THIS PINS. `run: Rscript gate.R | tee -a "$GITHUB_STEP_SUMMARY"`
  # reports the exit status of tee, not of the gate. scientific-readiness
  # printed "NOT SCIENTIFICALLY RUNNABLE" plus four ::SCIENTIFIC-BLOCKER::
  # markers and the job went GREEN. Twelve piped steps across three workflows
  # had the same shape.
  #
  # GitHub's default shell is `bash -e {0}` -- no pipefail. Declaring
  # `defaults.run.shell: bash` switches it to `-eo pipefail`. Any workflow that
  # pipes MUST declare it, or its gates cannot fail.
  root <- .source_tree_root()
  skip_if(length(root) == 0, "repository sources not present")
  wf <- list.files(file.path(root[1], ".github", "workflows"),
                   pattern = "[.]ya?ml$", full.names = TRUE)
  skip_if(length(wf) == 0, "no workflows present")

  offenders <- character(0)
  for (f in wf) {
    txt <- readLines(f, warn = FALSE)
    pipes <- grepl("\\|[[:space:]]*tee", txt)
    if (!any(pipes)) next
    has_pipefail <- any(grepl("shell:[[:space:]]*bash", txt)) ||
      any(grepl("pipefail", txt))
    if (!has_pipefail) offenders <- c(offenders, basename(f))
  }
  expect_equal(offenders, character(0),
               info = paste("workflow(s) pipe to tee without pipefail, so a",
                            "failing gate reports success:",
                            paste(offenders, collapse = ", ")))
})
