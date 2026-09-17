# A FROZEN PRE-REGISTRATION MAY BE ANNOTATED, NEVER REWRITTEN.
#
# docs/INCIDENT_ENTRY_ESTIMAND.md declares itself frozen before data access, so
# that the definition cannot be adjusted once the answer is visible. That
# guarantee is worth exactly as much as the guarantee that its original text is
# still there.
#
# It has since been overtaken in four places by the later
# PATHWAY_STATE_TRANSITION_AUDIT.md §7-§8 ruling: §3 presents an already-settled
# choice as open, §9 calls the target a "hazard", §12 says §3 blocks the rest,
# and §14 lists resolving §3 as pending work. The repair is DATED ANNOTATION,
# not correction -- silently editing the paragraphs would leave a document that
# reads as though it always said the right thing, which is the failure mode a
# pre-registration exists to prevent.
#
# So this asserts both halves: the stale original text is still present, AND
# every stale site carries a dated post-freeze note.

.estimand_doc <- function() {
  f <- file.path(.repo_root(), "docs", "INCIDENT_ENTRY_ESTIMAND.md")
  skip_if_not(file.exists(f), "INCIDENT_ENTRY_ESTIMAND.md not present")
  # Wrap-insensitive, for the same reason as test-apcd-request-contract.R: the
  # file is hard-wrapped, so a phrase can fall across a line break.
  trimws(gsub("[[:space:]]+", " ",
              paste(readLines(f, warn = FALSE), collapse = " ")))
}

test_that("the frozen pre-registration retains its original overtaken wording", {
  doc <- .estimand_doc()
  originals <- c(
    "the choice is a modelling decision to be made",
    "age-specific entry hazard",
    "the open one and blocks the rest",
    "resolve §3 denominator"
  )
  for (phrase in originals) {
    expect_true(
      grepl(phrase, doc, fixed = TRUE),
      info = paste0(
        "'", phrase, "' has been removed from INCIDENT_ENTRY_ESTIMAND.md. ",
        "This document is frozen before data access: overtaken language is ",
        "annotated with a dated note, never deleted. Deleting it produces a ",
        "pre-registration that reads as though it always said the right thing."
      )
    )
  }
})

test_that("every overtaken passage carries a dated post-freeze clarification", {
  doc <- .estimand_doc()
  # One for the superseded "hazard acting on those eligible to enter" wording in
  # §2, plus §3, §9, §12 and §14.
  n <- length(gregexpr("POST-FREEZE CLARIFICATION, 2026-08-29", doc, fixed = TRUE)[[1]])
  expect_gte(n, 5L)

  # Each note must name what actually settled the question, or a reader cannot
  # tell an annotation from an opinion.
  expect_true(grepl("PATHWAY_STATE_TRANSITION_AUDIT.md", doc, fixed = TRUE))
  # And must state the correct canonical name, since three of the four stale
  # sites are stale precisely about naming.
  expect_true(grepl("annual_first_urps_entry_rate", doc, fixed = TRUE))
  expect_true(grepl("not a conditional hazard", doc, fixed = TRUE))
})
