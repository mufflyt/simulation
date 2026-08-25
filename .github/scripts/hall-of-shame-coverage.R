#!/usr/bin/env Rscript
# Every recorded scientific failure must have an enforcing test.
#
# THE INVARIANT IS A RATIO, NOT A COUNT. "We have N regression tests" says
# nothing about whether the defects we actually shipped can recur. What matters
# is
#
#     historical failures covered / historical failures total
#
# and it must reach the denominator. docs/HALL_OF_SHAME.md is a machine-
# readable registry only if something machine-reads it; otherwise it is a
# memoir.
#
# LINKING. A test claims an entry by naming it in its description:
#
#     test_that("@hall_of_shame 24 | an installed package is not the source tree", ...)
#
# The tag is the join key. Nothing else has to be maintained in parallel, which
# matters because a second hand-maintained list is itself entry 18's defect.
#
# WAIVERS. Some entries record PROCESS failures -- a fix applied twice in
# opposite directions, a status reported for a dead process, documentation
# regenerated in someone else's tree. Those have no executable law, and writing
# a fake test for them would be worse than admitting it: it would inflate the
# ratio while protecting nothing. tests/hall-of-shame-waivers.csv carries them
# with a reason each, and the gate reports waived entries separately so the
# number is never mistaken for coverage.
#
# Usage: hall-of-shame-coverage.R [--check]
#   default   print the inventory
#   --check   exit 1 if any entry is neither covered nor waived

arguments <- commandArgs(trailingOnly = TRUE)
enforce <- "--check" %in% arguments

registry_path <- "docs/HALL_OF_SHAME.md"
waiver_path <- "tests/hall-of-shame-waivers.csv"
test_dir <- file.path("tests", "testthat")

if (!file.exists(registry_path)) {
  stop("Hall of Shame registry not found: ", registry_path, call. = FALSE)
}

# ---- entries ---------------------------------------------------------------
registry_lines <- readLines(registry_path, warn = FALSE)
heading_lines <- grep("^### ", registry_lines, value = TRUE)
heading_text <- sub("^### ", "", heading_lines)

# Most entries are numbered ("12.", "4b."). A few were never numbered; they get
# a slug from their title so they are still addressable and still counted.
entry_id <- sub("^([0-9]+[a-z]?)\\..*$", "\\1", heading_text)
unnumbered <- entry_id == heading_text
entry_id[unnumbered] <- tolower(gsub(
  "[^a-z0-9]+", "-",
  tolower(substr(heading_text[unnumbered], 1L, 40L))
))
entry_id[unnumbered] <- gsub("(^-|-$)", "", entry_id[unnumbered])

entries <- data.frame(
  id = entry_id,
  title = sub("^[0-9]+[a-z]?\\.\\s*", "", heading_text),
  stringsAsFactors = FALSE
)
if (anyDuplicated(entries$id) > 0L) {
  stop(
    "Duplicate Hall of Shame entry ids: ",
    paste(unique(entries$id[duplicated(entries$id)]), collapse = ", "),
    call. = FALSE
  )
}

# ---- claims ----------------------------------------------------------------
test_files <- list.files(test_dir, pattern = "^test-.*[.]R$", full.names = TRUE)
claims <- list()
for (path in test_files) {
  lines <- readLines(path, warn = FALSE)
  matches <- regmatches(
    lines, gregexpr("@hall_of_shame[[:space:]]+([A-Za-z0-9-]+)", lines)
  )
  found <- unlist(matches, use.names = FALSE)
  if (!length(found)) next
  ids <- sub("^@hall_of_shame[[:space:]]+", "", found)
  for (id in ids) {
    claims[[id]] <- unique(c(claims[[id]], basename(path)))
  }
}

# ---- waivers ---------------------------------------------------------------
waivers <- data.frame(id = character(0), reason = character(0),
                      stringsAsFactors = FALSE)
if (file.exists(waiver_path)) {
  waivers <- utils::read.csv(waiver_path, stringsAsFactors = FALSE,
                             comment.char = "#")
  waivers$id <- as.character(waivers$id)
}

entries$covered_by <- vapply(entries$id, function(id) {
  if (is.null(claims[[id]])) "" else paste(claims[[id]], collapse = "; ")
}, character(1))
entries$waived <- entries$id %in% waivers$id
entries$covered <- nzchar(entries$covered_by)

total <- nrow(entries)
covered <- sum(entries$covered)
waived_only <- sum(entries$waived & !entries$covered)
uncovered <- entries[!entries$covered & !entries$waived, , drop = FALSE]

cat("== Hall of Shame coverage ==\n\n")
for (i in seq_len(total)) {
  mark <- if (entries$covered[i]) "COVERED" else if (entries$waived[i]) "waived " else "OPEN   "
  cat(sprintf("  [%s] %-28s %s\n", mark, entries$id[i],
              substr(entries$title[i], 1L, 60L)))
  if (entries$covered[i]) cat(sprintf("            %s\n", entries$covered_by[i]))
}

cat(sprintf(
  "\n  COVERAGE: %d/%d enforced by a test; %d waived as process-only; %d open\n",
  covered, total, waived_only, nrow(uncovered)
))

# A claim naming an entry that does not exist means the registry was edited and
# the tag was left behind, so the test now guards nothing identifiable.
orphan_claims <- setdiff(names(claims), entries$id)
if (length(orphan_claims) > 0L) {
  cat("\n")
  for (id in orphan_claims) {
    cat(sprintf(
      "::error::@hall_of_shame %s names no entry in %s (tag left behind by an edit?)\n",
      id, registry_path
    ))
  }
}

stale_waivers <- setdiff(waivers$id, entries$id)
if (length(stale_waivers) > 0L) {
  for (id in stale_waivers) {
    cat(sprintf("::error::waiver for %s names no registry entry\n", id))
  }
}

if (enforce) {
  problems <- nrow(uncovered) + length(orphan_claims) + length(stale_waivers)
  if (nrow(uncovered) > 0L) {
    cat("\n")
    for (i in seq_len(nrow(uncovered))) {
      cat(sprintf(
        "::error::Hall of Shame entry %s has no enforcing test and no waiver: %s\n",
        uncovered$id[i], uncovered$title[i]
      ))
    }
    cat("::error::Add a test tagged '@hall_of_shame <id>', or record a waiver in ",
        waiver_path, " with a reason.\n", sep = "")
  }
  if (problems > 0L) quit(status = 1L)
  cat("\n  Every recorded failure is enforced or explicitly waived.\n")
}
