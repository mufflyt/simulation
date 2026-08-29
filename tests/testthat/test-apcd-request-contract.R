# THE DATA REQUEST MUST NOT DRIFT BACK TO A NEVER-ENTERED DENOMINATOR.
#
# docs/APCD_DATA_REQUEST.md is the document that will be turned into a DUA
# amendment and extraction SQL. Its §3 previously required subjects to be
# "not already in the pathway at the start of the observation year" and
# estimated an "annual transition probability" on that denominator -- the
# conditional never-entered hazard that PATHWAY_STATE_TRANSITION_AUDIT.md §8
# explicitly REJECTED, because that denominator is not directly observable and
# an APCD numerator over an external prevalence denominator does not estimate
# it.
#
# WHY THIS NEEDS A TEST RATHER THAN A CAREFUL READER. The failure is silent and
# expensive in a way ordinary documentation drift is not: the wording is
# plausible, it reads like a tightened cohort definition rather than a changed
# estimand, and it would be discovered only AFTER a multi-month DUA amendment
# delivered exactly the right claims for the wrong question. Every other guard
# in this repository fires in seconds; this one protects a decision whose
# feedback loop is months long.
#
# The canonical estimand is a POPULATION-LEVEL RATE: all eligible prevalent
# women in the year, regardless of prior care history. Prior care history and
# continuous enrolment classify the NUMERATOR; neither may restrict the
# denominator.

# The operative spec: the document minus its strikethrough (items recorded as
# NOT applied) and minus its blockquotes (provenance notes that must be able to
# quote the stale wording verbatim in order to reject it).
#
# Whitespace is also collapsed, because this is prose wrapped at 80 columns: a
# banned or required phrase can fall across a line break and silently satisfy
# either direction of the guard. That is not theoretical -- "annual
# population-level first-entry rate" was added to the document and the guard
# still reported it missing, because the wrap landed between the last two
# words. A phrase guard on hard-wrapped markdown must be wrap-insensitive or it
# is testing the line width.
.apcd_operative <- function(doc) {
  lines <- strsplit(doc, "\n", fixed = TRUE)[[1]]
  lines <- grep("^\\s*>", lines, value = TRUE, invert = TRUE)
  flat <- gsub("~~[^~]*~~", "", paste(lines, collapse = " "))
  # Strip inline emphasis and code markers. THE ORIGINAL STALE REQUIREMENT WAS
  # WRITTEN "4. **Not** already in the pathway...", so a literal phrase guard
  # that ignores markdown would have sailed straight past the exact text it
  # exists to forbid -- caught by mutation-testing this guard rather than by
  # reading it. Emphasis is the most natural thing in the world to add to a
  # requirement someone is reinstating, and it must not be a way through.
  # NOTE: `*` and backticks only. Underscore is NOT stripped -- it is emphasis
  # in markdown but it is also the word separator in every identifier this file
  # checks for (annual_first_urps_entry_rate, numerator_source), and removing it
  # made those unmatchable. Two guards went green-to-red on that alone.
  flat <- gsub("[*`]", "", flat)
  trimws(gsub("[[:space:]]+", " ", flat))
}

.apcd_request <- function() {
  f <- file.path(.repo_root(), "docs", "APCD_DATA_REQUEST.md")
  skip_if_not(file.exists(f), "APCD_DATA_REQUEST.md not present")
  paste(readLines(f, warn = FALSE), collapse = "\n")
}

test_that("the APCD request states the population-level denominator", {
  # Flattened for the same wrap-insensitivity reason as every other check here;
  # a required phrase must not pass or fail on where the line happens to break.
  doc <- .apcd_operative(.apcd_request())

  expect_match(doc, "regardless of prior care history", fixed = TRUE,
               info = paste(
                 "The denominator must be stated as ALL eligible prevalent women",
                 "regardless of prior care history. Without that phrase the",
                 "extraction is free to be read as a never-entered risk set."
               ))
  expect_match(doc, "annual_first_urps_entry_rate", fixed = TRUE,
               info = "the request must name the canonical estimand it serves")
  # "population-level rate, NOT a conditional hazard" is the distinction that
  # keeps a depletion correction from being applied on top of a rate that
  # already embeds depletion empirically.
  expect_match(doc, "population-level", ignore.case = TRUE,
               info = "the request must say the quantity is a population-level rate")
  expect_match(doc, "not a conditional hazard", ignore.case = TRUE, fixed = FALSE,
               info = "the request must say explicitly that it is NOT a conditional hazard")
})

test_that("the APCD request does not reimpose a never-entered risk set", {
  doc <- .apcd_request()

  # Check the OPERATIVE specification, not the document's own record of what it
  # corrected. Two constructs are excluded, and both are deliberate:
  #
  #   ~~struck through~~  -- items explicitly listed as NOT applied
  #   > blockquotes       -- the provenance notes, which must be free to QUOTE
  #                          the stale wording in order to say it was removed
  #
  # Without the second exclusion this guard forbids the document from recording
  # its own correction history, which is the opposite of what it is for. That
  # is not hypothetical: the first draft of this test failed on the very
  # paragraph explaining why the phrase is wrong -- the same trap as a guard in
  # nightly.yaml that was satisfied by the comment justifying it.
  live <- .apcd_operative(doc)

  banned <- c(
    "Not already in the pathway",
    "not already in the pathway",
    "never entered before",
    "never-entered denominator",
    "with no prior event"
  )
  for (phrase in banned) {
    expect_false(
      grepl(phrase, live, fixed = TRUE),
      info = paste0(
        "APCD_DATA_REQUEST.md reintroduces a never-entered denominator via '",
        phrase, "'. PATHWAY_STATE_TRANSITION_AUDIT.md §8 rejected that ",
        "estimand: its denominator is not directly observable and would ",
        "require persistent per-woman state. Prior care history belongs to ",
        "the NUMERATOR, where it establishes 'first observed'."
      )
    )
  }

  # "annual transition probability" named the rejected estimand. The
  # replacement wording must be present, not merely the old wording absent --
  # a document can drop a phrase and still say nothing correct in its place.
  expect_false(
    grepl("annual transition probability", live, fixed = TRUE),
    info = "'annual transition probability' is the rejected conditional-hazard framing"
  )
  expect_match(live, "first-entry rate", fixed = TRUE,
               info = "the replacement framing must be stated, not just the old one deleted")
})

test_that("the APCD request keeps enrolment and washout on the numerator side", {
  doc <- .apcd_request()
  live <- .apcd_operative(doc)

  # Continuous enrolment is an OBSERVABILITY condition. Requiring it of the
  # denominator silently swaps the external prevalence stock for a
  # claims-enrolled population -- a different, smaller, differently-selected
  # group, and the substitution is invisible in the resulting single ratio.
  expect_match(
    live, "does not define denominator membership|must not be required",
    info = paste(
      "The request must say explicitly that continuous enrolment does not",
      "define denominator membership. Stating it only as a data requirement",
      "leaves an analyst free to apply it to both sides."
    )
  )
  # The denominator must be sourced from the model's prevalence science.
  expect_match(live, "NOT from these claims|not from claims", ignore.case = TRUE,
               info = "the denominator must be stated as external to the claims extract")
  # Numerator and denominator provenance must stay separable: a state APCD
  # numerator over a national prevalence denominator is a category error that a
  # single ratio hides.
  expect_true(
    grepl("numerator_source", live, fixed = TRUE) &&
      grepl("denominator_source", live, fixed = TRUE),
    info = paste(
      "The request must reference the separate provenance arguments that",
      "annual_first_urps_entry_rate() requires -- population misalignment is",
      "invisible once the two are collapsed into one number."
    )
  )
})

test_that("the APCD request covers all three limbs and does not gate on Medicare FFS", {
  doc <- .apcd_request()
  live <- .apcd_operative(doc)

  # The canonical blocker names ui, pop AND ai. An earlier revision said the
  # unresolved parameter was on "both the pop and ui limbs", which would have
  # scoped the extraction to two of the three refusing limbs.
  expect_match(live, "UI, POP and AI", fixed = TRUE,
               info = "all three limbs refuse independently and all three must be requested")
  # Backtick-free: .apcd_operative() strips code markers, so the stale phrase
  # must be matched as it appears AFTER stripping. Written with backticks it
  # matched nothing and the two-limb mutant survived.
  expect_false(
    grepl("both the pop and ui limbs", live, fixed = TRUE),
    info = "the two-limb framing is stale; the blocker covers ui, pop and ai"
  )
  # The three-cohort statement must be in the NUMERATOR spec, not only in the
  # purpose line -- otherwise §3a can be narrowed to two limbs while the
  # document still says "UI, POP and AI" up top.
  expect_match(live, "Three parallel cohorts", fixed = TRUE,
               info = "§3a must specify three cohorts, one per refusing limb")

  # Medicare FFS must be desired-for-replication, not a required floor. MA APCD
  # -- this document's own recommended source -- does not include it, so an
  # all-payer requirement that names Medicare FFS disqualifies the extract the
  # same document recommends requesting.
  expect_match(live, "NOT disqualifying|not as an eligibility bar", ignore.case = TRUE,
               info = paste(
                 "Medicare FFS must be stated as desired-for-replication.",
                 "As a required floor it rejects MA APCD, which §5 recommends."
               ))
})
