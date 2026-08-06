#!/usr/bin/env Rscript
# Emit BACKTEST_RECORD_2020_2023 from the scored artifact ----
#
#   Rscript scripts/diagnostics/emit_backtest_record.R
#
# WHY THIS EXISTS. `BACKTEST_RECORD_2020_2023` in R/validation-backtest_status.R is a
# hand-transcribed copy of artifacts/backtest_2020_to_2023_summary.csv, carried
# in the package because artifacts/ is .Rbuildignore'd and the status stamp must
# travel with an installed build. Hand transcription is a silent-error channel:
# a mistyped digit produces a status that no artifact supports, and the only
# thing standing between that and a shipped projection is one test.
#
# This prints the tribble to paste, derived from the CSV, so the transcription
# step cannot introduce a value the artifact does not contain. It does NOT edit
# R/validation-backtest_status -- the record is deliberately a reviewed constant, not generated code.
#
# The arm labels are shortened the same way every time, so re-running after a
# re-scored back-test produces a minimal diff.

f <- "artifacts/backtest_2020_to_2023_summary.csv"
if (!file.exists(f)) stop("No scored artifact at ", f, call. = FALSE)
s <- utils::read.csv(f, stringsAsFactors = FALSE)

short <- function(arm) {
  n <- sub("^([0-9]+)\\..*$", "\\1", arm)
  cohort <- if (grepl("Synthetic", arm)) "Synthetic cohort" else "Derived cohort"
  matched <- grepl("no-attrition", arm, fixed = TRUE)
  kind <- if (grepl("NRMP", arm)) "pre-cutoff NRMP entrants" else if
    (grepl("pre-2021", arm)) "pre-cutoff entrants" else "assumed entrants"
  if (matched) sprintf("%s. %s [no-attrition]", n, cohort)
  else sprintf("%s. %s, %s", n, cohort, kind)
}

lab <- vapply(s$arm, short, character(1))
w <- max(nchar(lab)) + 2L

cat("BACKTEST_RECORD_2020_2023 <- tibble::tribble(\n")
cat(sprintf("  %-*s %15s, %11s, %11s,\n", w, "~arm,", "~percent_error", "~within_80", "~within_95"))
for (i in seq_len(nrow(s))) {
  cat(sprintf('  %-*s %15.6f, %11s, %11s%s\n', w, paste0('"', lab[i], '",'),
              s$percent_error[i], toupper(as.character(s$within_80[i])),
              toupper(as.character(s$within_95[i])),
              if (i < nrow(s)) "," else ""))
}
cat(")\n\n")

cat("# Derived facts for the surrounding narrative:\n")
cat(sprintf("#   arms                %d\n", nrow(s)))
cat(sprintf("#   coverage 95         %.0f%% (%d/%d)\n", 100 * mean(s$within_95),
            sum(s$within_95), nrow(s)))
cat(sprintf("#   coverage 80         %.0f%%\n", 100 * mean(s$within_80)))
cat(sprintf("#   error range         %.2f%% to %.2f%%\n",
            min(s$percent_error), max(s$percent_error)))
cat(sprintf("#   all same direction  %s\n",
            length(unique(sign(s$percent_error))) == 1L))
w95 <- s$pi95_upper - s$pi95_lower
cat(sprintf("#   PI95 widths         %.0f-%.0f\n", min(w95), max(w95)))
best <- which.min(abs(s$percent_error))
cat(sprintf("#   most accurate arm   %s (%.2f%%)\n", lab[best], s$percent_error[best]))
cat(sprintf("#   entrant rates       %s\n",
            paste(sort(unique(round(s$entrants_per_year, 2))), collapse = ", ")))

# The checksum must move with the record; BACKTEST_RECORD_SHA256 is what lets an
# installed build detect that the artifact was regenerated underneath it.
sha <- tryCatch(digest::digest(file = f, algo = "sha256"),
                error = function(e) NA_character_)
cat(sprintf('\nBACKTEST_RECORD_SHA256 <- "%s"\n', sha))
