#!/usr/bin/env Rscript
# Survivor conditioning in the retrospective series: figure and tables ----
#
#   Rscript scripts/plot_survivor_conditioning.R
#
# Everything here is generated from inst/extdata/survivor_falsification.json, so
# the figure, the tables and the manuscript sentence cannot disagree about a
# number. Rebuild the artifact with
# scripts/data_acquisition/09_build_survivor_falsification.R.
#
# WHY THE FIGURE LOOKS LIKE THIS. The comparison that matters is like with like:
# a SINGLE denominator -- urogynecologists observed billing Medicare Part B in a
# given year, having certified by that year -- split into those the 2025 roster
# retained and those it excluded. The excluded group is a literal subset of the
# same bar, not a second series on a second axis, because the claim is precisely
# that the retrospective series deletes part of its own denominator.
#
# The clinician-directory evidence is NOT in this figure. It is enrolment rather
# than billed care, it covers only the later years, and putting it beside Part B
# would invite exactly the tier merge the analysis exists to prevent. It appears
# in the supplemental table instead.

suppressMessages(pkgload::load_all(".", quiet = TRUE))

FIG    <- "figures/survivor_conditioning.png"
TAB    <- "figures/survivor_falsification_table.csv"
TAB_MD <- "figures/survivor_falsification_table.md"

a   <- survivor_falsification_artifact()
rec <- survivor_falsification_record(a)
tbl <- survivor_falsification_table(a)
stopifnot(assert_survivor_falsification(a, tbl, rec))

an  <- a$annual
val <- seq.int(a$windows$validation[1], a$windows$validation[2])

RET <- "#4A6FA5"
EXC <- "#C0392B"

dir.create(dirname(FIG), recursive = TRUE, showWarnings = FALSE)
grDevices::png(FIG, width = 1500, height = 850, res = 150)
op <- graphics::par(mar = c(7.2, 5.2, 4.2, 1.4), xpd = FALSE)

bp <- graphics::barplot(
  rbind(an$retained_observed, an$excluded_observed),
  names.arg = an$year, col = c(RET, EXC), border = NA,
  ylim = c(0, max(an$total_observed) * 1.30), las = 1,
  xlab = "", ylab = "URPS physicians observed billing Medicare Part B",
  main = "The retrospective series deletes physicians who were billing Medicare")

# Label the excluded slice, which is the quantity the retrospective series sets
# to zero in every one of these years.
graphics::text(bp, an$total_observed + max(an$total_observed) * 0.035,
               sprintf("%d", an$excluded_observed), col = EXC, cex = 0.72,
               font = 2)

# The validation window, and the persistent subgroup inside it.
vx <- bp[an$year %in% val]
graphics::rect(min(vx) - 0.5, 0, max(vx) + 0.5, max(an$total_observed) * 1.08,
               border = "grey35", lty = 3, lwd = 1.1)
graphics::text(mean(vx), max(an$total_observed) * 1.12,
               sprintf("validation window %d-%d", min(val), max(val)),
               cex = 0.72, col = "grey25")

graphics::legend(
  "topleft", bty = "n", cex = 0.78, fill = c(EXC, RET), border = NA, inset = c(0, 0),
  legend = c("EXCLUDED by the later active-roster adjudication",
             "Retained in the later active roster"))

graphics::mtext("Year", side = 1, line = 2.4, cex = 0.9)
graphics::mtext(
  paste("One denominator: certified by that year and observed billing.",
        "Red is a literal subset, not a separate series."),
  side = 1, line = 4.0, cex = 0.68)
graphics::mtext(
  sprintf(paste("Of %d excluded NPI-linked physicians, %d billed during %d-%d;",
                "%d billed in ALL SIX years = %d provider-years erased."),
          rec$linkage_denominator, rec$any_partb_window, min(val), max(val),
          rec$n_persistent_billers, rec$provider_years_erased),
  side = 1, line = 4.9, cex = 0.68)
graphics::mtext(
  "Clinician-directory (enrolment) evidence is excluded here; see the supplemental table.",
  side = 1, line = 5.8, cex = 0.63, col = "grey35")

graphics::par(op)
grDevices::dev.off()

# ---- tables ------------------------------------------------------------------

utils::write.csv(tbl, TAB, row.names = FALSE)

writeLines(survivor_falsification_markdown(a), TAB_MD)

cat(sprintf("wrote %s\n       %s\n       %s\n\n", FIG, TAB, TAB_MD))
cat(strwrap(survivor_falsification_statement(a), 78), sep = "\n")
cat("\n\n")
print(tbl, n = nrow(tbl))
