#!/usr/bin/env Rscript
# Manuscript figure: coverage rewards the wrong model; the interval score does not.
#
#   Rscript scripts/figures/fig_interval_calibration.R
#
# Plots the three real out-of-sample evaluations of the certification stock
# (2021-2023) in (empirical coverage, mean interval score) space, from the
# committed scorecard artifact. The eye should land on the split: coverage points
# RIGHT to the uninformative wide model; the interval score (lower is better)
# points DOWN to the definition-matched sharp model. Base R only.
#
# Writes artifacts/figures/interval_calibration.{png,pdf}.

csv <- Find(file.exists, c("artifacts/diagnostics/interval_honesty_scorecard.csv"))
if (is.null(csv)) stop("Run scripts/diagnostics/interval_honesty_scorecard.R first.", call. = FALSE)
d <- utils::read.csv(csv, stringsAsFactors = FALSE)

# Okabe-Ito colourblind-safe: green = best (matched), orange = wide, vermillion = mismatch.
key <- c("sharp, no-attrition (definition-MATCHED)" = "#009E73",
         "rolling-origin (wide)"                    = "#E69F00",
         "sharp, attrition ON (definition MISMATCH)"= "#D55E00")
short <- c("sharp, no-attrition (definition-MATCHED)" = "definition-matched\n(sharp)",
           "rolling-origin (wide)"                    = "rolling-origin\n(wide)",
           "sharp, attrition ON (definition MISMATCH)"= "attrition ON\n(definition mismatch)")
d$col <- key[d$label]; d$lab <- short[d$label]
d$x <- 100 * d$coverage
d$y <- d$mean_interval_score

render <- function(open, close) {
  open()
  op <- par(mar = c(4.6, 4.8, 3.6, 1.4), xpd = NA)
  on.exit({ par(op); close() }, add = TRUE)
  plot(d$x, d$y, log = "y", type = "n",
       xlim = c(-6, 106), ylim = c(90, 2600),
       xlab = "Empirical coverage of the 95% interval (%)",
       ylab = "Mean interval score  (lower is better)",
       main = "", axes = FALSE)
  axis(1, at = seq(0, 100, 25))
  axis(2, at = c(100, 200, 500, 1000, 2000), las = 1)
  box(bty = "l")
  title(main = "Coverage rewards the wrong model; the interval score does not",
        cex.main = 1.15, font.main = 2, line = 2.1)
  mtext("Three real out-of-sample forecasts of the URPS certification stock, 2021-2023",
        side = 3, line = 0.7, cex = 0.86, col = "grey30")
  abline(v = 95, lty = 3, col = "grey65")
  text(95, 2550, "nominal 95%", cex = 0.7, col = "grey55", pos = 2)

  # direction cues
  text(50, 2500, "coverage prefers this way ->", col = "#E69F00", cex = 0.82, font = 2)
  text(-6, 118, "interval score\nprefers down", col = "#009E73", cex = 0.82, font = 2, pos = 4)

  points(d$x, d$y, pch = 21, bg = d$col, col = "white", cex = 2.4, lwd = 2)
  lab2 <- sprintf("%s\ncoverage %.0f%%,  IS %d", d$lab, d$x, round(d$y))
  posv <- rep(3, nrow(d))
  posv[d$lab == short[["rolling-origin (wide)"]]] <- 1            # wide label below its point
  for (i in seq_len(nrow(d))) {
    text(d$x[i], d$y[i], lab2[i], pos = posv[i], offset = 1.4,
         cex = 0.8, col = d$col[i], font = 2)
  }
  mtext(paste0("Interval score = width + (2/alpha) x shortfall (Gneiting & Raftery 2007).",
               "  Source: interval_honesty_scorecard.csv"),
        side = 1, line = 3.4, cex = 0.66, col = "grey45", adj = 0)
}

dir.create("artifacts/figures", recursive = TRUE, showWarnings = FALSE)
render(function() grDevices::png("artifacts/figures/interval_calibration.png",
                                 width = 1500, height = 1050, res = 170),
       grDevices::dev.off)
render(function() grDevices::pdf("artifacts/figures/interval_calibration.pdf",
                                 width = 8.5, height = 6),
       grDevices::dev.off)
cat("Wrote artifacts/figures/interval_calibration.{png,pdf}\n")
