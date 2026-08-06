#!/usr/bin/env Rscript

# Validate WU2011_SURGERY_RATE_PER_1000 against Wu 2011's own published counts.
#
#   Rscript scripts/validate_wu2011_rates.R
#
# WHY THIS EXISTS
#
# The rates shipped before 2026-08-05 (1.5 / 4.6 / 6.3 / 3.2 per 1,000) were
# rounded whole-band totals with no component split and no derivation recorded.
# They overstated the source by ~16%, and nothing caught it, because a rate
# constant with a citation next to it looks checked. This is the check.
#
# THE TEST
#
# Wu 2011 (Am J Obstet Gynecol) publishes ABSOLUTE annual counts, not just
# rates: SUI 210,700 + POP 166,000 = 376,700 in 2010, rising to
# 310,050 + 245,970 = 555,020 in 2050 (+47.2%). Applying a candidate rate table
# to a real female population by age band must reproduce that series. Because
# the published series is 2010-2050 and the in-repo Census file (NPP 2023) begins
# at 2022, the anchor is linearly interpolated to 2022.
#
# That interpolation is the weakest link: Wu projected on a 2008-vintage Census
# series, so a few percent of disagreement is expected from the population input
# alone. It is still far more than enough to discriminate -- the corrected rates
# land within 1% and the old ones are off by 16%.
#
# Exit status is 0 when every component is within TOLERANCE, 1 otherwise, so
# this can gate a change to the constant.

suppressPackageStartupMessages(library(pkgload))
pkgload::load_all(normalizePath("."), quiet = TRUE)

TOLERANCE <- 0.05   # 5% -- looser than the 0.8% achieved, tighter than the 16% error

census <- file.path("data-raw", "census", "np2023_d1_mid.csv")
if (!file.exists(census)) {
  stop("Census NPP file not found at ", census,
       ". It is required to validate the rates against a real population.",
       call. = FALSE)
}

x <- utils::read.csv(census)
# SEX 2 = female; ORIGIN 0 / RACE 0 = all origins, all races (the total series).
f <- subset(x, x$SEX == 2 & x$ORIGIN == 0 & x$RACE == 0 & x$YEAR == 2022)
stopifnot(nrow(f) == 1)
band_pop <- function(lo, hi) sum(as.numeric(f[, paste0("POP_", lo:hi)]))

# Wu's published bands are 20-39 / 40-59 / 60-79 / >=80. The package splits
# 60-79 into 60-64 and 65-79 (both carry the published 60-79 rate), so the
# population is summed on the PUBLISHED bands to compare like with like.
pop <- c("20-39" = band_pop(20, 39), "40-59" = band_pop(40, 59),
         "60-79" = band_pop(60, 79), "80+"   = band_pop(80, 100))

comp <- WU2011_SURGERY_RATE_COMPONENTS
rate <- function(col, band) {
  b <- if (band == "60-79") "60-64" else band     # 60-64 and 65-79 are equal
  comp[[col]][comp$age_band == b]
}
sui_rates <- vapply(names(pop), function(b) rate("sui", b), numeric(1))
pop_rates <- vapply(names(pop), function(b) rate("pop", b), numeric(1))

computed_sui <- sum(pop * sui_rates / 1000)
computed_pop <- sum(pop * pop_rates / 1000)

interp <- function(v2010, v2050, year = 2022) {
  v2010 + (year - 2010) / (2050 - 2010) * (v2050 - v2010)
}
wu_sui   <- interp(210700, 310050)
wu_pop   <- interp(166000, 245970)
wu_total <- interp(376700, 555020)

err <- function(computed, published) computed / published - 1

rows <- data.frame(
  component = c("SUI", "POP", "TOTAL"),
  computed  = round(c(computed_sui, computed_pop, computed_sui + computed_pop)),
  published = round(c(wu_sui, wu_pop, wu_total)),
  error_pct = round(100 * c(err(computed_sui, wu_sui), err(computed_pop, wu_pop),
                            err(computed_sui + computed_pop, wu_total)), 2)
)

cat("2022 US female population by Wu band (NPP2023 mid), millions:\n")
print(round(pop / 1e6, 1))
cat("\nWu 2011 reproduction check (anchor interpolated to 2022):\n")
print(rows, row.names = FALSE)

worst <- max(abs(rows$error_pct)) / 100
cat(sprintf("\nworst component error: %.2f%% (tolerance %.0f%%)\n",
            100 * worst, 100 * TOLERANCE))

if (worst > TOLERANCE) {
  cat("\nFAIL: the shipped rates do not reproduce Wu 2011.\n")
  quit(status = 1)
}
cat("PASS: the shipped rates reproduce Wu 2011 within tolerance.\n")
