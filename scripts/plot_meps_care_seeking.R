#!/usr/bin/env Rscript
# Fit the MEPS two-part care-seeking model and plot it.
#
#   Rscript scripts/plot_meps_care_seeking.R
#
# Downloads MEPS 2023 (HC-248G office-based events, plus FYC/COND/CLNK) through
# the MEPS R package on first run and caches the frames under data-raw/meps/,
# which is build- and git-ignored. Writes two figures:
#
#   figures/meps_care_seeking_multipliers.png  what the data can and cannot identify
#   figures/meps_care_seeking_comorbidity.png  the gradient that actually carries demand
#
# ---------------------------------------------------------------------------
# HOW THESE FIGURES ARE MADE
# ---------------------------------------------------------------------------
#
# DATA LINEAGE (all four files, MEPS 2023 public-use, via the MEPS R package):
#   HC-248G  OB    office-based events   -- EVNTIDX, OBXP23X
#   HC-243   FYC   full-year consolidated-- DUPERSID, SEX, AGELAST, RACETHX,
#                                            POVCAT23, INSURC23, PERWT23F,
#                                            VARPSU, VARSTR
#   HC-241   COND  conditions            -- CONDIDX, ICD10CDX
#   HC-241IF CLNK  condition-event link  -- CONDIDX <-> EVNTIDX
#
#   A pelvic-floor visit = an office-based event linked (COND -> CLNK -> OB) to a
#   condition whose 3-character ICD-10 is N39 / N81 / R32 / R15. MEPS truncates
#   ICD-10 to three characters, so these are prefixes by construction.
#   unique() is applied before the OB join: one visit routinely carries the same
#   condition on several COND records, and counting those twice would inflate the
#   visit count for exactly the women the model is about.
#
# ESTIMATION (build_meps_care_seeking_panel + fit_care_seeking_model, R/data-meps_care_seeking):
#   Design  svydesign(id = ~VARPSU, strata = ~VARSTR, weights = ~PERWT23F,
#                     nest = TRUE), options(survey.lonely.psu = "adjust")
#   Part 1  sought ~ age_c + insurance + income + race + n_comorbid,
#           quasibinomial, all adult women
#   Part 2  pf_visits ~ age_c + insurance + income + n_comorbid,
#           quasipoisson, care-seekers ONLY
#   Multipliers are ratios of PREDICTED PROBABILITIES from part 1 at the
#   reference woman, with delta-method intervals via survey::SE(); the lower
#   limit is clamped at zero because a negative multiplier is not a quantity.
#
# FIGURE 1  meps_care_seeking_multipliers.png  (8.2 x 6.2 in, 200 dpi)
#   Form      points + horizontal intervals, faceted by covariate, reference
#             line at 1.0. NOT bars: a bar encodes distance from zero, and these
#             are ratios whose null is 1.0, so a bar would draw the wrong
#             comparison. The interval, not the point, is the content.
#   Encoding  blue solid = interval excludes 1.0 (the data identifies it);
#             muted gray hollow = interval covers 1.0. Colour is reinforced by
#             point shape, so the reading never rests on colour alone.
#   Labels    direct value labels ONLY on identified estimates -- labelling every
#             point would be noise on intervals this wide.
#
# FIGURE 2  meps_care_seeking_comorbidity.png  (9.4 x 4.3 in, 200 dpi)
#   Form      small multiples with free y-axes. The three quantities carry
#             different units and ranges (a probability, a count, their product);
#             a shared axis or a second y-axis would misstate all three.
#   Labels    endpoints only (0 and 12 conditions).
#
# PALETTE   series #2a78d6, muted ink #898781 on surface #fcfcfb. The series hue
#   passes the six-check validator (lightness band, chroma floor, CVD separation,
#   normal-vision floor, contrast >= 3:1) against that surface. The gray is INK
#   for de-emphasis, not a second categorical series.
#
# REPRODUCE  Rscript scripts/plot_meps_care_seeking.R
#   Writes both PNGs plus data-raw/meps/meps_2023_care_seeking_manifest.txt,
#   which records the sample sizes and every plotted estimate so the numbers
#   quoted in README.md trace to a generated artifact rather than to prose.

suppressMessages({
  library(ggplot2)
  pkgload::load_all(".", quiet = TRUE)
})

CACHE <- "data-raw/meps"
FIGS  <- "figures"
dir.create(CACHE, recursive = TRUE, showWarnings = FALSE)
dir.create(FIGS, recursive = TRUE, showWarnings = FALSE)

# ---- Design tokens ---------------------------------------------------------
# One categorical hue for "the data identifies this", muted ink for "it does
# not". The gray is INK, not a second series colour -- the distinction is
# emphasis, not identity -- and it is reinforced by point shape so the encoding
# never rests on colour alone.
SERIES  <- "#2a78d6"
MUTED   <- "#898781"
SURFACE <- "#fcfcfb"
GRID    <- "#e1e0d9"
BASE    <- "#c3c2b7"
INK     <- "#0b0b0b"
INK2    <- "#52514e"

theme_urps <- function() {
  theme_minimal(base_size = 11) +
    theme(
      plot.background   = element_rect(fill = SURFACE, colour = NA),
      panel.background  = element_rect(fill = SURFACE, colour = NA),
      panel.grid.minor  = element_blank(),
      panel.grid.major  = element_line(colour = GRID, linewidth = 0.3),
      axis.line.x       = element_line(colour = BASE, linewidth = 0.4),
      axis.ticks        = element_blank(),
      axis.text         = element_text(colour = MUTED),
      axis.title        = element_text(colour = INK2, size = 9.5),
      strip.text        = element_text(colour = INK2, face = "bold", size = 9.5, hjust = 0),
      plot.title        = element_text(colour = INK, face = "bold", size = 13),
      plot.subtitle     = element_text(colour = INK2, size = 9.5, lineheight = 1.15),
      plot.caption      = element_text(colour = MUTED, size = 8, hjust = 0),
      legend.position   = "top",
      legend.title      = element_blank(),
      legend.text       = element_text(colour = INK2, size = 9)
    )
}

# ---- Data ------------------------------------------------------------------
meps_file <- function(ty) file.path(CACHE, sprintf("meps_%s_2023.rds", ty))
get_meps <- function(ty) {
  f <- meps_file(ty)
  if (!file.exists(f)) {
    if (!requireNamespace("MEPS", quietly = TRUE)) {
      stop("MEPS package required: remotes::install_github('e-mitchell/meps_r_pkg/MEPS')",
           call. = FALSE)
    }
    message("downloading MEPS 2023 ", ty, " ...")
    saveRDS(MEPS::read_MEPS(year = 2023, type = ty), f)
  }
  readRDS(f)
}

panel <- build_meps_care_seeking_panel(
  fyc = get_meps("FYC"), cond = get_meps("COND"),
  clnk = get_meps("CLNK"), ob = get_meps("OB"), year = 2023L)
model <- fit_care_seeking_model(panel)
print(model)

ref <- data.frame(
  age_c      = 0,
  insurance  = factor("Private",  levels = levels(panel$insurance)),
  income     = factor("GE400FPL", levels = levels(panel$income)),
  race       = factor("NH_White", levels = levels(panel$race)),
  n_comorbid = 2)

# ---- Figure 1: multipliers and their identifiability ------------------------
LAB <- c(insurance = "Insurance", income = "Income (% FPL)",
         race = "Race / ethnicity")
# Raw MEPS category codes are not axis labels.
PRETTY <- c(GE400FPL = ">=400%", `200_399FPL` = "200-399%", `100_199FPL` = "100-199%",
            LT100FPL = "<100%", NH_White = "White (NH)", NH_Black = "Black (NH)",
            NH_Asian = "Asian (NH)", NH_Other = "Other (NH)", Hispanic = "Hispanic",
            Private = "Private", Public = "Public", Uninsured = "Uninsured")
mult <- do.call(rbind, lapply(names(LAB), function(v) {
  m <- care_seeking_multipliers(model, v, ref); m$panel <- LAB[[v]]; m
}))
mult$panel <- factor(mult$panel, levels = unname(LAB))
mult$level <- unname(ifelse(mult$level %in% names(PRETTY), PRETTY[mult$level], mult$level))
mult$level <- factor(mult$level, levels = rev(unique(mult$level)))
mult$state <- ifelse(mult$identified, "Distinguishable from 1.0",
                     "Not distinguishable from 1.0")

p1 <- ggplot(mult, aes(x = multiplier, y = level, colour = state, shape = state)) +
  geom_vline(xintercept = 1, colour = BASE, linewidth = 0.5) +
  geom_errorbarh(aes(xmin = conf_low, xmax = conf_high), height = 0, linewidth = 0.9) +
  geom_point(size = 2.6, fill = SURFACE, stroke = 0.9) +
  # Direct labels only on the estimates the data supports; labelling every point
  # would be noise on intervals this wide.
  geom_text(data = subset(mult, identified),
            aes(label = sprintf("%.2f", multiplier)),
            vjust = -1.15, size = 3.1, colour = INK, show.legend = FALSE) +
  scale_colour_manual(values = c("Distinguishable from 1.0" = SERIES,
                                 "Not distinguishable from 1.0" = MUTED)) +
  scale_shape_manual(values = c("Distinguishable from 1.0" = 16,
                                "Not distinguishable from 1.0" = 21)) +
  facet_grid(panel ~ ., scales = "free_y", space = "free_y", switch = "y") +
  labs(
    title = "What MEPS can and cannot say about who seeks pelvic-floor care",
    subtitle = sprintf(paste("Care-seeking rate relative to the reference level, with 95%% intervals.",
                             "\n%s adult women, %s carrying a pelvic-floor ambulatory visit."),
                       format(model$n_persons, big.mark = ","),
                       format(model$n_events, big.mark = ",")),
    x = "Care-seeking multiplier (reference = 1.0)", y = NULL,
    caption = paste0("MEPS 2023 (HC-248G office-based events joined to FYC/COND/CLNK); ",
                     "survey-weighted logistic model.\nIntervals crossing 1.0 are shown muted: ",
                     "the sample does not support treating them as measured effects.")
  ) +
  theme_urps() + theme(strip.placement = "outside")

ggsave(file.path(FIGS, "meps_care_seeking_multipliers.png"), p1,
       width = 8.2, height = 6.2, dpi = 200, bg = SURFACE)

# ---- Figure 2: the comorbidity gradient ------------------------------------
grid_df <- do.call(rbind, lapply(0:12, function(k) { d <- ref; d$n_comorbid <- k; d }))
pred <- cbind(n_comorbid = 0:12, predict_care_seeking(model, grid_df))

long <- rbind(
  data.frame(n_comorbid = pred$n_comorbid, value = pred$p_seek,
             panel = "P(any pelvic-floor visit)"),
  data.frame(n_comorbid = pred$n_comorbid, value = pred$visits_if_seek,
             panel = "Visits per woman in care"),
  data.frame(n_comorbid = pred$n_comorbid, value = pred$expected_visits,
             panel = "Expected visits per woman")
)
long$panel <- factor(long$panel, levels = c("P(any pelvic-floor visit)",
                                            "Visits per woman in care",
                                            "Expected visits per woman"))

p2 <- ggplot(long, aes(n_comorbid, value)) +
  geom_line(colour = SERIES, linewidth = 0.9) +
  geom_point(data = subset(long, n_comorbid %in% c(0, 12)),
             colour = SERIES, size = 2.2) +
  geom_text(data = subset(long, n_comorbid %in% c(0, 12)),
            aes(label = ifelse(value < 1, sprintf("%.3f", value), sprintf("%.2f", value))),
            vjust = -1.1, hjust = c(0, 1)[1 + (subset(long, n_comorbid %in% c(0, 12))$n_comorbid > 0)],
            size = 3.1, colour = INK) +
  facet_wrap(~ panel, scales = "free_y", nrow = 1) +
  scale_x_continuous(breaks = seq(0, 12, 3), expand = expansion(mult = 0.08)) +
  scale_y_continuous(expand = expansion(mult = c(0.05, 0.18))) +
  labs(
    title = "Comorbidity burden, not insurance, is what moves pelvic-floor care seeking",
    subtitle = paste("Both parts of the model rise with comorbidity, so the product rises steeply.",
                     "\nHeld at the reference woman: age 50, private insurance, >=400% FPL, NH White."),
    x = "Distinct non-pelvic-floor conditions recorded", y = NULL,
    caption = paste0("MEPS 2023, survey-weighted two-part model. Panels carry different units ",
                     "and are drawn on their own scales;\nplotting them against a shared axis ",
                     "would misstate all three.")
  ) +
  theme_urps()

ggsave(file.path(FIGS, "meps_care_seeking_comorbidity.png"), p2,
       width = 9.4, height = 4.3, dpi = 200, bg = SURFACE)

# ---- Provenance manifest ----------------------------------------------------
# Every number quoted in README.md must be reproducible from a generated
# artifact. This is that artifact.
man <- c(
  "MEPS 2023 care-seeking manifest",
  paste("Generated by:", "scripts/plot_meps_care_seeking.R"),
  "",
  "Source files (MEPS R package, AHRQ public use):",
  "  HC-248G office-based events (OB); HC-243 full-year (FYC);",
  "  HC-241 conditions (COND); HC-241IF condition-event link (CLNK)",
  paste("Pelvic-floor ICD-10 prefixes:", paste(MEPS_PELVIC_FLOOR_ICD10, collapse = ", ")),
  "",
  "Survey design: id = ~VARPSU, strata = ~VARSTR, weights = ~PERWT23F, nest = TRUE",
  "",
  sprintf("Analytic sample (adult women): %d", model$n_persons),
  sprintf("Care-seeking events:           %d", model$n_events),
  sprintf("Weighted care-seeking rate:    %.3f%% of adult women per year",
          100 * model$weighted_care_seeking),
  "",
  "Estimated care-seeking multipliers (reference = 1.0):",
  sprintf("  %-16s %-12s %6.3f  [%.3f, %.3f]  identified=%s",
          mult$panel, mult$level, mult$multiplier, mult$conf_low, mult$conf_high,
          mult$identified),
  "",
  "Comorbidity gradient at the reference woman:",
  sprintf("  %2d conditions: p_seek %.4f  visits_if_seek %.3f  expected %.4f",
          pred$n_comorbid, pred$p_seek, pred$visits_if_seek, pred$expected_visits),
  "",
  "Figures: figures/meps_care_seeking_multipliers.png,",
  "         figures/meps_care_seeking_comorbidity.png"
)
writeLines(man, file.path(CACHE, "meps_2023_care_seeking_manifest.txt"))

message("wrote figures/meps_care_seeking_multipliers.png, .../meps_care_seeking_comorbidity.png",
        " and ", file.path(CACHE, "meps_2023_care_seeking_manifest.txt"))
