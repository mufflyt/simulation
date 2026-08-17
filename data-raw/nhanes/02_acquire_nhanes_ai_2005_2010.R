# NHANES AI Person-Level Microdata Acquisition (2005-2010) ----
#
# Source: NCHS / CDC NHANES Continuous Survey
# Cycles: 2005-2006 (BHQ_D, RHQ_D, DEMO_D, BMX_D) + 2007-2008 (BHQ_E, RHQ_E, DEMO_E, BMX_E) + 2009-2010 (BHQ_F, RHQ_F, DEMO_F, BMX_F)
# Target: Women 20+ with survey-weighted Anal/Fecal Incontinence, hysterectomy, parity, BMI, and age
#
# Codebook Variable Verification:
#   BHQ010   — Gas leakage frequency
#   BHQ020   — Mucus leakage frequency
#   BHQ030   — Liquid stool leakage frequency
#   BHQ040   — Solid stool leakage frequency
#   BHQ060   — Bristol Stool Form Scale type (Type 1-7)
#   RHD280 / RHQ140 / RHQ141 — Had uterus removed / hysterectomy
#   RHD167 / RHQ165 — Number of vaginal deliveries
#   RHQ171   — Number of live birth deliveries
#   RHQ131   — Ever pregnant
#   BMXBMI   — Body Mass Index (kg/m^2)
#   RIDAGEYR — Age in single years
#   RIAGENDR — Gender

suppressPackageStartupMessages({
  library(nhanesA)
  library(dplyr)
})

out_dir  <- "data-raw/nhanes"
out_path <- file.path(out_dir, "nhanes_ai_person_2005_2010.rds")
manifest <- file.path(out_dir, "nhanes_ai_person_manifest.txt")

dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)

fetch_cycle <- function(bhq_mod, rhq_mod, demo_mod, bmx_mod, cycle_name) {
  message("Downloading NHANES AI modules (", cycle_name, ")...")
  bhq  <- nhanesA::nhanes(bhq_mod)
  rhq  <- nhanesA::nhanes(rhq_mod)
  demo <- nhanesA::nhanes(demo_mod)
  bmx  <- nhanesA::nhanes(bmx_mod)

  names(bhq)  <- toupper(names(bhq))
  names(rhq)  <- toupper(names(rhq))
  names(demo) <- toupper(names(demo))
  names(bmx)  <- toupper(names(bmx))

  wt_col <- intersect(c("WTMEC2YR", "WTMEC4YR"), names(demo))[1]

  demo |>
    dplyr::select(SEQN, RIDAGEYR, RIAGENDR, dplyr::any_of(c("RIDRETH1", "RIDRETH3")), WTMEC = dplyr::all_of(wt_col)) |>
    dplyr::left_join(bhq  |> dplyr::select(SEQN, dplyr::any_of(c("BHQ010", "BHQ020", "BHQ030", "BHQ040", "BHD050", "BHQ060"))), by = "SEQN") |>
    dplyr::left_join(rhq  |> dplyr::select(SEQN, dplyr::any_of(c("RHD280", "RHQ140", "RHQ141", "RHQ131", "RHD167", "RHQ165", "RHQ171", "RHQ031", "RHQ060"))), by = "SEQN") |>
    dplyr::left_join(bmx  |> dplyr::select(SEQN, dplyr::any_of("BMXBMI")), by = "SEQN") |>
    dplyr::mutate(WTMEC_pooled = WTMEC / 3, cycle = cycle_name)
}

c0506 <- fetch_cycle("BHQ_D", "RHQ_D", "DEMO_D", "BMX_D", "2005-2006")
c0708 <- fetch_cycle("BHQ_E", "RHQ_E", "DEMO_E", "BMX_E", "2007-2008")
c0910 <- fetch_cycle("BHQ_F", "RHQ_F", "DEMO_F", "BMX_F", "2009-2010")

pooled <- dplyr::bind_rows(c0506, c0708, c0910)

is_yes <- function(x) {
  cx <- as.character(x)
  dplyr::case_when(
    cx %in% c("Yes", "1") ~ TRUE,
    cx %in% c("No", "2")  ~ FALSE,
    TRUE ~ NA
  )
}

is_at_least_monthly <- function(x) {
  cx <- as.character(x)
  cx %in% c("1-3 times a month, or", "once a week,", "2 or more times a week,", "once a day,", "2 or more times a day,")
}

num_val <- function(x) {
  if (is.numeric(x)) return(x)
  suppressWarnings(as.numeric(as.character(x)))
}

ai_women <- pooled |>
  dplyr::filter(
    as.character(RIAGENDR) %in% c("Female", "2"),
    RIDAGEYR >= 20,
    !is.na(WTMEC_pooled),
    WTMEC_pooled > 0
  ) |>
  dplyr::mutate(
    # Fecal Incontinence definitions
    fi_wu  = is_at_least_monthly(BHQ020) | is_at_least_monthly(BHQ030) | is_at_least_monthly(BHQ040), # Mucus, liquid, or solid
    fi_nhs = is_at_least_monthly(BHQ030) | is_at_least_monthly(BHQ040), # Liquid or solid
    hysterectomy  = is_yes(RHD280),
    ever_pregnant = is_yes(RHQ131),
    live_births   = num_val(RHQ171),
    bmi = num_val(BMXBMI),
    age = num_val(RIDAGEYR)
  )

saveRDS(ai_women, out_path)
message("Saved AI person-level extract: ", out_path, " (", nrow(ai_women), " rows x ", ncol(ai_women), " cols)")

# Sanity checks
fi_wu_prev <- weighted.mean(ai_women$fi_wu, ai_women$WTMEC_pooled, na.rm = TRUE)
fi_nhs_prev <- weighted.mean(ai_women$fi_nhs, ai_women$WTMEC_pooled, na.rm = TRUE)
h_prev <- weighted.mean(ai_women$hysterectomy, ai_women$WTMEC_pooled, na.rm = TRUE)

message(sprintf("SANITY CHECK — FI (Wu / Mucus+Liquid+Solid) prevalence: %.1f%%", fi_wu_prev * 100))
message(sprintf("SANITY CHECK — FI (NIH / Liquid+Solid) prevalence: %.1f%%", fi_nhs_prev * 100))
message(sprintf("SANITY CHECK — Hysterectomy prevalence: %.1f%%", h_prev * 100))

writeLines(c(
  "NHANES AI Person-Level Microdata (2005-2010)",
  paste("Generated:", Sys.time()),
  paste("File:", out_path),
  paste("Rows (women 20+):", nrow(ai_women)),
  paste("Columns:", ncol(ai_women)),
  paste("Weighted FI Wu Prevalence (Mucus/Liquid/Solid):", sprintf("%.1f%%", fi_wu_prev * 100)),
  paste("Weighted FI NIH Prevalence (Liquid/Solid):", sprintf("%.1f%%", fi_nhs_prev * 100)),
  paste("Weighted Hysterectomy Prevalence:", sprintf("%.1f%%", h_prev * 100)),
  "",
  "Verified Codebook Variables:",
  "  BHQ010   -> gas leakage frequency",
  "  BHQ020   -> mucus leakage frequency",
  "  BHQ030   -> liquid stool leakage frequency",
  "  BHQ040   -> solid stool leakage frequency",
  "  BHQ060   -> Bristol Stool Form Scale type (1-7)",
  "  RHD280   -> hysterectomy (Yes/No)",
  "  RHD167   -> vaginal_deliveries",
  "  RHQ171   -> live_births",
  "  BMXBMI   -> bmi"
), manifest)

message("Manifest written: ", manifest)
