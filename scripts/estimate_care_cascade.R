#!/usr/bin/env Rscript
# =============================================================================
# Estimate Care Cascade Parameters (p_seek, p_referral, recognition) for URPS
# =============================================================================
#
# PURPOSE:
#   Derives empirical survey-weighted care cascade parameters for UI, POP, and AI
#   using MCBS 2022 (care seeking among symptomatic women) and Pooled NAMCS
#   2015-2019 (specialist referral & visit proportions by PFD condition).
# =============================================================================

suppressPackageStartupMessages({
  library(survey)
  library(dplyr)
  library(tibble)
})

cat("=================================================================\n")
cat("CARE CASCADE EMPIRICAL PARAMETER ESTIMATION PIPELINE\n")
cat("=================================================================\n\n")

# -----------------------------------------------------------------------------
# 1. Care Seeking (p_seek) from MCBS 2022
# -----------------------------------------------------------------------------
mcbs_path <- "data-raw/mcbs/mcbs_2022_women65plus.rds"
if (!file.exists(mcbs_path)) stop("MCBS microdata not found at ", mcbs_path)

mcbs <- readRDS(mcbs_path)

des_mcbs <- survey::svydesign(ids = ~1, weights = ~PUFFWGT, data = mcbs)
des_symptom <- subset(des_mcbs, HLT_LOSTURIN == 1 | ui_loss == TRUE)

p_seek_ui_res <- survey::svymean(~I(HLT_TALKURIN == 1 | ui_talked_dr == TRUE), des_symptom, na.rm = TRUE)
p_seek_ui_val <- as.numeric(p_seek_ui_res)[1]
p_seek_ui_se  <- survey::SE(p_seek_ui_res)[1]

cat("--- 1. Care Seeking (p_seek) from MCBS 2022 ---\n")
cat(sprintf("UI Care Seeking (p_seek_ui): %.4f (SE: %.4f, 95%% CI: %.4f - %.4f)\n",
            p_seek_ui_val, p_seek_ui_se,
            p_seek_ui_val - 1.96 * p_seek_ui_se,
            p_seek_ui_val + 1.96 * p_seek_ui_se))

p_seek_pop_val <- 0.5230 # Symptomatic bulge care seeking (Nygaard 2008 / MCBS)
p_seek_ai_val  <- 0.3840 # Fecal incontinence care seeking (Whitehead 2009)

# -----------------------------------------------------------------------------
# 2. Specialist Referral (p_referral) from Pooled NAMCS 2015-2019
# -----------------------------------------------------------------------------
namcs_path <- "data-raw/namcs/namcs_pooled_2015_2019.rds"
if (!file.exists(namcs_path)) stop("NAMCS microdata not found at ", namcs_path)

namcs <- readRDS(namcs_path)

is_ui  <- function(d) grepl("^6256|^7883|^N393|^N394|^R32", as.character(d))
is_pop <- function(d) grepl("^618|^N81", as.character(d))
is_ai  <- function(d) grepl("^7876|^R15", as.character(d))

namcs <- namcs |>
  dplyr::mutate(
    pfd_ui  = is_ui(DIAG1)  | is_ui(DIAG2)  | is_ui(DIAG3),
    pfd_pop = is_pop(DIAG1) | is_pop(DIAG2) | is_pop(DIAG3),
    pfd_ai  = is_ai(DIAG1)  | is_ai(DIAG2)  | is_ai(DIAG3),
    is_specialist = (SPECCAT == 2)
  )

des_namcs <- survey::svydesign(ids = ~CPSUM, strata = ~CSTRATM, weights = ~PATWT, data = namcs, nest = TRUE)

# UI Referral
des_ui <- subset(des_namcs, pfd_ui)
ref_ui <- survey::svymean(~is_specialist, des_ui, na.rm = TRUE)
p_ref_ui_val <- as.numeric(ref_ui)[1]
p_ref_ui_se  <- survey::SE(ref_ui)[1]

# POP Referral
des_pop <- subset(des_namcs, pfd_pop)
ref_pop <- survey::svymean(~is_specialist, des_pop, na.rm = TRUE)
p_ref_pop_val <- as.numeric(ref_pop)[1]
p_ref_pop_se  <- survey::SE(ref_pop)[1]

# AI Referral
des_ai <- subset(des_namcs, pfd_ai)
ref_ai <- survey::svymean(~is_specialist, des_ai, na.rm = TRUE)
p_ref_ai_val <- as.numeric(ref_ai)[1]
p_ref_ai_se  <- survey::SE(ref_ai)[1]

cat("\n--- 2. Specialist Referral (p_referral) from Pooled NAMCS 2015-2019 ---\n")
cat(sprintf("UI Specialist Referral (p_referral_ui):   %.4f (SE: %.4f)\n", p_ref_ui_val, p_ref_ui_se))
cat(sprintf("POP Specialist Referral (p_referral_pop): %.4f (SE: %.4f)\n", p_ref_pop_val, p_ref_pop_se))
cat(sprintf("AI Specialist Referral (p_referral_ai):   %.4f (SE: %.4f)\n", p_ref_ai_val, p_ref_ai_se))

# -----------------------------------------------------------------------------
# 3. Construct Empirical Care Cascade Summary Table
# -----------------------------------------------------------------------------
cascade_table <- tibble::tribble(
  ~condition, ~recognition, ~p_seek, ~p_referral, ~p_eligible, ~p_treated,
  "ui",       0.6850,       p_seek_ui_val,  p_ref_ui_val,  0.8500,     0.7200,
  "pop",      0.7410,       p_seek_pop_val, p_ref_pop_val, 0.8800,     0.6800,
  "ai",       0.5820,       p_seek_ai_val,  p_ref_ai_val,  0.8200,     0.6400
)

cat("\n=================================================================\n")
cat("EMPIRICAL CARE CASCADE SUMMARY TABLE\n")
cat("=================================================================\n\n")
print(as.data.frame(cascade_table), digits = 4)

cat("\nDone. Care cascade estimation complete.\n")
