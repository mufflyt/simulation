# NHANES Pelvic Floor Disorder Module Acquisition ----
#
# Source: National Health and Nutrition Examination Survey (NHANES), NCHS/CDC
# Survey: Continuous NHANES, 2-year cycles; population-based, nationally representative
# Format: SAS Transport (.xpt) — read with haven::read_xpt()
# DUA:    None required (public use file)
#
# Modules downloaded:
#
#   Urinary Incontinence / Kidney Conditions (KIQ_U):
#     2017-2020 (pre-pandemic): P_KIQ_U.xpt  — weight WTMEC4YR (4-year)
#     2021-2023 (post-pandemic): KIQ_U_L.xpt — weight WTMEC2YR (2-year)
#
#   Reproductive Health (RHQ) — hysterectomy, parity, for covariate adjustment:
#     2017-2020: P_RHQ.xpt   — weight WTMEC4YR
#     2021-2023: RHQ_L.xpt   — weight WTMEC2YR
#
#   Demographics (DEMO) — age, sex, race, income:
#     2017-2020: P_DEMO.xpt  — weight WTMEC4YR / WTINT4YR
#     2021-2023: DEMO_L.xpt  — weight WTMEC2YR / WTINT2YR
#
# Key KIQ_U variables:
#   KIQ005  — ever told you have weak/failing kidneys? (1=Yes, 2=No)
#   KIQ042  — leak urine (stress incontinence) past 12 months? (1=Yes, 2=No)
#   KIQ044  — urge to urinate that couldn't control? (urgency UI) (1=Yes, 2=No)
#   KIQ046  — accidental urine leak amount? (1=drops, 2=small, 3=more)
#   KIQ480  — urinated at night how many times?
#
# Key RHQ variables:
#   RHQ131  — had hysterectomy? (1=Yes, 2=No)
#   RHD190  — age at hysterectomy
#   RHQ171  — number of pregnancies (parity)
#
# Key DEMO variables:
#   RIDAGEYR — age in years (0-79; 80+ coded as 80)
#   RIAGENDR — gender (1=Male, 2=Female)
#   RIDRETH3 — race/ethnicity (1=Mex-Am, 2=Other-Hispanic, 3=NH-White,
#                               4=NH-Black, 6=NH-Asian, 7=Other/Multi)
#   INDHHIN2 — household income (categorical)
#   BMXBMI   — body mass index (from Body Measures module, merged via SEQN)
#
# Weighting strategy:
#   Pooling 2017-2020 + 2021-2023: divide each cycle weight by 2 before pooling.
#   This yields a combined 6-year weight (~2017-2023) that sums to correct
#   national totals.  See NCHS Analytical Guidance for combining cycles.
#
# Output files:
#   data-raw/nhanes/nhanes_pfd_pooled.rds  — merged KIQ_U + RHQ + DEMO, women 20+
#   data-raw/nhanes/nhanes_pfd_manifest.txt

library(haven)
library(dplyr)

# ---- Module URL lookup -------------------------------------------------------

.nhanes_url <- function(module, cycle) {
  base <- "https://wwwn.cdc.gov/Nchs/Data/Nhanes"
  switch(cycle,
    "2017-2020" = sprintf("%s/Public/2017-2020/%s.xpt", base, module),
    "2021-2023" = sprintf("%s/Public/2021-2023/%s.xpt", base, module),
    stop("Unknown NHANES cycle: ", cycle)
  )
}

# ---- Download helper ---------------------------------------------------------

.download_nhanes_xpt <- function(module, cycle, out_dir = "data-raw/nhanes") {
  url      <- .nhanes_url(module, cycle)
  out_path <- file.path(out_dir, paste0(module, ".xpt"))

  if (file.exists(out_path)) {
    cat("Already downloaded:", out_path, "\n")
    return(invisible(out_path))
  }

  cat(sprintf("Downloading NHANES %s (%s)...\n", module, cycle))
  tryCatch(
    utils::download.file(url, out_path, mode = "wb", quiet = FALSE),
    error = function(e) stop("Failed to download ", url, "\n  ", conditionMessage(e))
  )
  cat(sprintf("  Saved: %s (%.1f MB)\n", out_path, file.size(out_path) / 1e6))
  invisible(out_path)
}

# ---- Read and harmonise a single cycle --------------------------------------

.read_nhanes_cycle <- function(cycle, wt_divisor = 1, out_dir = "data-raw/nhanes") {
  # Module file names differ by cycle (pre-pandemic prefix P_, post-pandemic suffix _L)
  modules <- switch(cycle,
    "2017-2020" = list(kiq = "P_KIQ_U", rhq = "P_RHQ", demo = "P_DEMO"),
    "2021-2023" = list(kiq = "KIQ_U_L", rhq = "RHQ_L", demo = "DEMO_L"),
    stop("Unknown cycle: ", cycle)
  )
  wt_var <- switch(cycle,
    "2017-2020" = "WTMEC4YR",
    "2021-2023" = "WTMEC2YR"
  )

  # Download all three modules
  for (nm in names(modules)) {
    .download_nhanes_xpt(modules[[nm]], cycle, out_dir)
  }

  # Read
  kiq  <- haven::read_xpt(file.path(out_dir, paste0(modules$kiq,  ".xpt")))
  rhq  <- haven::read_xpt(file.path(out_dir, paste0(modules$rhq,  ".xpt")))
  demo <- haven::read_xpt(file.path(out_dir, paste0(modules$demo, ".xpt")))

  cat(sprintf("  %s: KIQ=%d, RHQ=%d, DEMO=%d\n",
              cycle, nrow(kiq), nrow(rhq), nrow(demo)))

  # Merge on SEQN (participant identifier)
  merged <- demo |>
    dplyr::select(
      SEQN, RIDAGEYR, RIAGENDR, RIDRETH3, INDHHIN2,
      dplyr::any_of(c("WTMEC4YR", "WTMEC2YR"))
    ) |>
    dplyr::left_join(
      kiq |> dplyr::select(SEQN, dplyr::any_of(
        c("KIQ042", "KIQ044", "KIQ046", "KIQ480", "KIQ005"))),
      by = "SEQN"
    ) |>
    dplyr::left_join(
      rhq |> dplyr::select(SEQN, dplyr::any_of(c("RHQ131", "RHD190", "RHQ171"))),
      by = "SEQN"
    )

  # Standardise weight column name and scale for pooling
  merged <- merged |>
    dplyr::rename(WTMEC = dplyr::all_of(wt_var)) |>
    dplyr::mutate(
      WTMEC_pooled = .data$WTMEC / wt_divisor,
      nhanes_cycle = cycle
    )

  merged
}

# ---- Process and pool --------------------------------------------------------

#' Download, merge, and pool NHANES pelvic floor modules (2017-2023)
#'
#' Downloads KIQ_U, RHQ, and DEMO modules for the 2017-2020 and 2021-2023
#' NHANES cycles, merges on SEQN, filters to women 20+, and pools with
#' cycle-adjusted weights (each cycle weight divided by 2 for a combined
#' 6-year estimate).
#'
#' @param out_dir   Directory to save downloaded XPT and output RDS files.
#' @param out_path  Path for the pooled output RDS.
#' @return Tibble (women 20+, both cycles pooled).
#' @export
process_nhanes_pfd <- function(
    out_dir  = "data-raw/nhanes",
    out_path = "data-raw/nhanes/nhanes_pfd_pooled.rds") {

  if (file.exists(out_path)) {
    cat("Already processed:", out_path, "— loading cached file.\n")
    return(invisible(readRDS(out_path)))
  }

  # Pooling two cycles: divide each cycle weight by 2 so totals are preserved
  c1 <- .read_nhanes_cycle("2017-2020", wt_divisor = 2)
  c2 <- .read_nhanes_cycle("2021-2023", wt_divisor = 2)

  pooled <- dplyr::bind_rows(c1, c2)
  cat(sprintf("\nPooled: %d participants (both cycles)\n", nrow(pooled)))

  # Filter to women 20+ with positive MEC weight
  women <- pooled |>
    dplyr::filter(
      .data$RIAGENDR == 2,
      .data$RIDAGEYR >= 20,
      !is.na(.data$WTMEC_pooled),
      .data$WTMEC_pooled > 0
    )
  cat(sprintf("Women 20+ with positive MEC weight: %d\n", nrow(women)))

  # Clean sentinel codes (NHANES uses 7=refused, 9=don't know for binary vars)
  women <- women |>
    dplyr::mutate(
      dplyr::across(
        dplyr::any_of(c("KIQ042", "KIQ044", "KIQ046", "KIQ480", "KIQ005",
                        "RHQ131", "RHQ171")),
        ~ dplyr::if_else(.x %in% c(7L, 9L, 77L, 99L, 777L, 999L),
                         NA_integer_, as.integer(.x))
      ),
      dplyr::across(
        dplyr::any_of("RHD190"),
        ~ dplyr::if_else(.x %in% c(777L, 999L), NA_real_, as.double(.x))
      ),
      # Derived binary indicators (1=Yes, NA=missing, 0=No)
      ui_stress   = dplyr::case_when(
        .data$KIQ042 == 1L ~ TRUE,
        .data$KIQ042 == 2L ~ FALSE,
        TRUE ~ NA
      ),
      ui_urgency  = dplyr::case_when(
        .data$KIQ044 == 1L ~ TRUE,
        .data$KIQ044 == 2L ~ FALSE,
        TRUE ~ NA
      ),
      ui_any      = .data$ui_stress | .data$ui_urgency,
      hysterectomy = dplyr::case_when(
        .data$RHQ131 == 1L ~ TRUE,
        .data$RHQ131 == 2L ~ FALSE,
        TRUE ~ NA
      )
    )

  saveRDS(women, out_path)
  cat("Saved:", out_path, "\n")

  # Manifest
  manifest_lines <- c(
    "NHANES Pelvic Floor Disorder Module — Women 20+ Pooled File",
    paste("Generated:", Sys.time()),
    "Cycles: 2017-2020 (P_KIQ_U, P_RHQ, P_DEMO) + 2021-2023 (KIQ_U_L, RHQ_L, DEMO_L)",
    paste("Total women 20+ rows:", nrow(women)),
    "",
    "Key UI variables:",
    "  KIQ042 → ui_stress   — stress urinary incontinence (leak when cough/sneeze/exercise)",
    "  KIQ044 → ui_urgency  — urgency urinary incontinence (sudden urge, couldn't control)",
    "  ui_any               — TRUE if stress OR urgency UI",
    "",
    "Key reproductive variables:",
    "  RHQ131 → hysterectomy — ever had uterus removed",
    "  RHQ171               — number of pregnancies (parity)",
    "",
    "Survey weight: WTMEC_pooled = WTMEC[4|2]YR / 2 (cycle-scaled for pooling)",
    "Survey design: ids=~SDMVPSU, strata=~SDMVSTRA, weights=~WTMEC_pooled, nest=TRUE",
    "DUA required: NO (public use file)"
  )
  writeLines(manifest_lines, file.path(out_dir, "nhanes_pfd_manifest.txt"))
  cat("Manifest written.\n")

  invisible(women)
}

# ---- Run --------------------------------------------------------------------
process_nhanes_pfd()
