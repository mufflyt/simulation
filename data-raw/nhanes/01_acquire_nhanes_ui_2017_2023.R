# NHANES UI Person-Level Microdata Acquisition (2017-2023) ----
#
# Source: NCHS / CDC NHANES Continuous Survey
# Cycles: 2017-2020 (P_KIQ_U, P_RHQ, P_DEMO, P_BMX) + 2021-2023 (KIQ_U_L, RHQ_L, DEMO_L, BMX_L)
# Target: Women 20+ with survey-weighted UI, hysterectomy, parity, BMI, and age
#
# Codebook Variable Verification:
#   RHD280   — Had uterus removed / hysterectomy (Yes/No)  [RHQ131 is ever pregnant!]
#   RHD190   — Age at hysterectomy
#   RHD167   — Number of vaginal deliveries
#   RHQ171   — Number of live birth deliveries
#   RHQ131   — Ever pregnant (Yes/No)
#   RHQ031   — Had regular periods in past 12 months (Yes/No)
#   RHQ060   — Age at last menstrual period
#   KIQ042   — Leak urine with physical activity (Stress UI)
#   KIQ044   — Urge to urinate that couldn't control (Urgency UI)
#   KIQ010   — Amount of urine leakage (Drops, Small, More)
#   KIQ046   — Frequency of leakage during nonphysical activities
#   BMXBMI   — Body Mass Index (kg/m^2)
#   RIDAGEYR — Age in single years
#   RIAGENDR — Gender
#   RIDRETH3 — Race/ethnicity

suppressPackageStartupMessages({
  library(nhanesA)
  library(dplyr)
})

out_dir  <- "data-raw/nhanes"
out_path <- file.path(out_dir, "nhanes_ui_person_2017_2023.rds")
manifest <- file.path(out_dir, "nhanes_ui_person_manifest.txt")

dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)

message("Downloading NHANES UI modules (2017-2020)...")
kiq_p  <- nhanesA::nhanes("P_KIQ_U")
rhq_p  <- nhanesA::nhanes("P_RHQ")
demo_p <- nhanesA::nhanes("P_DEMO")
bmx_p  <- nhanesA::nhanes("P_BMX")

names(kiq_p)  <- toupper(names(kiq_p))
names(rhq_p)  <- toupper(names(rhq_p))
names(demo_p) <- toupper(names(demo_p))
names(bmx_p)  <- toupper(names(bmx_p))

wt_p <- intersect(c("WTMECPRP", "WTMEC4YR", "WTMEC2YR"), names(demo_p))[1]

p1720 <- demo_p |>
  dplyr::select(SEQN, RIDAGEYR, RIAGENDR, RIDRETH3, dplyr::any_of(c("INDFMPIR", "INDHHIN2")), WTMEC = dplyr::all_of(wt_p)) |>
  dplyr::left_join(kiq_p |> dplyr::select(SEQN, dplyr::any_of(c("KIQ042", "KIQ044", "KIQ010", "KIQ046", "KIQ005"))), by = "SEQN") |>
  dplyr::left_join(rhq_p |> dplyr::select(SEQN, dplyr::any_of(c("RHD280", "RHD190", "RHQ131", "RHD167", "RHQ171", "RHQ031", "RHQ060"))), by = "SEQN") |>
  dplyr::left_join(bmx_p |> dplyr::select(SEQN, dplyr::any_of("BMXBMI")), by = "SEQN") |>
  dplyr::mutate(WTMEC_pooled = WTMEC / 2, cycle = "2017-2020")

message("Downloading NHANES UI modules (2021-2023)...")
kiq_l  <- nhanesA::nhanes("KIQ_U_L")
rhq_l  <- nhanesA::nhanes("RHQ_L")
demo_l <- nhanesA::nhanes("DEMO_L")
bmx_l  <- nhanesA::nhanes("BMX_L")

names(kiq_l)  <- toupper(names(kiq_l))
names(rhq_l)  <- toupper(names(rhq_l))
names(demo_l) <- toupper(names(demo_l))
names(bmx_l)  <- toupper(names(bmx_l))

wt_l <- intersect(c("WTMEC2YR", "WTMEC4YR"), names(demo_l))[1]

p2123 <- demo_l |>
  dplyr::select(SEQN, RIDAGEYR, RIAGENDR, RIDRETH3, dplyr::any_of(c("INDFMPIR", "INDHHIN2")), WTMEC = dplyr::all_of(wt_l)) |>
  dplyr::left_join(kiq_l |> dplyr::select(SEQN, dplyr::any_of(c("KIQ042", "KIQ044", "KIQ010", "KIQ046", "KIQ005"))), by = "SEQN") |>
  dplyr::left_join(rhq_l |> dplyr::select(SEQN, dplyr::any_of(c("RHD280", "RHD190", "RHQ131", "RHD167", "RHQ171", "RHQ031", "RHQ060"))), by = "SEQN") |>
  dplyr::left_join(bmx_l |> dplyr::select(SEQN, dplyr::any_of("BMXBMI")), by = "SEQN") |>
  dplyr::mutate(WTMEC_pooled = WTMEC / 2, cycle = "2021-2023")

pooled <- dplyr::bind_rows(p1720, p2123)

is_yes <- function(x) {
  cx <- as.character(x)
  dplyr::case_when(
    cx %in% c("Yes", "1") ~ TRUE,
    cx %in% c("No", "2")  ~ FALSE,
    TRUE ~ NA
  )
}

num_val <- function(x) {
  if (is.numeric(x)) return(x)
  suppressWarnings(as.numeric(as.character(x)))
}

ui_women <- pooled |>
  dplyr::filter(
    as.character(RIAGENDR) %in% c("Female", "2"),
    RIDAGEYR >= 20,
    !is.na(WTMEC_pooled),
    WTMEC_pooled > 0
  ) |>
  dplyr::mutate(
    ui_stress          = is_yes(KIQ042),
    ui_urgency         = is_yes(KIQ044),
    ui_any             = ui_stress | ui_urgency,
    hysterectomy       = is_yes(RHD280),
    ever_pregnant      = is_yes(RHQ131),
    postmenopausal     = is_yes(RHQ031) == FALSE,
    vaginal_deliveries = num_val(RHD167),
    live_births        = num_val(RHQ171),
    age_at_hysterectomy= num_val(RHD190),
    age_at_menopause   = num_val(RHQ060),
    bmi                = num_val(BMXBMI),
    age                = num_val(RIDAGEYR)
  )

saveRDS(ui_women, out_path)
message("Saved UI person-level extract: ", out_path, " (", nrow(ui_women), " rows x ", ncol(ui_women), " cols)")

# Sanity checks
h_prev <- weighted.mean(ui_women$hysterectomy, ui_women$WTMEC_pooled, na.rm = TRUE)
ui_prev <- weighted.mean(ui_women$ui_any, ui_women$WTMEC_pooled, na.rm = TRUE)

message(sprintf("SANITY CHECK — Hysterectomy prevalence (RHD280): %.1f%%", h_prev * 100))
message(sprintf("SANITY CHECK — UI Any prevalence (KIQ042/044): %.1f%%", ui_prev * 100))

writeLines(c(
  "NHANES UI Person-Level Microdata (2017-2023)",
  paste("Generated:", Sys.time()),
  paste("File:", out_path),
  paste("Rows (women 20+):", nrow(ui_women)),
  paste("Columns:", ncol(ui_women)),
  paste("Weighted Hysterectomy Prevalence (RHD280):", sprintf("%.1f%%", h_prev * 100)),
  paste("Weighted UI Any Prevalence (KIQ042/044):", sprintf("%.1f%%", ui_prev * 100)),
  "",
  "Verified Codebook Variables:",
  "  RHD280   -> hysterectomy (Yes/No)",
  "  RHD190   -> age_at_hysterectomy",
  "  RHD167   -> vaginal_deliveries",
  "  RHQ171   -> live_births",
  "  RHQ131   -> ever_pregnant",
  "  RHQ031/60-> postmenopausal / age_at_menopause",
  "  KIQ042   -> ui_stress",
  "  KIQ044   -> ui_urgency",
  "  KIQ010   -> amount of leakage",
  "  KIQ046   -> nonphysical leak frequency",
  "  BMXBMI   -> bmi"
), manifest)

message("Manifest written: ", manifest)
