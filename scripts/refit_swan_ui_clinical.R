#!/usr/bin/env Rscript
# ===========================================================================
# PHASE 1 (UI) -- memory-safe clinical refit + identification check.
#
# Fixes:
#  * MEMORY: swan_all_visits.rds deserializes to 16,142 x 9,159. Load ONCE, subset
#    immediately to (SWANID + verified severity items + covariates), drop the wide
#    object, gc(full), THEN build small per-visit frames. Never hold 8 full-width
#    frames. Checkpoints (message + object.size) around every stage; an OS kill is
#    not tryCatch-catchable, so a silent death is located by the last checkpoint.
#  * DEFINITION: clinical UI state = Sandvik moderate-or-worse (needs BOTH frequency
#    and amount). Where amount is missing (visit 5) or stress-specific only
#    (12/13/15), the state is MISSING (NA) for the transition analysis -- never
#    silently FALSE. leakage_ever is the gate, NOT the disease state.
#
# Returns the identification diagnostics: eligible clinical-UI visit pairs, counts of
# clinical 0->1 (onset) and 1->0 (remission) transitions, and -- only if identifiable
# -- the fitted onset/remission with external Wu validation (>=80 included).
# Writes nothing "calibrated".
# ===========================================================================
suppressPackageStartupMessages(pkgload::load_all(".", quiet = TRUE))
ck <- function(tag, obj = NULL) message(sprintf("[ck] %-34s %s", tag,
  if (is.null(obj)) "" else sprintf("dim=%s size=%.1f MB",
    paste(dim(obj), collapse="x"), as.numeric(utils::object.size(obj))/1e6)))

# verified severity crosswalk: which visits have a usable amount item (Sandvik-able)
vm <- readr::read_csv(system.file("extdata","swan","swan_incontinence_variable_map.csv",
                                  package="urpssim"), show_col_types = FALSE)
vm_ok <- vm[vm$verification_status == "verified" & !is.na(vm$amount_variable) &
            vm$amount_scope == "overall", ]         # overall amount only (exclude stress-specific)
SEV_VISITS <- sort(as.integer(vm_ok$visit))
ck(sprintf("verified overall-amount visits: %s", paste(SEV_VISITS, collapse=",")))

ck("before readRDS")
sw <- load_swan_archive(verbose = FALSE)
ck("after readRDS (wide archive)", sw)
prov <- attr(sw, "swan_archive_provenance")

# union of required columns: SWANID + severity items for verified visits + covariates
sev_cols <- unique(na.omit(unlist(vm_ok[, c("ever_variable","frequency_variable",
                                            "amount_variable","urge_amount_variable")])))
cov_cols <- unique(unlist(lapply(SEV_VISITS, function(v)
  unlist(swan_covariate_columns(v)))))
# baseline covariates build_swan_dmdm_panel needs but swan_covariate_columns omits:
# parity (NUMCHILD, asked once) + baseline comorbidity items.
base_cov <- c("NUMCHILD", get("SWAN_COMORBIDITY_ITEMS", asNamespace("urpssim")),
              "CANCER2", "DIABETE2")
keep <- intersect(unique(c("SWANID", sev_cols, cov_cols, base_cov)), names(sw))
slim <- sw[, keep, drop = FALSE]
attr(slim, "swan_archive_provenance") <- prov
rm(sw); invisible(base::gc(full = TRUE))
ck("after subset + rm(wide) + gc", slim)

# per-visit frames from the SMALL object (references, not copies)
frames <- setNames(lapply(SEV_VISITS, function(v) slim), as.character(SEV_VISITS))
ck(sprintf("built %d per-visit frames (refs)", length(frames)))

ip <- build_swan_incontinence_panel(frames, verbose = FALSE)
ck("after build_swan_incontinence_panel", ip)
sev <- score_sandvik_severity(ip, surgical_threshold = "moderate")
# clinical UI state: moderate+ where severity computable; NA (missing) otherwise
sev$clinical_ui <- ifelse(is.na(sev$frequency_level) | is.na(sev$amount_level), NA_integer_,
                          as.integer(sev$leakage_ever & sev$surgical_threshold_met))
ck(sprintf("sandvik scored | clinical_ui: %d obs, %d moderate+, %d missing-severity",
           sum(!is.na(sev$clinical_ui)), sum(sev$clinical_ui, na.rm=TRUE), sum(is.na(sev$clinical_ui))))

# covariate panel; swap in clinical UI (NA where severity incomputable)
panel <- build_swan_dmdm_panel(slim, conditions = "ui", verbose = FALSE)
key <- match(paste(panel$person_id, panel$year), paste(sev$swan_id, sev$visit))
panel$has_ui <- sev$clinical_ui[key]
panel <- panel[!is.na(panel$has_ui), , drop = FALSE]
ck("clinical covariate panel (has_ui non-NA)", panel)

# identification: count clinical 0->1 and 1->0 transitions across consecutive pairs
td <- dmdm_transition_data(panel, conditions = "ui")
tu <- td[td$condition == "ui", ]
n_onset <- sum(tu$from == 0L & tu$event == 1L); n_at_risk0 <- sum(tu$from == 0L)
n_rem   <- sum(tu$from == 1L & tu$event == 1L); n_at_risk1 <- sum(tu$from == 1L)
# TRULY consecutive pairs (diff == 1) with clinical state at BOTH visits, per person
py <- panel[order(panel$person_id, panel$year), c("person_id","year")]
pairs <- sum(unlist(tapply(py$year, py$person_id, function(y) if (length(y) < 2) 0L else sum(diff(sort(y)) == 1L))))
sev_gate_visits <- sort(unique(panel$year))
cat("\n== IDENTIFICATION (clinical Sandvik moderate+ UI) ==\n")
cat(sprintf("consistent-gate (INVOLEA) visits with computable clinical state: %s\n",
            paste(sev_gate_visits, collapse = ",")))
cat(sprintf("truly-consecutive (diff==1) clinical visit-pairs: %d\n", pairs))
cat(sprintf("0->1 clinical onset transitions:   %d  (of %d at-risk person-visits)\n", n_onset, n_at_risk0))
cat(sprintf("1->0 clinical remission transitions: %d  (of %d at-risk person-visits)\n", n_rem, n_at_risk1))

MIN_TRANS <- 30L   # prespecified minimum to attempt a longitudinal fit
if (n_onset < MIN_TRANS || n_rem < MIN_TRANS) {
  cat(sprintf("\nIDENTIFICATION FAIL: <%d transitions in a cell -- clinical UI is NOT longitudinally\n", MIN_TRANS))
  cat("identifiable from SWAN's verified overall-amount visits. This is a scientific result\n")
  cat("(sparse severity measurement), not a computational failure. Do NOT fit; escalate the\n")
  cat("UI-source decision (LEKINVO-with-caveat vs cross-sectional-onset+literature-remission).\n")
  cat("\nPOP: NOT fitted from SWAN (PROLAPS is binary self-report, not POP-Q). CONFIRMED.\n")
  quit(status = 0)
}

fit <- fit_dmdm_transitions(td, conditions = "ui")
cat(sprintf("\nfitted UI (clinical): remission=%.3f\n", fit$remission["ui"]))
print(round(fit$onset$ui, 3))
WU <- c("60-69"=0.247,"70-79"=0.297,"80+"=0.382); BANDS <- list("60-69"=60:69,"70-79"=70:79,"80+"=80:89); AGES<-18:89
onp <- get(".dmdm_onset_p", asNamespace("urpssim")); REP <- list(vag=2,ysl=0,bmi=27,hyst=0,com=0)
sp <- function(a,rem){meno<-as.integer(AGES>=51); o<-onp(a,AGES,REP$vag,REP$ysl,REP$bmi,REP$hyst,meno,REP$com)
  p<-numeric(length(AGES)); for(i in 2:length(AGES)) p[i]<-p[i-1]*(1-rem)+(1-p[i-1])*o[i-1]; setNames(p,AGES)}
pred <- vapply(BANDS, function(g) mean(sp(fit$onset$ui, fit$remission["ui"])[as.character(g)]), numeric(1))
cat("\n== UI external validation vs Wu (predicted vs observed) ==\n")
cat(sprintf("   observed  %.3f %.3f %.3f\n", WU[1],WU[2],WU[3]))
cat(sprintf("   predicted %.3f %.3f %.3f\n", pred[1],pred[2],pred[3]))
cat(sprintf("   rel err   %.0f%% %.0f%% %.0f%%\n", 100*(pred-WU)/WU))
cat("\nPOP: NOT fitted from SWAN (PROLAPS binary self-report). CONFIRMED.\n")
