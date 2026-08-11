# Empirical urogynecology service taxonomy for Medicare Part B (carrier/PUF) data.
#
# Scope claim: this maps HCPCS codes that Medicare Part B CAN identify. Codes are
# assigned to the FIRST matching category, so the order below is the precedence
# order. Anything unmatched falls to "other_services" rather than being forced
# into a urogynecologic bucket.
#
# Explicit non-identifiable services -- see UROGYN_NOT_CAPTURED below. These are
# NOT inferred and NOT redistributed into other categories.

UROGYN_SERVICE_CATEGORIES <- list(
  urodynamics = c("51725","51726","51727","51728","51729","51736","51741","51772",
                  "51784","51785","51792","51795","51797"),
  bladder_botox = c("52287"),
  cystoscopy = c("52000","52001","52005","52007","52204","52214","52224","52234",
                 "52235","52240","52250","52260","52265","52270","52275","52281",
                 "52282","52283","52285","52310","52315","52317","52318","52332"),
  sling_incontinence_surgery = c("51992","51990","51715","53440","53442","53444",
                                 "53445","53446","53447","53448","53449","57287",
                                 "57288","57289"),
  prolapse_surgery = c("45560","57106","57110","57120","57240","57250","57260",
                       "57265","57267","57268","57270","57280","57282","57283",
                       "57284","57285","57423","57425","57426","57556","58400"),
  pessary_care = c("57160","A4561","A4562"),
  other_urogyn_procedure = c("51700","51701","51702","51703","51705","51710","51798",
                             "51845","51840","51841","64561","64566","64581",
                             "64585","64590","64595","53500","53502","53505",
                             "53510","53515","53520","57200","57210","57220",
                             "57230","57295","57296","57426"),
  laboratory_and_diagnostics = c(sprintf("%05d", 81000:81099), "36415","82570",
                 "87086","87088","87150","87481","87491","87591","87661","87798",
                 "87800","87801","85025","85027","80048","80053","76770","76775",
                 "76856","76857","51798"),
  office_em = c(sprintf("%05d", 99201:99215), sprintf("%05d", 99241:99245),
                sprintf("%05d", 99341:99350), sprintf("%05d", 99381:99404),
                sprintf("%05d", 99406:99429), "99354","99355","99417","99358","99359")
)

# Services urogynecologists deliver that Part B does NOT reliably identify.
# Stated so that a zero in the table above is never read as a zero in practice.
# HCPCS_Drug_Ind == "Y" lines report Tot_Srvcs in DRUG UNITS, not procedures.
# J0585 (onabotulinumtoxinA) is billed per unit, ~100-200 units per bladder
# injection, so summing it with procedure codes adds unlike quantities: in 2023
# drug lines were 58% of raw Tot_Srvcs from 0.3% of lines. Every service count
# used for D1/D2 and for the case-mix table therefore EXCLUDES drug lines, which
# are reported separately in provider_year_drug_units.csv.
UROGYN_DRUG_LINES_EXCLUDED <- TRUE

UROGYN_NOT_CAPTURED <- c(
  "Drug units (HCPCS_Drug_Ind == 'Y') are not procedures and are excluded from
   all service counts; they are reported separately and must not be summed with
   procedure volume.",
  "In-office laboratory (urinalysis, culture, PCR) is reported under the rendering
   physician NPI but reflects practice infrastructure and staff work, not physician
   clinical time; it is kept in its own category and never counted as procedures.",
  "Pessary supplies A4561/A4562 ARE present in this file and are counted in
   pessary_care. What remains uncaptured is ongoing pessary MAINTENANCE, which is
   billed as an office E&M visit with no pessary-specific code, so pessary_care is
   a floor on pessary workload, not a measure of it.",
  "Post-operative visits inside a 90-day global period -- bundled, so surgical
   follow-up volume is invisible.",
  "All care to non-Medicare patients (commercial, Medicaid, self-pay, VA/DoD).
   This is age-selective: a practice weighted toward younger patients with pelvic
   pain or childbirth injury appears far less active than a prolapse practice.",
  "Services billed under a facility or group NPI rather than the rendering
   individual NPI.",
  "Physician work inside a global surgical package other than the index procedure.",
  "CMS suppresses provider-HCPCS cells with fewer than 11 beneficiaries, so
   low-volume services are systematically missing, not zero."
)

classify_hcpcs <- function(hcpcs) {
  out <- rep("other_services", length(hcpcs))
  assigned <- rep(FALSE, length(hcpcs))
  for (cat in names(UROGYN_SERVICE_CATEGORIES)) {
    hit <- !assigned & hcpcs %in% UROGYN_SERVICE_CATEGORIES[[cat]]
    out[hit] <- cat; assigned <- assigned | hit
  }
  out
}
