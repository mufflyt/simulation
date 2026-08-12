# Individual-level entry panel ------------------------------------------------
#
# Replaces the aggregate deconvolution in `supply-fellowship_conversion.R` for
# the purpose of locating ENTRY INTO INDEPENDENT PRACTICE. That fit returned
# R2 = 0.006 against first-billing counts; the failure is not fixable by tuning,
# because annual national totals cannot identify a per-person transition. This
# file works one clinician at a time instead.
#
# FIRST MEDICARE BILLING IS NOT ENTRY. A clinician can begin independent
# practice and bill no Medicare at all -- a young panel, a commercial or
# Medicaid payer mix, an employed position billing under a group. Entry is
# therefore a MULTI-SOURCE OBSERVATION STATE, and every source keeps its own
# date so a reader can see which one spoke and when.
#
# UNKNOWN IS NOT INACTIVE. Every indicator is three-state: TRUE (the source
# observed activity), FALSE (the source could have observed activity and did
# not), NA (the source cannot speak to that provider-year at all). Collapsing
# NA to FALSE would convert a gap in the data into a claim of non-practice,
# which is the single most consequential error available here.

#' Evidence sources for entry into independent practice, in precedence order
#'
#' `grade` is what separates a source that can ESTABLISH independent practice
#' from one that can only corroborate it:
#'
#' * `practice` -- positive evidence of independent practice. PECOS enrollment
#'   is first because a clinician must enrol to bill Medicare independently, and
#'   enrolment precedes the first claim. An NPPES taxonomy transition out of
#'   student (390200000X) is a self-reported end of training. Part B billing is
#'   direct but late and payer-limited.
#' * `corroborating` -- consistent with practice but incapable of establishing
#'   it. Open Payments is deliberately here: industry meals and education
#'   payments are made to FELLOWS, so a payment does not distinguish a trainee
#'   from an attending and must never define entry on its own.
#' * `bounding` -- certification post-dates entry, so it bounds entry from above
#'   and never establishes its year.
#'
#' @format Data frame of sources with `rank`, `grade` and observability window.
#' @family entry panel
#' @concept supply
#' @export
ENTRY_EVIDENCE_SOURCES <- data.frame(
  source     = c("pecos_enrollment", "nppes_taxonomy_exit_student",
                 "part_b_billing", "open_payments", "certification"),
  rank       = 1:5,
  grade      = c("practice", "practice", "practice",
                 "corroborating", "bounding"),
  first_year = c(2016L, 2013L, 2013L, 2015L, NA_integer_),
  last_year  = c(2025L, 2024L, 2024L, 2023L, NA_integer_),
  observed_per_year = c(FALSE, TRUE, TRUE, TRUE, FALSE),
  stringsAsFactors = FALSE
)

#' Default location of the 84 GB credentials/claims database
#' @family entry panel
#' @concept supply
#' @export
ENTRY_PANEL_DB_DEFAULT <- "/Volumes/MufflySamsung 1 1/DuckDB/nber_my_duckdb.duckdb"

.ep_stop <- function(...) stop("entry panel: ", ..., call. = FALSE)

.ep_npis <- function(x) {
  if (is.data.frame(x)) {
    if (!"npi" %in% names(x)) {
      .ep_stop(sprintf("`cohort` is a data frame without an `npi` column; it has %s.",
                       paste(names(x), collapse = ", ")))
    }
    x$npi <- as.character(x$npi)
    return(x)
  }
  data.frame(npi = as.character(x), stringsAsFactors = FALSE)
}

.ep_connect <- function(db) {
  if (!file.exists(db)) {
    .ep_stop(sprintf("database not found at '%s'. Attach the external volume, or pass `db =` explicitly.", db))
  }
  tryCatch(DBI::dbConnect(duckdb::duckdb(), dbdir = db, read_only = TRUE),
           error = function(e)
             .ep_stop(sprintf("could not open '%s' read-only: %s", db, conditionMessage(e))))
}

#' Build a longitudinal provider-year entry panel for a pilot cohort
#'
#' One row per NPI per year, carrying each source's own three-state indicator
#' plus the provider-level entry determination repeated across that provider's
#' rows.
#'
#' @param cohort Character vector of NPIs, or a data frame with an `npi` column
#'   and optionally `fellowship_completion_year` and `certification_year`.
#'   Fellowship completion is NOT derivable from any local source and must be
#'   supplied; without it `years_from_fellowship_to_entry` is NA and the panel
#'   still builds.
#' @param years Panel years.
#' @param db Path to the credentials/claims DuckDB.
#' @param roster_path Optional roster CSV used to fill `certification_year` when
#'   the cohort does not supply it.
#' @return Data frame, one row per NPI-year, of class `entry_panel`.
#' @family entry panel
#' @concept supply
#' @export
build_entry_panel <- function(cohort,
                              years = 2013:2024,
                              db = ENTRY_PANEL_DB_DEFAULT,
                              roster_path = NULL) {
  co <- .ep_npis(cohort)
  if (!nrow(co)) .ep_stop("`cohort` is empty.")
  bad <- co$npi[!grepl("^[0-9]{10}$", co$npi)]
  if (length(bad)) {
    .ep_stop(sprintf("%d NPI(s) are not 10 digits, e.g. '%s'. NPIs must be 10-digit strings; a numeric column loses leading zeros.",
                     length(bad), bad[1]))
  }
  if (anyDuplicated(co$npi)) {
    d <- co$npi[duplicated(co$npi)]
    .ep_stop(sprintf("`cohort` has %d duplicate NPI(s), e.g. '%s'.", length(unique(d)), d[1]))
  }
  years <- sort(unique(as.integer(years)))

  con <- .ep_connect(db)
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  duckdb::duckdb_register(con, "ep_cohort", co[, "npi", drop = FALSE])

  # ---- source 1: PECOS enrolment (interval-censored) ------------------------
  pecos <- DBI::dbGetQuery(con, "
    SELECT c.npi, p.first_enrollment_year, p.last_enrollment_year
    FROM ep_cohort c LEFT JOIN credentials.ppef_enrollment_status p ON p.npi = c.npi")

  # ---- source 2: NPPES taxonomy, per snapshot year --------------------------
  ny <- intersect(years, 2013:2024)
  nppes <- if (length(ny)) {
    q <- paste(sprintf(
      "SELECT %d AS year, t.npi, t.taxonomy_1, t.practice_address_state AS state
       FROM credentials.temporal_nppes_%d_fixed t
       JOIN ep_cohort c ON c.npi = t.npi", ny, ny), collapse = " UNION ALL ")
    DBI::dbGetQuery(con, q)
  } else data.frame(year = integer(), npi = character(),
                    taxonomy_1 = character(), state = character())

  # ---- source 3: Medicare Part B, per year ---------------------------------
  py <- intersect(years, 2013:2024)
  partb <- if (length(py)) {
    q <- paste(sprintf(
      "SELECT %d AS year, CAST(s.Rndrng_NPI AS VARCHAR) AS npi,
              SUM(CASE WHEN s.HCPCS_Drug_Ind = 'Y' THEN 0 ELSE s.Tot_Srvcs END) AS services,
              MAX(s.Rndrng_Prvdr_State_Abrvtn) AS state,
              MAX(s.Rndrng_Prvdr_Type) AS prvdr_type
       FROM main.medicare_part_b_by_service_%d s
       JOIN ep_cohort c ON c.npi = CAST(s.Rndrng_NPI AS VARCHAR)
       GROUP BY 1, 2", py, py), collapse = " UNION ALL ")
    DBI::dbGetQuery(con, q)
  } else data.frame(year = integer(), npi = character(), services = numeric(),
                    state = character(), prvdr_type = character())

  # ---- source 4: Open Payments (interval-censored rollup) ------------------
  op <- DBI::dbGetQuery(con, "
    SELECT c.npi, o.first_payment_year, o.last_payment_year
    FROM ep_cohort c LEFT JOIN credentials.open_payments_activity o ON o.npi = c.npi")

  # ---- certification -------------------------------------------------------
  cert <- if ("certification_year" %in% names(co)) {
    stats::setNames(as.integer(co$certification_year), co$npi)
  } else {
    rp <- roster_path %||% system.file("extdata", "provider_year",
                                       "provider_year_activity_long.csv",
                                       package = "simulation")
    if (nzchar(rp) && file.exists(rp)) {
      d <- utils::read.csv(rp, stringsAsFactors = FALSE)
      d <- d[!duplicated(d$npi), c("npi", "cert_year")]
      stats::setNames(as.integer(d$cert_year), as.character(d$npi))[co$npi]
    } else stats::setNames(rep(NA_integer_, nrow(co)), co$npi)
  }
  fell <- if ("fellowship_completion_year" %in% names(co)) {
    stats::setNames(as.integer(co$fellowship_completion_year), co$npi)
  } else stats::setNames(rep(NA_integer_, nrow(co)), co$npi)

  # ---- assemble, three-state throughout ------------------------------------
  grid <- expand.grid(npi = co$npi, year = years,
                      stringsAsFactors = FALSE, KEEP.OUT.ATTRS = FALSE)
  win <- function(src, yr) {
    s <- ENTRY_EVIDENCE_SOURCES[ENTRY_EVIDENCE_SOURCES$source == src, ]
    yr >= s$first_year & yr <= s$last_year
  }
  # A source that has NO row at all for an NPI cannot speak to that NPI in any
  # year: NA, not FALSE. `in_universe` carries that distinction per source.
  pec_u <- co$npi %in% pecos$npi[!is.na(pecos$first_enrollment_year)]
  op_u  <- co$npi %in% op$npi[!is.na(op$first_payment_year)]
  npp_u <- co$npi %in% nppes$npi
  names(pec_u) <- names(op_u) <- names(npp_u) <- co$npi

  pec_first <- stats::setNames(pecos$first_enrollment_year[match(co$npi, pecos$npi)], co$npi)
  pec_last  <- stats::setNames(pecos$last_enrollment_year[match(co$npi, pecos$npi)], co$npi)
  op_first  <- stats::setNames(op$first_payment_year[match(co$npi, op$npi)], co$npi)
  op_last   <- stats::setNames(op$last_payment_year[match(co$npi, op$npi)], co$npi)

  key <- paste(grid$npi, grid$year)
  pb_i <- match(key, paste(partb$npi, partb$year))
  np_i <- match(key, paste(nppes$npi, nppes$year))

  three <- function(observable, positive) {
    out <- rep(NA, length(observable))
    out[observable] <- positive[observable]
    out
  }

  pb_obs <- win("part_b_billing", grid$year)
  panel <- data.frame(
    npi = grid$npi, year = grid$year,
    fellowship_completion_year = unname(fell[grid$npi]),
    certification_year = unname(cert[grid$npi]),

    ev_pecos_enrolled = three(win("pecos_enrollment", grid$year) & pec_u[grid$npi],
                              grid$year >= pec_first[grid$npi] &
                                grid$year <= pec_last[grid$npi]),
    ev_nppes_observed = three(win("nppes_taxonomy_exit_student", grid$year) & npp_u[grid$npi],
                              !is.na(np_i)),
    ev_nppes_taxonomy = nppes$taxonomy_1[np_i],
    ev_nppes_student = ifelse(is.na(np_i), NA, nppes$taxonomy_1[np_i] == "390200000X"),
    ev_nppes_state = nppes$state[np_i],

    ev_partb_billed = three(pb_obs, !is.na(pb_i)),
    ev_partb_services = ifelse(is.na(pb_i), ifelse(pb_obs, 0, NA), partb$services[pb_i]),
    ev_partb_state = partb$state[pb_i],
    ev_partb_type = partb$prvdr_type[pb_i],

    ev_openpay_paid = three(win("open_payments", grid$year) & op_u[grid$npi],
                            grid$year >= op_first[grid$npi] &
                              grid$year <= op_last[grid$npi]),
    ev_certified = ifelse(is.na(cert[grid$npi]), NA, grid$year >= cert[grid$npi]),
    stringsAsFactors = FALSE
  )
  panel <- panel[order(panel$npi, panel$year), ]
  structure(cbind(panel, .ep_derive(panel)), class = c("entry_panel", "data.frame"))
}

# Provider-level entry determination, broadcast back across that provider's rows.
.ep_derive <- function(panel) {
  practice <- c("ev_pecos_enrolled", "ev_partb_billed")   # + taxonomy exit, below
  first_true <- function(d, col) {
    y <- d$year[which(d[[col]] %in% TRUE)]
    if (length(y)) min(y) else NA_integer_
  }
  parts <- lapply(split(panel, panel$npi), function(d) {
    d <- d[order(d$year), ]

    # NPPES taxonomy exit: the first year a specialty taxonomy replaces the
    # student taxonomy. Requires BOTH states to have been seen, or there is no
    # transition to date -- a provider only ever seen with a specialty taxonomy
    # has no observable exit and must not be given one.
    tax_exit <- NA_integer_
    seen_student <- any(d$ev_nppes_student %in% TRUE)
    if (seen_student) {
      spec <- d$year[which(d$ev_nppes_student %in% FALSE)]
      st <- max(d$year[which(d$ev_nppes_student %in% TRUE)])
      if (length(spec) && any(spec > st)) tax_exit <- min(spec[spec > st])
    }

    # Practice evidence RESTRICTED to years at or after fellowship completion.
    # OB/GYN residents and generalists bill Medicare years before subspecialty
    # fellowship: in the pilot dry run, clinicians certified 2019-2021 had first
    # Part B years of 2014-2015. Unrestricted "first billing" therefore measures
    # entry to GENERALIST practice for this population, not entry to independent
    # urogynecology. When the fellowship year is known, the restricted quantity
    # is the defensible one; the unrestricted dates are retained beside it.
    fy0 <- d$fellowship_completion_year[1]
    post <- if (is.na(fy0)) d else d[d$year >= fy0, , drop = FALSE]

    f_pecos <- first_true(d, "ev_pecos_enrolled")
    f_partb <- first_true(d, "ev_partb_billed")
    f_op    <- first_true(d, "ev_openpay_paid")
    f_cert  <- d$certification_year[1]

    prac <- c(pecos_enrollment = f_pecos,
              nppes_taxonomy_exit_student = tax_exit,
              part_b_billing = f_partb)
    prac <- prac[!is.na(prac)]

    if (length(prac)) {
      entry <- min(prac)
      src <- names(prac)[which.min(prac)]
      agree <- sum(abs(prac - entry) <= 1)
      conf <- if (length(prac) >= 2 && agree >= 2) "high" else "moderate"
    } else if (!is.na(f_op) || !is.na(f_cert)) {
      # Corroborating/bounding only. Open Payments cannot establish entry (it
      # pays fellows) and certification post-dates it, so this is an upper
      # bound flagged as such, never a measurement.
      entry <- suppressWarnings(min(c(f_op, f_cert), na.rm = TRUE))
      src <- if (!is.na(f_op) && (is.na(f_cert) || f_op <= f_cert)) "open_payments" else "certification"
      conf <- "low"
    } else {
      entry <- NA_integer_; src <- NA_character_; conf <- "unknown"
    }

    # Conflict: practice-grade sources more than two years apart, or practice
    # evidence PREDATING fellowship completion (pre-fellowship generalist
    # practice, supervised billing, or a wrong graduation year -- all worth a
    # human look rather than silent averaging).
    conflict <- FALSE
    if (length(prac) >= 2 && (max(prac) - min(prac)) > 2) conflict <- TRUE
    fy <- d$fellowship_completion_year[1]
    if (!is.na(fy) && !is.na(entry) && entry < fy) conflict <- TRUE

    n_avail <- rowSums(!is.na(d[, c("ev_pecos_enrolled", "ev_nppes_observed",
                                    "ev_partb_billed", "ev_openpay_paid")]))
    any_true <- function(cols) {
      m <- d[, cols, drop = FALSE]
      ifelse(rowSums(m == TRUE, na.rm = TRUE) > 0, TRUE,
             ifelse(rowSums(!is.na(m)) == 0, NA, FALSE))
    }

    prac_post <- c(pecos_enrollment = first_true(post, "ev_pecos_enrolled"),
                   nppes_taxonomy_exit_student = if (!is.na(tax_exit) && (is.na(fy0) || tax_exit >= fy0)) tax_exit else NA_integer_,
                   part_b_billing = first_true(post, "ev_partb_billed"))
    prac_post <- prac_post[!is.na(prac_post)]
    entry_post <- if (length(prac_post)) min(prac_post) else NA_integer_

    data.frame(
      active_practice_observed = any_true(c("ev_pecos_enrolled", "ev_partb_billed")),
      entry_year_best = entry,
      entry_year_post_fellowship = entry_post,
      entry_source_post_fellowship = if (length(prac_post)) names(prac_post)[which.min(prac_post)] else NA_character_,
      years_from_fellowship_to_entry = if (is.na(fy) || is.na(entry_post)) NA_integer_ else entry_post - fy,
      entry_source = src, entry_confidence = conf,
      evidence_available = n_avail, evidence_conflict = conflict,
      first_pecos_year = f_pecos, first_nppes_taxonomy_exit_year = tax_exit,
      first_partb_year = f_partb, first_openpay_year = f_op,
      stringsAsFactors = FALSE
    )
  })
  do.call(rbind, parts[order(names(parts))])[
    order(order(paste(panel$npi, panel$year))), , drop = FALSE]
}

#' Collapse an entry panel to one row per clinician
#' @param panel Result of [build_entry_panel()].
#' @return One row per NPI with the entry determination and source dates.
#' @family entry panel
#' @concept supply
#' @export
summarise_entry_panel <- function(panel) {
  if (!inherits(panel, "entry_panel")) {
    .ep_stop("`panel` must come from build_entry_panel(); got class ",
             paste(class(panel), collapse = "/"), ".")
  }
  keep <- c("npi", "fellowship_completion_year", "certification_year",
            "first_pecos_year", "first_nppes_taxonomy_exit_year",
            "first_partb_year", "first_openpay_year", "entry_year_best",
            "entry_year_post_fellowship", "entry_source_post_fellowship",
            "years_from_fellowship_to_entry", "entry_source",
            "entry_confidence", "evidence_conflict")
  out <- panel[!duplicated(panel$npi), keep]
  out$years_observed_active <- vapply(split(panel, panel$npi), function(d)
    sum(d$active_practice_observed %in% TRUE), integer(1))[out$npi]
  rownames(out) <- NULL
  out
}
