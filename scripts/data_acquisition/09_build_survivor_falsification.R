#!/usr/bin/env Rscript
# Survivor-conditioning falsification inputs ----
#
#   URPS_CLIFF_ABOG=... URPS_CLIFF_ABU=... URPS_DUCKDB=... \
#     Rscript scripts/data_acquisition/09_build_survivor_falsification.R
#
# WHY THIS EXISTS. The 2013-2023 `board_certified_active` series is a 2025
# roster back-projected by certification year (docs/DENOMINATOR_AUDIT.md). A
# physician the 2025 adjudication drops is therefore absent from EVERY earlier
# year of that series -- including years in which they were demonstrably
# delivering and billing care. That is survivor conditioning, and it is
# falsifiable without a model: link the dropped physicians to a federal activity
# source and count how many were observed.
#
# WHAT THIS SCRIPT PRODUCES. A small aggregate artifact holding every count the
# validation module reports, plus the provenance of the sources it was built
# from. The module reads that artifact. It does NOT hold the counts itself, and
# neither does this script: every number below is computed from the data, and
# the gates at the bottom fail if the arithmetic does not close.
#
# EVIDENCE TIERS, which must not be collapsed into one binary "active" flag:
#
#   Tier 1  Medicare Part B billing        care actually delivered and billed
#   Tier 2  Medicare clinician directory   enrolment / practice listing only,
#                                          NOT evidence of patient-care volume
#   Tier 3  no federal observation         in the sources examined
#
# The clinician directory covers 2018 onward ONLY, so it is silent on 2016-2017
# and cannot rescue the early half of the validation window. That is why it is
# reported as a separate, weaker tier rather than unioned into Part B.
#
# THE SOURCES ARE NOT IN THIS REPOSITORY, and must not be. Identity comes from
# the cliff enriched ABOG/ABU rosters; activity from a local Medicare Part B
# provider-year panel and clinician directory in DuckDB. Both carry physician
# identifiers. All three paths are REQUIRED environment variables and this
# script stops if any is missing -- there is deliberately no fallback to a
# prepared CSV, because a fallback is how a hand-edited table becomes canonical.
#
# Output: inst/extdata/survivor_falsification.json  (aggregate counts only, no
# NPIs, no names, no per-physician rows)

OUT <- "inst/extdata/survivor_falsification.json"

FRAME               <- 2013:2023   # Part B provider-year panel coverage
VALIDATION          <- 2016:2021   # the back-test validation window
DIRECTORY_FRAME     <- 2018:2025   # clinician directory coverage
SUSTAINED_MIN_YEARS <- 3L          # "sustained" directory listing

PARTB_TABLE     <- "medicare_part_b_unified"
DIRECTORY_TABLE <- "mips_providers"

# ---- inputs: explicit, required, never silently substituted -----------------

require_path <- function(var, what) {
  p <- Sys.getenv(var, unset = "")
  if (!nzchar(p)) {
    stop("Environment variable ", var, " is not set. It must give the path to ",
         what, ". This script has no fallback: the sources carry physician ",
         "identifiers, live outside this repository, and a silent fallback to ",
         "a prepared file is how an unverified table becomes canonical.",
         call. = FALSE)
  }
  if (!file.exists(p)) {
    stop(var, " points at a path that does not exist: ", p, call. = FALSE)
  }
  normalizePath(p)
}

ABOG   <- require_path("URPS_CLIFF_ABOG", "the enriched ABOG URPS roster CSV")
ABU    <- require_path("URPS_CLIFF_ABU", "the enriched ABU URPS roster CSV")

# The rosters come from a separate repository. Record WHICH COMMIT of it, so the
# artifact is reproducible by anyone with access rather than only by whoever
# happens to hold this checkout. Absent when the CSVs are not inside a git
# checkout, which is recorded rather than guessed at.
git_provenance <- function(path) {
  d <- dirname(path)
  run <- function(...) {
    out <- suppressWarnings(system2("git", c("-C", d, ...), stdout = TRUE,
                                    stderr = FALSE))
    if (length(out) && nzchar(out[1])) out[1] else NA_character_
  }
  if (is.na(run("rev-parse", "--is-inside-work-tree"))) {
    return(list(repository = NA_character_, commit = NA_character_,
                committed_at = NA_character_,
                note = "the roster CSVs are not inside a git checkout"))
  }
  list(repository = run("remote", "get-url", "origin"),
       commit = run("rev-parse", "HEAD"),
       committed_at = run("log", "-1", "--format=%cI"),
       dirty = length(suppressWarnings(system2(
         "git", c("-C", d, "status", "--porcelain", "--", basename(path)),
         stdout = TRUE, stderr = FALSE))) > 0L)
}
ROSTER_GIT <- git_provenance(ABOG)
DUCKDB <- require_path("URPS_DUCKDB", "the DuckDB database holding the Medicare
  Part B provider-year panel and the Medicare clinician directory")

message("ABOG roster : ", ABOG)
message("ABU roster  : ", ABU)
message("DuckDB      : ", DUCKDB)

# ---- 1. the identity universe ------------------------------------------------
#
# Both boards certify into the same subspecialty, so the universe is the union.
# `in_model_baseline` is the later active-roster adjudication: a FLAG on a
# retained row, not a filter that removed rows, which is what makes this test
# possible at all.

read_roster <- function(path, pathway) {
  d <- utils::read.csv(path, stringsAsFactors = FALSE, check.names = FALSE)
  need <- c("npi", "cert_year", "in_model_baseline")
  miss <- setdiff(need, names(d))
  if (length(miss)) {
    stop(basename(path), " is missing required column(s): ",
         paste(miss, collapse = ", "), call. = FALSE)
  }
  data.frame(
    npi       = trimws(as.character(d$npi)),
    pathway   = pathway,
    cert_year = suppressWarnings(as.integer(d$cert_year)),
    retained  = toupper(trimws(as.character(d$in_model_baseline))) %in%
      c("TRUE", "T", "1", "YES"),
    stringsAsFactors = FALSE
  )
}

ids <- rbind(read_roster(ABOG, "abog"), read_roster(ABU, "abu"))
ids$pid <- sprintf("URPS%04d", seq_len(nrow(ids)))

n_universe <- nrow(ids)
n_retained <- sum(ids$retained)
n_excluded <- sum(!ids$retained)

# A usable NPI is exactly ten digits. Blank and malformed are distinguished
# because they mean different things: blank is an identity the roster never
# carried, malformed would be an identity we mangled.
npi_blank     <- !nzchar(ids$npi) | is.na(ids$npi)
npi_malformed <- !npi_blank & !grepl("^[0-9]{10}$", ids$npi)
ids$npi_usable <- !npi_blank & !npi_malformed

excl <- ids[!ids$retained, ]
n_excluded_blank_npi     <- sum(!nzchar(excl$npi) | is.na(excl$npi))
n_excluded_malformed_npi <- sum(!(!nzchar(excl$npi) | is.na(excl$npi)) &
                                  !grepl("^[0-9]{10}$", excl$npi))
n_excluded_no_npi        <- sum(!excl$npi_usable)
n_linkable               <- sum(excl$npi_usable)

# A duplicated NPI inside the linkage set would double-count a physician in
# every count downstream, so it is a hard stop rather than a note.
dup_npi <- sum(duplicated(ids$npi[ids$npi_usable]))
if (dup_npi > 0L) {
  stop("The identity universe contains ", dup_npi, " duplicated usable NPI(s). ",
       "Every downstream count would double-count those physicians.",
       call. = FALSE)
}

message(sprintf("identity universe %d = retained %d + excluded %d",
                n_universe, n_retained, n_excluded))
message(sprintf("excluded: %d NPI-linkable + %d without usable NPI",
                n_linkable, n_excluded_no_npi))

# ---- 2. federal activity observation -----------------------------------------

con <- DBI::dbConnect(duckdb::duckdb(), DUCKDB, read_only = TRUE)
on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

have <- DBI::dbListTables(con)
miss <- setdiff(c(PARTB_TABLE, DIRECTORY_TABLE), have)
if (length(miss)) {
  stop("The DuckDB database is missing required table(s): ",
       paste(miss, collapse = ", "), ". Present tables include: ",
       paste(utils::head(sort(have), 20), collapse = ", "), call. = FALSE)
}

DBI::dbWriteTable(con, "urps_ids", ids[ids$npi_usable, c("npi", "pid")],
                  temporary = TRUE, overwrite = TRUE)

# Part B presence is a ROW in the provider-year panel. It is deliberately NOT
# defined on the service-count column: `total_services` is VARCHAR and carries
# the literal string 'NA' where CMS suppresses small cells, so treating a
# missing count as absence understated observation by roughly a third of all
# provider-years in an earlier draft of this analysis.
partb <- DBI::dbGetQuery(con, sprintf(
  "SELECT i.pid AS pid, m.data_year AS year
     FROM urps_ids i JOIN %s m ON m.npi_char = i.npi
    WHERE m.data_year BETWEEN %d AND %d
    GROUP BY 1, 2", PARTB_TABLE, min(FRAME), max(FRAME)))

directory <- DBI::dbGetQuery(con, sprintf(
  "SELECT i.pid AS pid, p.data_year AS year
     FROM urps_ids i JOIN %s p ON p.npi = i.npi
    GROUP BY 1, 2", DIRECTORY_TABLE))

# Provenance for sources too large to hash. A fingerprint of what was actually
# queried is worth more than a checksum of an 80 GB file we cannot recompute.
fingerprint_table <- function(tbl, year_col) {
  q <- DBI::dbGetQuery(con, sprintf(
    "SELECT COUNT(*) AS n_rows, MIN(%s) AS min_year, MAX(%s) AS max_year,
            COUNT(DISTINCT %s) AS n_years FROM %s", year_col, year_col,
    year_col, tbl))
  list(table = tbl, n_rows = as.numeric(q$n_rows), min_year = as.integer(q$min_year),
       max_year = as.integer(q$max_year), n_years = as.integer(q$n_years))
}
fp_partb     <- fingerprint_table(PARTB_TABLE, "data_year")
fp_directory <- fingerprint_table(DIRECTORY_TABLE, "data_year")

# ---- 3. observation matrices -------------------------------------------------

obs_matrix <- function(long, years) {
  m <- matrix(FALSE, nrow(ids), length(years),
              dimnames = list(ids$pid, as.character(years)))
  keep <- long$year %in% years
  if (any(keep)) {
    m[cbind(long$pid[keep], as.character(long$year[keep]))] <- TRUE
  }
  m
}

M_frame <- obs_matrix(partb, FRAME)
M_val   <- M_frame[, as.character(VALIDATION), drop = FALSE]
M_dir   <- obs_matrix(directory, DIRECTORY_FRAME)

linkable_pid <- ids$pid[!ids$retained & ids$npi_usable]

any_frame_pid  <- linkable_pid[rowSums(M_frame[linkable_pid, , drop = FALSE]) > 0L]
any_val_pid    <- linkable_pid[rowSums(M_val[linkable_pid, , drop = FALSE]) > 0L]
persistent_pid <- linkable_pid[rowSums(M_val[linkable_pid, , drop = FALSE]) ==
                                 length(VALIDATION)]
none_frame_pid <- setdiff(linkable_pid, any_frame_pid)

n_any_frame  <- length(any_frame_pid)
n_any_val    <- length(any_val_pid)
n_persistent <- length(persistent_pid)
n_none_frame <- length(none_frame_pid)

# ---- 4. the residual group, classified by the WEAKER source ------------------
#
# These physicians have no billed care anywhere in the Part B frame. The
# clinician directory can still place them in a practice, which is enrolment,
# not care delivered -- so they are classified separately and never added to the
# Part B counts.

dir_years <- rowSums(M_dir[none_frame_pid, , drop = FALSE])
n_dir_sustained <- sum(dir_years >= SUSTAINED_MIN_YEARS)
n_dir_isolated  <- sum(dir_years >= 1L & dir_years < SUSTAINED_MIN_YEARS)
n_dir_neither   <- sum(dir_years == 0L)

# ---- 5. annual panel for the figure -----------------------------------------
#
# One denominator: physicians already certified by the year in question and
# observed billing Part B that year, split into retained and later-excluded.
# The split is a literal partition of the same bar, which is the only honest way
# to show that the excluded group is a subset rather than a separate series.

eligible <- outer(ids$cert_year, FRAME,
                  function(cy, y) !is.na(cy) & cy <= y)
dimnames(eligible) <- dimnames(M_frame)
linked <- ids$npi_usable

annual <- do.call(rbind, lapply(seq_along(FRAME), function(j) {
  seen <- M_frame[, j] & eligible[, j] & linked
  data.frame(
    year              = FRAME[j],
    retained_observed = sum(seen & ids$retained),
    excluded_observed = sum(seen & !ids$retained),
    total_observed    = sum(seen),
    persistent_observed = sum(seen & ids$pid %in% persistent_pid),
    eligible_total    = sum(eligible[, j] & linked),
    stringsAsFactors  = FALSE
  )
}))

# ---- 6. gates: the arithmetic must close before anything is written ----------
#
# These are relationships, not expected values. They hold for any correct
# extraction and fail for the mistakes this analysis is actually prone to:
# mixing denominators, unioning tiers, and losing rows in a join.

gate <- function(ok, msg) if (!isTRUE(ok)) stop("GATE FAILED: ", msg, call. = FALSE)

gate(n_universe == n_retained + n_excluded,
     "the identity universe does not split into retained + excluded")
gate(n_excluded == n_linkable + n_excluded_no_npi,
     "the excluded denominator does not close into linkable + unlinkable")
gate(n_excluded_no_npi == n_excluded_blank_npi + n_excluded_malformed_npi,
     "unusable NPIs do not split into blank + malformed")
gate(n_any_frame + n_none_frame == n_linkable,
     "Part B present + absent does not equal the linkage denominator")
gate(all(persistent_pid %in% any_val_pid),
     "a persistent biller is not in the validation-window observed set")
gate(all(any_val_pid %in% any_frame_pid),
     "a validation-window biller is not in the full-frame observed set")
gate(n_persistent <= n_any_val && n_any_val <= n_any_frame &&
       n_any_frame <= n_linkable,
     "the window counts do not nest")
gate(n_dir_sustained + n_dir_isolated + n_dir_neither == n_none_frame,
     "the no-Part-B residual does not partition into the directory classes")
gate(length(intersect(none_frame_pid, any_frame_pid)) == 0L,
     "a physician is counted as both observed and unobserved in Part B")
gate(all(annual$retained_observed + annual$excluded_observed ==
           annual$total_observed),
     "the annual panel does not partition into retained + excluded")
gate(all(annual$total_observed <= annual$eligible_total),
     "more physicians observed than were certified and linkable that year")
gate(min(fp_partb$min_year) <= min(FRAME) && fp_partb$max_year >= max(FRAME),
     "the Part B panel does not cover the analysis frame")
gate(fp_directory$min_year >= min(DIRECTORY_FRAME),
     "the clinician directory starts earlier than its documented coverage")

# ---- 7. write the artifact ---------------------------------------------------

artifact <- list(
  schema_version = 1L,
  generated_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%S%z"),
  generated_by = "scripts/data_acquisition/09_build_survivor_falsification.R",
  windows = list(
    frame = range(FRAME),
    validation = range(VALIDATION),
    directory = range(DIRECTORY_FRAME),
    sustained_min_years = SUSTAINED_MIN_YEARS
  ),
  denominators = list(
    identity_universe = n_universe,
    retained = n_retained,
    excluded_total = n_excluded,
    excluded_without_npi = n_excluded_no_npi,
    excluded_blank_npi = n_excluded_blank_npi,
    excluded_malformed_npi = n_excluded_malformed_npi,
    linkage_denominator = n_linkable
  ),
  partb = list(
    any_frame = n_any_frame,
    any_validation = n_any_val,
    persistent_validation = n_persistent,
    none_frame = n_none_frame,
    provider_years_persistent = n_persistent * length(VALIDATION)
  ),
  directory = list(
    sustained = n_dir_sustained,
    isolated = n_dir_isolated,
    neither = n_dir_neither
  ),
  annual = annual,
  exclusion_reason = paste(
    "in_model_baseline == FALSE (later active-roster adjudication);",
    "the roster records no finer per-provider reason"),
  provenance = list(
    abog = list(file = basename(ABOG), sha256 = digest::digest(ABOG, algo = "sha256", file = TRUE),
                bytes = as.numeric(file.size(ABOG))),
    abu = list(file = basename(ABU), sha256 = digest::digest(ABU, algo = "sha256", file = TRUE),
               bytes = as.numeric(file.size(ABU))),
    roster_repository = ROSTER_GIT,
    duckdb = list(file = basename(DUCKDB), bytes = as.numeric(file.size(DUCKDB)),
                  mtime = format(file.mtime(DUCKDB), "%Y-%m-%dT%H:%M:%S%z"),
                  note = paste("too large to checksum; the table fingerprints",
                               "below identify what was queried"),
                  partb = fp_partb, directory = fp_directory)
  )
)

dir.create(dirname(OUT), recursive = TRUE, showWarnings = FALSE)
jsonlite::write_json(artifact, OUT, auto_unbox = TRUE, pretty = TRUE, digits = NA)

message("\nwrote ", OUT)
message(sprintf(
  paste0("  identity universe %d | excluded %d | linkable %d\n",
         "  Part B any %d-%d %d | any %d-%d %d | ALL SIX %d (%d provider-years)\n",
         "  no Part B %d -> directory sustained %d, isolated %d, neither %d"),
  n_universe, n_excluded, n_linkable,
  min(FRAME), max(FRAME), n_any_frame,
  min(VALIDATION), max(VALIDATION), n_any_val,
  n_persistent, n_persistent * length(VALIDATION),
  n_none_frame, n_dir_sustained, n_dir_isolated, n_dir_neither))
