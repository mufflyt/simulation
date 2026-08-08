# Provenance stamp for validation outputs ----
#
# WHY. These numbers are now cited in a manuscript. Someone must be able to say
# "the reported figures came from commit X, using roster snapshot Y, seeds Z,
# under these package versions" and reproduce them after the model has moved on.
# A results document without this is a claim about a state of the world that no
# longer exists.

validation_provenance <- function(seeds = NULL, iterations = NULL,
                                  params = list(), extra = list()) {
  # THREE DIFFERENT COMMITS, and conflating them makes the record untrue.
  #   model_sha      -- the source state whose behaviour produced the numbers
  #   validation_sha -- the analysis code that measured it
  #   contract_sha   -- the commit of the DATA dependency (mufflyaccess artifact)
  # A validation commit that only records HEAD cannot support the sentence
  # "regenerated from source commit X using validation code at commit Y".
  sh_lines <- function(cmd) tryCatch(system(cmd, intern = TRUE, ignore.stderr = TRUE),
                                     error = function(e) character())
  sh <- function(cmd) { x <- sh_lines(cmd); if (length(x)) trimws(x[1]) else NA_character_ }

  contract <- tryCatch({
    s <- urps_baseline_supply(year = 2023L, include_urology = TRUE)
    list(version = s$contract_version, snapshot = s$snapshot_date,
         source_commit = s$source_git_commit, national = s$national)
  }, error = function(e) list())

  roster_path <- "data-raw/urps_roster/urps_roster_2026-07-22.csv"
  roster_id <- if (file.exists(roster_path)) {
    tryCatch(paste0(substr(digest::digest(file = roster_path, algo = "sha256"), 1, 16),
                    " (", basename(roster_path), ")"),
             error = function(e) basename(roster_path))
  } else NA_character_

  p <- list(
    run_timestamp     = format(Sys.time(), "%Y-%m-%d %H:%M:%S %Z"),
    head_sha          = sh("git rev-parse HEAD"),
    model_sha         = sh("git log -1 --format=%H -- R DESCRIPTION NAMESPACE"),
    validation_sha    = sh("git log -1 --format=%H -- scripts/validation"),
    tree_clean_model  = length(sh_lines("git status --porcelain -- R DESCRIPTION NAMESPACE")) == 0,
    tree_clean_valid  = length(sh_lines("git status --porcelain -- scripts/validation")) == 0,
    r_version         = paste(R.version$major, R.version$minor, sep = "."),
    urpssim_version   = as.character(tryCatch(utils::packageVersion("urpssim"),
                                              error = function(e) NA)),
    mufflyaccess      = as.character(tryCatch(utils::packageVersion("mufflyaccess"),
                                              error = function(e) NA)),
    contract_version  = contract$version %||% NA_character_,
    contract_sha      = contract$source_commit %||% NA_character_,
    roster_snapshot   = contract$snapshot %||% NA_character_,
    roster_sha256_16  = roster_id,
    baseline_supply   = contract$national %||% NA,
    seeds       = if (is.null(seeds)) NA_character_ else paste(seeds, collapse = ", "),
    iterations  = if (is.null(iterations)) NA_character_ else paste(iterations, collapse = ", "),
    parameters  = if (length(params)) paste(sprintf("%s=%s", names(params),
                       vapply(params, function(v) paste(v, collapse = "/"), character(1))),
                       collapse = "; ") else NA_character_,
    verification = "unverified until an end-to-end rerun matches the recorded values"
  )
  utils::modifyList(p, extra)
}

print_provenance <- function(p) {
  cat("\n== PROVENANCE ==\n")
  for (k in names(p)) cat(sprintf("  %-18s %s\n", k, paste(p[[k]], collapse = ", ")))
  if (isTRUE(p$git_dirty))
    cat("  NOTE               package sources are DIRTY; these numbers are not\n",
        "                     attributable to the recorded commit alone.\n", sep = "")
}

# ---- Run identity established BEFORE computation ---------------------------
#
# WHY THIS ORDER. Provenance stamped after a run describes the repository as it
# was when the stamp executed, not as it was when the numbers were produced. In
# this repository that distinction is not academic: an exploratory validation
# analysis ran while concurrent commits modified supply-model source files, so
# no single repository state corresponds to the complete analysis. The numbers
# were plausible and the code stayed syntactically valid the whole time -- the
# scientific object being evaluated simply changed underneath the computation.
#
# begin_validation_run() writes the manifest FIRST, refuses to start from a
# dirty tree, and returns a run directory the analysis writes its tables into.
# A number and its provenance then share a directory and cannot be separated.

# Inputs a manuscript run actually consumes. Hashed at start AND at finish: a
# pinned worktree makes CODE immutable, and does nothing about data-raw/, which
# is symlinked to the live checkout precisely because it is gitignored. Another
# session replacing an input mid-run is the same failure class as the concurrent
# commits, one layer down.
validation_inputs <- function() {
  c(roster    = "data-raw/urps_roster/urps_roster_2026-07-22.csv",
    coords    = "data-raw/urps_roster/urps_provider_coordinates_2026-08-02.csv",
    nrmp      = "data-raw/calibration/nrmp_urps_entrants_series.csv",
    backtest  = "artifacts/backtest_2020_to_2023_summary.csv",
    canonical = "config/canonical_sources.yml")
}

hash_inputs <- function(paths = validation_inputs()) {
  vapply(paths, function(f) {
    if (!file.exists(f)) return(NA_character_)
    tryCatch(digest::digest(file = f, algo = "sha256"), error = function(e) NA_character_)
  }, character(1))
}

begin_validation_run <- function(analysis, seeds = NULL, iterations = NULL,
                                 params = list(), root = "artifacts/validation",
                                 require_clean = TRUE, exploratory = !require_clean) {
  p <- validation_provenance(seeds = seeds, iterations = iterations, params = params)
  p$analysis <- analysis
  # EXPLORATORY IS UNAVOIDABLE AND STICKY. An exploratory run must never be
  # citable six months from now by someone who finds its directory and assumes
  # a number in it is manuscript evidence.
  p$exploratory <- isTRUE(exploratory)
  p$status <- if (p$exploratory) "EXPLORATORY -- not citable" else "started"

  if (isTRUE(require_clean) && !isTRUE(p$tree_clean_model)) {
    stop("begin_validation_run(): model sources are dirty. A manuscript run must ",
         "start from an immutable state -- use a pinned worktree:\n",
         "  git worktree add --detach /tmp/urpssim-validation <sha>\n",
         "Pass require_clean = FALSE only for exploratory work whose numbers will ",
         "not be cited.", call. = FALSE)
  }

  h <- hash_inputs()

  # The validation CODE actually executed, not just the repository SHA. These
  # can coincide while a single commit holds both; they mean different things
  # and a future split of the code should not silently collapse them.
  p$validation_code_sha256 <- tryCatch(
    substr(digest::digest(paste(unlist(lapply(
      sort(list.files("scripts/validation", "[.]R$", full.names = TRUE)),
      function(f) readLines(f, warn = FALSE))), collapse = "\n"), algo = "sha256"), 1, 16),
    error = function(e) NA_character_)

  # ATOMIC AND COLLISION-PROOF. dir.create() returns FALSE if the directory
  # already exists, so the loop claims a unique name rather than two concurrent
  # sessions writing into one evidence directory.
  base_id <- paste0(format(Sys.time(), "%Y%m%dT%H%M%S"), "_", analysis, "_",
                    substr(p$model_sha, 1, 7), if (p$exploratory) "_EXPLORATORY" else "")
  attempt <- 0L
  repeat {
    run_id <- if (attempt == 0L) base_id else paste0(base_id, "-", attempt)
    dir <- file.path(root, run_id)
    if (isTRUE(suppressWarnings(dir.create(dir, recursive = TRUE)))) break
    attempt <- attempt + 1L
    if (attempt > 50L) stop("begin_validation_run(): cannot claim a unique run directory.",
                            call. = FALSE)
  }
  p$run_id <- run_id

  lines <- c(vapply(names(p), function(k)
    sprintf("%-20s %s", k, paste(p[[k]], collapse = ", ")), character(1)),
    "", "input SHA-256 at start:",
    vapply(names(h), function(k) sprintf("  %-10s %s", k, h[[k]]), character(1)))
  writeLines(lines, file.path(dir, "MANIFEST.txt"))

  if (p$exploratory)
    writeLines(c("EXPLORATORY RUN -- NOT CITABLE.",
                 "Numbers here must not enter a manuscript and cannot be marked Reproduced."),
               file.path(dir, "EXPLORATORY"))

  cat("run_id:", run_id, "\nrun directory:", dir, "\n")
  print_provenance(p)
  invisible(list(dir = dir, run_id = run_id, provenance = p, input_hashes = h))
}

# A run that stopped partway must never look like completed evidence. Results
# are only trustworthy if the inputs they were computed from are the inputs the
# manifest recorded, so the hashes are rechecked here rather than assumed.
complete_validation_run <- function(run, tables = list()) {
  after <- hash_inputs()
  changed <- names(after)[!identical_hash(run$input_hashes, after)]
  if (length(changed)) {
    writeLines(c("FAILED: inputs changed during the run.",
                 paste("changed:", paste(changed, collapse = ", "))),
               file.path(run$dir, "FAILED"))
    stop("complete_validation_run(): input(s) changed while the analysis ran: ",
         paste(changed, collapse = ", "),
         ". These results are not attributable to the recorded manifest.",
         call. = FALSE)
  }
  for (nm in names(tables))
    utils::write.csv(tables[[nm]], file.path(run$dir, paste0(nm, ".csv")), row.names = FALSE)
  writeLines(c(sprintf("completed  %s", format(Sys.time(), "%Y-%m-%d %H:%M:%S %Z")),
               sprintf("run_id     %s", run$run_id),
               sprintf("status     %s", if (isTRUE(run$provenance$exploratory))
                 "EXPLORATORY -- not citable" else "authoritative_awaiting_reproduction"),
               "", "input SHA-256 at finish (unchanged):",
               vapply(names(after), function(k) sprintf("  %-10s %s", k, after[[k]]), character(1))),
             file.path(run$dir, "COMPLETED"))
  cat("\ncompleted:", run$run_id, "\n")
  invisible(run)
}

identical_hash <- function(a, b) {
  vapply(names(a), function(k) identical(unname(a[[k]]), unname(b[[k]])), logical(1))
}

# Reproduction is decided programmatically, not by eye.
compare_validation_runs <- function(dir_a, dir_b, tolerance = 0) {
  fa <- sort(list.files(dir_a, "[.]csv$")); fb <- sort(list.files(dir_b, "[.]csv$"))
  if (!identical(fa, fb)) {
    cat("DIFFERENT TABLE SETS:\n  a:", paste(fa, collapse = ", "),
        "\n  b:", paste(fb, collapse = ", "), "\n")
    return(invisible(FALSE))
  }
  ok <- TRUE
  for (f in fa) {
    a <- utils::read.csv(file.path(dir_a, f)); b <- utils::read.csv(file.path(dir_b, f))
    num <- vapply(a, is.numeric, logical(1))
    same <- isTRUE(all.equal(a[num], b[num], tolerance = tolerance)) &&
            identical(dim(a), dim(b)) && identical(names(a), names(b))
    cat(sprintf("  %-28s %s\n", f, if (same) "match" else "DIFFER"))
    ok <- ok && same
  }
  cat(if (ok) "REPRODUCED: every table matches.\n" else "NOT reproduced.\n")
  invisible(ok)
}

# ---- Enforcement, not documentation ----------------------------------------
#
# A manuscript-reporting path must be unable to read a run that did not complete
# cleanly. Stating the rule in a document leaves it to whoever is in a hurry.
read_validation_run <- function(dir, allow_exploratory = FALSE) {
  refuse <- function(why) stop("read_validation_run(): ", dir, " is not citable -- ", why,
                               call. = FALSE)
  if (!dir.exists(dir)) refuse("no such run directory.")
  if (file.exists(file.path(dir, "FAILED")))
    refuse("the run FAILED (inputs changed during execution).")
  if (!file.exists(file.path(dir, "COMPLETED")))
    refuse("no COMPLETED marker: the run did not finish, so its tables are partial.")
  if (!allow_exploratory && file.exists(file.path(dir, "EXPLORATORY")))
    refuse("it is an EXPLORATORY run and its numbers must not enter a manuscript.")
  man <- readLines(file.path(dir, "MANIFEST.txt"), warn = FALSE)
  if (any(grepl("^tree_clean_model\\s+FALSE", man)))
    refuse("model sources were dirty at launch, so no immutable state produced it.")
  tabs <- list.files(dir, "[.]csv$", full.names = TRUE)
  out <- lapply(tabs, utils::read.csv)
  names(out) <- sub("[.]csv$", "", basename(tabs))
  attr(out, "manifest") <- man
  attr(out, "run_id") <- sub("^run_id\\s+", "", grep("^run_id", man, value = TRUE)[1])
  out
}
