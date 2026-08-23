# No manuscript analysis may read a non-canonical artifact ----
#
# THE INVARIANT. A script under scripts/validation/ produces run-identified
# evidence that reaches a manuscript. It may read canonical inputs -- registered
# in config/canonical_sources.yml with a SHA-256 and hashed into the run
# manifest. It may NOT read a derived intermediate, a download cache, or an
# obsolete prototype, because none of those carries a hash, a manifest entry, or
# any guarantee about which source produced it.
#
# WHY A TEST AND NOT A CONVENTION. The failure this prevents is not a crash. A
# validation script reading `urps_basket_prov_svc.rds` instead of the raw PUF
# would run fine, produce plausible bounds, and reproduce at zero tolerance --
# while the number's actual provenance quietly became "whatever RDS happened to
# be on that laptop". Reproducibility machinery cannot detect that, because the
# arithmetic really would be identical on both runs.
#
# The file names below are checked even though the files are deleted. That is
# deliberate: the prohibition has to outlive the artifact, or the first person to
# regenerate one reintroduces the defect with nothing objecting.
#
# Classes and rationale: docs/DATA_ARTIFACT_INVENTORY.md

root <- tryCatch(rprojroot::find_root(rprojroot::has_file("DESCRIPTION")),
                 error = function(e) NA_character_)
skip_if(is.na(root), "repository root not reachable")

# Non-canonical artifacts, by the class that forbids them.
FORBIDDEN <- c(
  # derived intermediates
  "urps_basket_prov_svc",
  # download caches
  "meps_FYC_2023", "meps_COND_2023", "meps_CLNK_2023", "meps_ob_2023",
  "cms_datajson",
  # obsolete exploratory
  "fitted_model"
)

validation_scripts <- function() {
  d <- file.path(root, "scripts", "validation")
  list.files(d, pattern = "^[0-9]+_.*[.]R$", full.names = TRUE)
}

# ---- Layer 1: resolve what each script actually opens -----------------------
#
# A NAME SCAN IS NOT PROOF OF ABSENCE. Grepping stripped source for
# `urps_basket_prov_svc` catches the literal call and misses
# `readRDS(file.path(cache_dir, f))` entirely. Indirection evades a text
# pattern, so the primary check parses instead: it walks the AST for reader
# calls, resolves the path argument through the script's own top-level
# constants, and requires every resolved path to be a DECLARED canonical input.
#
# That inverts the burden. A name scan asks "is this one forbidden file
# mentioned?" and can only ever enumerate files somebody thought of. This asks
# "is everything you open declared?", so a new cache invented next year is
# caught without anyone adding it to a list.
#
# Where it cannot resolve a path it says so rather than passing. See the
# `unresolved` test below -- that count is the honest boundary of the guarantee.

READERS <- c("readRDS", "fread", "read.csv", "read_csv", "read.delim",
             "readLines", "read_yaml", "read.table", "load", "readxl::read_excel")

# Only character literals and file.path() of them are evaluated. Anything else
# returns NULL and is reported unresolved -- evaluating arbitrary expressions
# from a script under test would be both unsafe and a lie about what is known.
# as.character() on a call head can return length > 1 (`utils::read.csv`,
# `x$y`), which errors under && in R >= 4.3. One helper, always length one.
.head_name <- function(e) {
  if (!is.call(e)) return("")
  h <- e[[1]]
  if (is.name(h)) return(as.character(h))
  paste(deparse(h), collapse = "")
}

.static_value <- function(e, env) {
  if (is.character(e) && length(e) == 1L) return(e)
  if (is.name(e)) return(env[[as.character(e)]] %||% NULL)
  if (identical(.head_name(e), "file.path")) {
    parts <- lapply(as.list(e)[-1], .static_value, env = env)
    if (any(vapply(parts, is.null, logical(1)))) return(NULL)
    return(do.call(file.path, parts))
  }
  NULL
}

# Top-level `NAME <- <literal | file.path(...)>` only. A constant built inside a
# block or an if() is deliberately not followed.
.script_constants <- function(exprs) {
  env <- list()
  for (e in exprs) {
    if (.head_name(e) %in% c("<-", "=") && is.name(e[[2]])) {
      v <- .static_value(e[[3]], env)
      if (!is.null(v)) env[[as.character(e[[2]])]] <- v
    }
  }
  env
}

.walk_reads <- function(e, env, acc) {
  if (is.call(e)) {
    fn <- sub("^.*::", "", .head_name(e))
    if (fn %in% sub("^.*::", "", READERS)) {
      args <- as.list(e)[-1]
      # digest(file = x) and read fns take the path first or as `file`/`input`.
      p <- if (!is.null(args$file)) args$file else
           if (!is.null(args$input)) args$input else
           if (length(args)) args[[1]] else NULL
      if (!is.null(p))
        acc[[length(acc) + 1L]] <- list(
          fn = fn, expr = paste(deparse(p), collapse = ""),
          path = .static_value(p, env))
    }
    if (identical(fn, "digest") && !is.null(as.list(e)$file)) {
      p <- as.list(e)$file
      acc[[length(acc) + 1L]] <- list(
        fn = "digest(file=)", expr = paste(deparse(p), collapse = ""),
        path = .static_value(p, env))
    }
    for (a in as.list(e)[-1]) if (!missing(a)) acc <- .walk_reads(a, env, acc)
  }
  acc
}

script_reads <- function(f) {
  exprs <- tryCatch(as.list(parse(f)), error = function(e) NULL)
  if (is.null(exprs)) return(list())
  env <- .script_constants(exprs)
  acc <- list()
  for (e in exprs) acc <- .walk_reads(e, env, acc)
  acc
}

# Paths a validation script is allowed to open, beyond declared canonical
# sources: its own version-controlled mapping tables, the registry, and the
# artifacts tree it writes into and reads back for A/B comparison.
ALLOWED_PREFIXES <- c("scripts/validation/", "config/", "artifacts/", "data-raw/")

test_that("every path a validation script opens resolves to a declared location", {
  # LAYER 1, the primary check: parse, resolve, require declaration. This is
  # what catches indirection a name scan cannot see.
  scripts <- validation_scripts()
  skip_if(length(scripts) == 0L, "repository root not reachable")

  bad <- character()
  for (f in scripts) {
    for (r in script_reads(f)) {
      if (is.null(r$path)) next          # unresolved: reported by the next test
      p <- sub("^[.]/", "", r$path)
      if (!any(startsWith(p, ALLOWED_PREFIXES)))
        bad <- c(bad, sprintf("%s: %s(%s) -> %s", basename(f), r$fn, r$expr, p))
      if (any(vapply(FORBIDDEN, function(b) grepl(b, p, fixed = TRUE), logical(1))))
        bad <- c(bad, sprintf("%s: %s reads a NON-CANONICAL artifact -> %s",
                              basename(f), r$fn, p))
    }
  }

  expect_equal(
    bad, character(),
    info = paste0(
      "A validation script opens a path that is not a declared location:\n  ",
      paste(bad, collapse = "\n  "),
      "\nSee docs/DATA_ARTIFACT_INVENTORY.md. Either read a canonical input, ",
      "or promote the artifact with a SHA-256 and a manifest entry."))
})

test_that("the resolver's blind spots are known and bounded", {
  # THE HONEST BOUNDARY. Layer 1 resolves character literals and file.path() of
  # literals through top-level constants. A path built inside a block, from a
  # function argument, or from a loop variable is NOT resolved -- it is counted
  # here instead of being silently treated as fine.
  #
  # Two reads are unresolvable for reasons that are legitimate, not sloppy:
  #   - 03's PRODUCTIVITY_REPORT is assigned inside a block that picks the
  #     first of four candidate extensions present on disk. Its candidates
  #     are all under data-raw/productivity/.
  #   - 04's SPEC_FILE is file.path(ARCHIVE_DIR, ...), where ARCHIVE_DIR is
  #     Sys.getenv("CADR_DIR", unset = "data-raw/cadr") -- an environment
  #     override, not a literal.
  # 07_service_share_calibration_validation.R adds six more of the same
  # ARCHIVE_DIR shape: its `paths` come from Sys.getenv() lookups over
  # URPS_SERVICE_SHARE_EVENTS / URPS_CALIBRATED_SERVICE_SHARE_BUNDLE /
  # URPS_CMS_SERVICE_SHARE_EVIDENCE / URPS_CHIA_SERVICE_SHARE_EVIDENCE, which
  # exist so the real-data validation run points at CI-mounted PUFs that are
  # never committed to the repo. Hardcoding them as top-level constants would
  # defeat the point -- the script already fails closed with
  # file.exists()-checked, named env vars when nothing is mounted (see
  # "Verify mounted evidence paths" in service-share-validation.yml), so this
  # indirection is deliberate, not a missed guarantee.
  #
  # This asserts the count does not GROW BEYOND today's known, justified set.
  # A new unresolvable read past this bound is a new hole in the guarantee,
  # and it should cost a deliberate edit here.
  scripts <- validation_scripts()
  skip_if(length(scripts) == 0L, "repository root not reachable")

  unresolved <- character()
  for (f in scripts)
    for (r in script_reads(f))
      if (is.null(r$path))
        unresolved <- c(unresolved, sprintf("%s: %s(%s)", basename(f), r$fn, r$expr))

  if (length(unresolved) > 8L)
    fail(paste0(
      "Reads whose path the resolver cannot evaluate statically:\n  ",
      paste(unresolved, collapse = "\n  "),
      "\nEach is a gap in the classification guarantee. Prefer a top-level ",
      "constant; if the indirection is genuinely needed, raise this bound ",
      "deliberately and say why."))
  expect_lte(length(unresolved), 8L)
})

test_that("no validation script mentions a non-canonical artifact by name", {
  # LAYER 2, a cheap backstop for the case layer 1 cannot see: a forbidden file
  # reached through a helper this test does not know is a reader. Documented as
  # a GUARDRAIL, not proof of absence -- if both layers pass, what has been
  # established is that no DECLARED reader opens an UNDECLARED path, and that no
  # forbidden name appears in code. Neither is a proof that no read happens.
  scripts <- validation_scripts()
  skip_if(length(scripts) == 0L, "repository root not reachable")

  offenders <- character()
  for (f in scripts) {
    # Comments are stripped first. These names are DISCUSSED in headers -- 04
    # explains that its extract used to live in a scratchpad, and that sentence
    # must not be read as a call site. A gate that cannot tell a warning from a
    # violation trains people to work around it.
    src <- readLines(f, warn = FALSE)
    src <- sub("#.*$", "", src)
    for (bad in FORBIDDEN) {
      if (any(grepl(bad, src, fixed = TRUE)))
        offenders <- c(offenders, sprintf("%s reads %s", basename(f), bad))
    }
  }

  expect_equal(
    offenders, character(),
    info = paste0(
      "A validation script reads a non-canonical artifact:\n  ",
      paste(offenders, collapse = "\n  "),
      "\nEither read the canonical source instead (see ",
      "config/canonical_sources.yml), or promote the artifact to a canonical ",
      "input with a SHA-256 and a manifest entry. Do not relax this test: the ",
      "number would still be reproducible and would no longer be attributable."))
})

test_that("analysis 05 reads the raw PUF, by name", {
  # The positive half. Asserting only the absence of the wrong path would still
  # pass if 05 stopped reading any CMS file at all.
  f <- file.path(root, "scripts", "validation",
                 "05_urps_share_partial_identification.R")
  skip_if_not(file.exists(f), "repository root not reachable")
  src <- readLines(f, warn = FALSE)
  expect_true(any(grepl("PHY_R26_P05_V10_D24_Prov_Svc.csv", src, fixed = TRUE)))
  expect_true(any(grepl("MUP_PHY_R26_P05_V10_D24_Geo.csv", src, fixed = TRUE)))
})

test_that("both CMS PUFs are registered with a SHA-256", {
  # What makes them canonical rather than merely present.
  cfg <- file.path(root, "config", "canonical_sources.yml")
  skip_if_not(file.exists(cfg), "canonical source registry not reachable")
  y <- yaml::read_yaml(cfg)
  src <- y$sources %||% y
  for (id in c("cms_mup_phy_2024_prov_svc", "cms_mup_phy_2024_geo")) {
    expect_true(!is.null(src[[id]]), info = id)
    expect_true(nzchar(src[[id]]$sha256 %||% ""), info = id)
    expect_match(src[[id]]$sha256, "^[0-9a-f]{64}$", info = id)
  }
})

test_that("the convenience builder declares itself non-canonical", {
  # The banner is load-bearing: it is what a reader who finds the RDS in six
  # months will follow back. If it is edited away, this fails.
  f <- file.path(root, "scripts", "data", "build_urps_basket_prov_svc.R")
  skip_if_not(file.exists(f), "repository root not reachable")
  src <- paste(readLines(f, warn = FALSE), collapse = "\n")
  expect_true(grepl("NOT a canonical input", src, fixed = TRUE))
  expect_true(grepl("NOT consumed by manuscript analysis 05", src, fixed = TRUE))
  # And it must resolve its source through the registry, not by filename.
  expect_true(grepl("resolve_canonical", src, fixed = TRUE))
})

test_that("the inventory documents every forbidden artifact", {
  # A prohibition with no explanation gets deleted by whoever it inconveniences.
  inv <- file.path(root, "docs", "DATA_ARTIFACT_INVENTORY.md")
  skip_if_not(file.exists(inv), "repository root not reachable")
  txt <- paste(readLines(inv, warn = FALSE), collapse = "\n")
  undocumented <- Filter(function(b) !grepl(b, txt, fixed = TRUE), FORBIDDEN)
  expect_equal(undocumented, character())
})
