# Guards against the structural problems this repository has had before.
#
# Each test here corresponds to a defect that was actually present: functions
# defined twice so source order silently decided which one ran; personal
# filesystem paths hardcoded in library code; a file read at source time; library
# code installing packages. These are cheap to check and expensive to rediscover.

.repo_root <- function() {
  for (p in c(".", "..", file.path("..", ".."), file.path("..", "..", ".."))) {
    if (file.exists(file.path(p, "DESCRIPTION"))) return(p)
  }
  skip("repository root not found")
}

.r_files <- function(dir) {
  root <- .repo_root()
  d <- file.path(root, dir)
  if (!dir.exists(d)) skip(sprintf("%s not present", dir))
  list.files(d, pattern = "[.]R$", full.names = TRUE)
}

.definitions <- function(files) {
  out <- lapply(files, function(f) {
    x <- readLines(f, warn = FALSE)
    m <- grep("^[a-zA-Z._][a-zA-Z0-9._]* *<- *function", x)
    if (!length(m)) return(NULL)
    data.frame(file = basename(f), fn = sub(" *<- *function.*", "", x[m]),
               line = m, stringsAsFactors = FALSE)
  })
  do.call(rbind, out)
}

test_that("no function is defined twice in the package R directory", {
  defs <- .definitions(.r_files("R"))
  dup <- names(which(table(defs$fn) > 1))
  expect_equal(
    dup, character(0),
    info = paste("Duplicate definitions silently shadow one another:",
                 paste(dup, collapse = ", "))
  )
})

test_that("no legacy script defines the same function twice within itself", {
  # Within-file duplicates are the worst case: the earlier definition is dead
  # code, and nothing warns. Cross-file duplicates are allowed but must be
  # declared in LEGACY_CANONICAL (checked below).
  defs <- .definitions(.r_files(file.path("inst", "legacy")))
  within <- defs[duplicated(defs[, c("file", "fn")]), ]
  expect_equal(
    nrow(within), 0L,
    info = paste("Within-file duplicates:",
                 paste(sprintf("%s:%s", within$file, within$fn), collapse = ", "))
  )
})

test_that("every cross-file legacy collision has a declared owner", {
  dir <- file.path(.repo_root(), "inst", "legacy")
  skip_if_not(dir.exists(dir))
  collisions <- legacy_collisions(dir)
  undeclared <- setdiff(collisions$fn, names(LEGACY_CANONICAL))
  expect_equal(undeclared, character(0),
               info = paste("Add to LEGACY_CANONICAL:", paste(undeclared, collapse = ", ")))
})

test_that("the legacy load order delivers the declared canonical implementations", {
  dir <- file.path(.repo_root(), "inst", "legacy")
  skip_if_not(dir.exists(dir))
  mismatches <- check_legacy_canonical(dir)
  expect_equal(nrow(mismatches), 0L,
               info = paste(utils::capture.output(print(mismatches)), collapse = "\n"))
})

test_that("no source file hardcodes a personal filesystem path", {
  files <- c(.r_files("R"), .r_files(file.path("inst", "legacy")),
             .r_files("scripts"))
  # R/00-paths.R is the resolver itself: the home-directory candidates it probes
  # are fallbacks, not hardcoded inputs. This test file contains the pattern it
  # searches for. Both are exempt by construction.
  files <- files[!basename(files) %in% c("00-paths.R", "test-repo-hygiene.R")]
  bad <- character(0)
  for (f in files) {
    x <- readLines(f, warn = FALSE)
    # Ignore roxygen and plain comments: documenting a historical path is fine.
    code <- x[!grepl("^\\s*#", x)]
    hits <- grep("/Users/|~/Dropbox|C:\\\\Users", code, value = TRUE)
    if (length(hits)) bad <- c(bad, sprintf("%s: %s", basename(f), hits[1]))
  }
  expect_equal(bad, character(0),
               info = paste("Route through swan_path()/data_raw_path()/external_path():",
                            paste(bad, collapse = " | ")))
})

test_that("no code calls install.packages()", {
  # Walk the AST rather than the deparsed text, so a mention inside a message
  # string ("Please install with: install.packages(...)") is not a false hit.
  files <- c(.r_files("R"), .r_files(file.path("inst", "legacy")))
  offenders <- character(0)
  for (f in files) {
    e <- tryCatch(parse(f), error = function(e) NULL)
    if (is.null(e)) next
    calls <- unlist(lapply(as.list(e), function(x) all.names(x, functions = TRUE)))
    if ("install.packages" %in% calls) offenders <- c(offenders, basename(f))
  }
  expect_equal(unique(offenders), character(0),
               info = "Library code must not install packages on the user's behalf.")
})

test_that("loading the legacy scripts never touches the filesystem", {
  # The legacy scripts DO contain top-level read_rds()/load()/read_excel() calls.
  # That is tolerated because load_legacy() evaluates definitions only. This test
  # is the guarantee that makes it tolerable: loading must succeed on a machine
  # with no external data present at all.
  dir <- file.path(.repo_root(), "inst", "legacy")
  skip_if_not(dir.exists(dir))
  env <- new.env(parent = globalenv())
  withr_root <- Sys.getenv("SIMULATION_DATA_ROOT")
  Sys.setenv(SIMULATION_DATA_ROOT = file.path(tempdir(), "definitely-not-here"))
  on.exit(Sys.setenv(SIMULATION_DATA_ROOT = withr_root), add = TRUE)

  expect_no_error(suppressMessages(load_legacy(dir, envir = env, quiet = TRUE)))
  # ...and the functions really did get defined.
  expect_true(is.function(env$validate_dpmm_with_swan_data))
  expect_true(is.function(env$simulate_supply))
})

test_that("the definition detector rejects statements that do work", {
  expect_true(.is_definition(quote(f <- function(x) x + 1)))
  expect_true(.is_definition(quote(K <- c(1, 2, 3))))
  expect_false(.is_definition(quote(d <- readRDS("x.rds"))))
  expect_false(.is_definition(quote(load("x.rda"))))
  expect_false(.is_definition(quote(library(dplyr))))
  expect_false(.is_definition(quote(plot(1:10))))
})

test_that("the package R directory has no top-level executable script code", {
  # Constants, stopifnot() guards and function definitions are fine. Anything
  # that performs work at load time is not.
  #
  # utils::globalVariables() is allowed: it is the sanctioned way to declare
  # names that R CMD check would otherwise read as undeclared bindings, and it
  # only registers metadata. It is matched on the fully-qualified form so a bare
  # call to something else via `::` is still caught.
  allowed <- c("<-", "=", "stopifnot", "if", "library", "requireNamespace")
  for (f in .r_files("R")) {
    e <- parse(f)
    for (x in e) {
      if (!is.call(x)) next
      head_fn <- as.character(x[[1]])[1]
      if (head_fn %in% allowed) next
      if (identical(head_fn, "::")) {
        qualified <- paste(as.character(x[[1]])[2:3], collapse = "::")
        if (identical(qualified, "utils::globalVariables")) next
        fail(sprintf("%s has a top-level call to %s()", basename(f), qualified))
        next
      }
      fail(sprintf("%s has a top-level call to %s()", basename(f), head_fn))
    }
  }
  succeed()
})

test_that("declared Imports cover the namespaces the package calls", {
  root <- .repo_root()
  desc <- read.dcf(file.path(root, "DESCRIPTION"))
  imports <- trimws(unlist(strsplit(
    paste(desc[, intersect(c("Imports", "Depends"), colnames(desc))], collapse = ","), ",")))
  imports <- sub("\\s*\\(.*", "", imports)
  suggests <- trimws(unlist(strsplit(paste(desc[, "Suggests"], collapse = ","), ",")))
  suggests <- sub("\\s*\\(.*", "", suggests)

  # Scan the parsed code, not raw text: a comment mentioning `cliff::wc_project`
  # is not a dependency.
  used <- character(0)
  for (f in .r_files("R")) {
    e <- tryCatch(parse(f), error = function(e) NULL)
    if (is.null(e)) next
    nm <- unlist(lapply(as.list(e), function(x) all.names(x, functions = TRUE)))
    idx <- which(nm %in% c("::", ":::"))
    if (length(idx)) used <- c(used, nm[idx + 1L])
  }
  missing <- setdiff(unique(used), c(imports, suggests, "base", "urpssim"))
  expect_equal(missing, character(0),
               info = paste("Add to DESCRIPTION Imports:", paste(missing, collapse = ", ")))
})

test_that("the package does not redefine operators that base or rlang provide", {
  # Defining `%||%` locally shadows base R (>= 4.4) and rlang, so behaviour would
  # depend on attach order.
  for (f in .r_files("R")) {
    x <- readLines(f, warn = FALSE)
    hits <- grep("^`%\\|\\|%` *<- *function", x)
    expect_equal(length(hits), 0L,
                 info = sprintf("%s redefines %%||%% unconditionally", basename(f)))
  }
})
