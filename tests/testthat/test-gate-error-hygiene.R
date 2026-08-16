# Gate: user-facing errors do not leak the internal call.
#
# The repository convention is that every stop()/warning() that BUILDS A MESSAGE
# passes `call. = FALSE`, so the user sees the diagnostic the author wrote, not
# `Error in .last_leaf_helper(x, mode) : ...` naming an internal frame the caller
# never invoked and cannot act on. Thirteen calls had drifted from this (four in
# data-urps_population.R alone) and read as internal errors.
#
# This gate walks the AST (not the text, so `call.` inside a string is not a
# false pass) and flags any message-building stop()/warning() lacking a `call.`
# argument. It deliberately does NOT flag a bare condition re-raise -- stop(cond)
# carries its own call semantics -- by keying on a string-building first
# argument (a literal, or sprintf/paste/paste0/gettextf/format/formatC).

geh_root <- function() {
  # Sources, not just "a package" -- see .source_tree_root() in helper-setup.R.
  # This gate reads R/*.R as text, which an installed tree does not ship.
  r <- .source_tree_root()
  if (length(r) == 0) NULL else r
}

geh_builds_message <- function(arg) {
  if (is.character(arg)) return(TRUE)
  if (is.call(arg)) {
    h <- arg[[1]]
    if (is.name(h) && as.character(h) %in%
        c("sprintf", "paste", "paste0", "gettextf", "format", "formatC")) return(TRUE)
  }
  FALSE
}

geh_violations <- function(root) {
  files <- list.files(file.path(root, "R"), "[.]R$", full.names = TRUE)
  out <- character()
  walk <- function(e, file) {
    if (!is.call(e)) return(invisible())
    h <- e[[1]]
    if (is.name(h) && as.character(h) %in% c("stop", "warning")) {
      args <- as.list(e)[-1]
      has_call <- "call." %in% names(args)
      first_msg <- length(args) >= 1 && geh_builds_message(args[[1]])
      if (first_msg && !has_call) out[[length(out) + 1]] <<- basename(file)
    }
    for (a in as.list(e)) if (!missing(a) && (is.call(a) || is.pairlist(a))) walk(a, file)
  }
  for (f in files) {
    ex <- tryCatch(parse(f), error = function(e) NULL)
    if (is.null(ex)) next
    for (e in ex) walk(e, f)
  }
  out
}

test_that("every message-building stop()/warning() passes call. = FALSE", {
  root <- geh_root()
  skip_if(is.null(root), "repository root not reachable")
  v <- geh_violations(root)
  # Zero, not a ratchet: the convention is universal and the 13 known drifts are
  # fixed, so any new omission should fail immediately rather than accumulate.
  expect_equal(sort(unique(v)), character(0),
               info = paste("stop()/warning() missing call. = FALSE in:",
                            paste(sort(table(v)), names(sort(table(v))),
                                  collapse = "; ")))
})
