#!/bin/bash
# ---------------------------------------------------------------------------
# SessionStart hook: provision R for Claude Code on the web sessions.
#
# urpssim is an R package; web sessions start from a fresh container without R,
# so tests, R CMD check and the example scripts cannot run. This installs base R
# when missing, the system libraries the compiled dependencies need, then the
# package's declared dependencies plus the check/document tooling.
#
# Design notes (why this is not a single pak::local_install_deps call):
#   * The DESCRIPTION lists mufflyaccess (a PRIVATE repo, wired via Remotes) in
#     Suggests. Resolving the whole dependency set as ONE transaction means a
#     missing private-repo token aborts the ENTIRE install -- so NOTHING lands,
#     not even duckdb/knitr. We therefore install in tiers:
#       1. hard deps (Depends/Imports/LinkingTo)  -- all public CRAN, attempted first
#       2. public Suggests + dev tooling          -- best-effort
#       3. mufflyaccess                            -- only when a token is present
#   * The package lists come from DESCRIPTION at run time (no hardcoded drift).
#   * Everything is best-effort overall: if the network policy blocks CRAN, base
#     R is still available and session start is never aborted.
# ---------------------------------------------------------------------------
set -euo pipefail

# Web (remote) sessions only.
if [ "${CLAUDE_CODE_REMOTE:-}" != "true" ]; then
  exit 0
fi

SUDO=""
if [ "$(id -u)" -ne 0 ]; then SUDO="sudo"; fi

export DEBIAN_FRONTEND=noninteractive

# 1) Base R (Ubuntu main archive).
if ! command -v Rscript >/dev/null 2>&1; then
  echo "[session-start] installing r-base-core ..."
  $SUDO apt-get update -qq || true
  $SUDO apt-get install -y --no-install-recommends r-base-core || true
fi

# 2) System libraries for the compiled dependencies (sf -> GDAL/GEOS/PROJ/udunits,
#    curl/openssl/xml for the tidyverse stack) and pandoc for the vignettes.
#    Best-effort: skip silently if apt is unavailable or the packages are present.
echo "[session-start] installing system libraries (best-effort) ..."
$SUDO apt-get update -qq || true
$SUDO apt-get install -y --no-install-recommends \
  libcurl4-openssl-dev libssl-dev libxml2-dev \
  libgdal-dev libgeos-dev libproj-dev libudunits2-dev \
  pandoc >/dev/null 2>&1 || echo "[session-start] system-library step skipped."

# 3) Let pak reach the private mufflyaccess contract when a token is provided.
if [ -z "${GITHUB_PAT:-}" ] && [ -n "${MUFFLYACCESS_PAT:-}" ]; then
  export GITHUB_PAT="$MUFFLYACCESS_PAT"
fi

# 4) R package dependencies + check/document tooling (tiered, best-effort).
if command -v Rscript >/dev/null 2>&1 && [ -f DESCRIPTION ]; then
  echo "[session-start] installing R dependencies (tiered, best-effort) ..."
  Rscript --vanilla -e '
    options(
      repos = c(CRAN = Sys.getenv("RSPM", "https://cloud.r-project.org")),
      warn  = 1,
      Ncpus = max(1L, tryCatch(parallel::detectCores(), error = function(e) 1L))
    )
    if (!requireNamespace("pak", quietly = TRUE))
      install.packages("pak")

    # Read the dependency tiers from DESCRIPTION (source of truth, no drift).
    dcf <- read.dcf("DESCRIPTION")
    field <- function(f) {
      if (!f %in% colnames(dcf)) return(character())
      x <- trimws(gsub("\\(.*?\\)", "", strsplit(dcf[, f], ",")[[1]]))
      setdiff(x[nzchar(x)], "R")
    }
    hard  <- unique(c(field("Depends"), field("Imports"), field("LinkingTo")))
    sugg  <- setdiff(field("Suggests"), "mufflyaccess")   # public Suggests only
    tools <- c("rcmdcheck", "roxygen2")                   # run tests + regenerate docs

    try_install <- function(pkgs, label) {
      if (!length(pkgs)) return(invisible())
      tryCatch(
        pak::pak(pkgs, ask = FALSE, upgrade = FALSE),
        error = function(e)
          message(sprintf("[session-start] %s install partial: %s", label, conditionMessage(e)))
      )
    }

    # Tier 1: hard deps (attempted first so a private-token failure cannot block them).
    try_install(hard, "hard-deps")
    # Tier 2: public Suggests + dev tooling.
    try_install(sugg,  "suggests")
    try_install(tools, "tooling")
    # Tier 3: private contract, only when a token is present.
    if (nzchar(Sys.getenv("GITHUB_PAT")))
      try_install("mufflyt/mufflyaccess", "mufflyaccess")
  ' || echo "[session-start] dependency install incomplete (CRAN unreachable?); base R is available."
fi

echo "[session-start] R $(Rscript --vanilla -e 'cat(as.character(getRversion()))' 2>/dev/null || echo '?') ready."
