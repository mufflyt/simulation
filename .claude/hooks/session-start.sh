#!/bin/bash
# ---------------------------------------------------------------------------
# SessionStart hook: provision R for Claude Code on the web sessions.
#
# urpssim is an R package; web sessions start from a fresh container without R,
# so tests, checks and the example scripts cannot run. This installs base R when
# missing, then best-effort installs the package's declared dependencies. The
# dependency step is non-fatal: if the network policy blocks CRAN, base R is
# still available and session start is not aborted.
# ---------------------------------------------------------------------------
set -euo pipefail

# Web (remote) sessions only.
if [ "${CLAUDE_CODE_REMOTE:-}" != "true" ]; then
  exit 0
fi

SUDO=""
if [ "$(id -u)" -ne 0 ]; then SUDO="sudo"; fi

# 1) Base R (Ubuntu main archive).
if ! command -v Rscript >/dev/null 2>&1; then
  echo "[session-start] installing r-base-core ..."
  export DEBIAN_FRONTEND=noninteractive
  $SUDO apt-get update -qq || true
  $SUDO apt-get install -y --no-install-recommends r-base-core
fi

# 2) Package dependencies (best-effort). pak resolves Imports+Suggests from
#    DESCRIPTION and installs them; cached in the container image afterwards.
if command -v Rscript >/dev/null 2>&1 && [ -f DESCRIPTION ]; then
  echo "[session-start] installing R package dependencies (best-effort) ..."
  Rscript -e 'tryCatch({
      if (!requireNamespace("pak", quietly = TRUE))
        install.packages("pak", repos = "https://cloud.r-project.org")
      pak::local_install_deps(root = ".", dependencies = TRUE, upgrade = FALSE)
    }, error = function(e) message("dependency install skipped: ", conditionMessage(e)))' \
    || echo "[session-start] dependency install skipped (CRAN unreachable?); base R is available."
fi

echo "[session-start] R $(Rscript --vanilla -e 'cat(as.character(getRversion()))' 2>/dev/null || echo '?') ready."
