#!/bin/bash
# ---------------------------------------------------------------------------
# SessionStart hook: provision R for Claude Code on the web sessions.
#
# urpssim is an R package; web sessions start from a fresh container without R,
# so tests, checks and the example scripts cannot run. This installs base R when
# missing, then installs the package's declared dependencies.
#
# Web sessions can reach the Ubuntu archive but the egress policy blocks CRAN
# and its mirrors (cran.r-project.org, cloud.r-project.org, packagemanager.posit.co
# all return 403 from the agent proxy). We therefore install the pre-built
# r-cran-* .deb packages from the Ubuntu archive instead of building from source
# off CRAN. Dependencies are read from DESCRIPTION so this stays correct as the
# package evolves; base R packages are skipped, and any dependency without an
# r-cran-* package (e.g. arrow, or the GitHub-only mufflyaccess) is reported
# loudly rather than failing silently.
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
  $SUDO apt-get install -y --no-install-recommends r-base-core
fi

# 2) Package dependencies, from the Ubuntu archive (r-cran-* .debs).
if command -v Rscript >/dev/null 2>&1 && [ -f DESCRIPTION ]; then
  echo "[session-start] resolving R dependencies from DESCRIPTION ..."

  # Emit apt package names for a DESCRIPTION field, minus base R packages.
  desc_apt_names() {
    Rscript --vanilla -e '
      f <- commandArgs(trailingOnly = TRUE)[1]
      d <- read.dcf("DESCRIPTION")
      if (!f %in% colnames(d)) quit(save = "no")
      raw <- unlist(strsplit(d[, f], ","))
      nm  <- trimws(gsub("\\(.*\\)", "", raw))            # drop version constraints
      nm  <- nm[nzchar(nm) & nm != "R"]
      nm  <- setdiff(nm, rownames(installed.packages(priority = "base")))
      if (length(nm)) cat(paste0("r-cran-", tolower(nm)), sep = "\n")
    ' "$1"
  }

  mapfile -t imports  < <(desc_apt_names Imports)
  mapfile -t suggests < <(desc_apt_names Suggests)

  # Partition a list of apt names into those with an install candidate and those without.
  avail=(); missing_imports=(); missing_suggests=()
  classify() {
    local kind="$1"; shift
    local p cand
    for p in "$@"; do
      cand="$(apt-cache policy "$p" 2>/dev/null | awk '/Candidate:/{print $2}')"
      if [ -n "$cand" ] && [ "$cand" != "(none)" ]; then
        avail+=("$p")
      elif [ "$kind" = "import" ]; then
        missing_imports+=("$p")
      else
        missing_suggests+=("$p")
      fi
    done
  }
  [ "${#imports[@]}"  -gt 0 ] && classify import  "${imports[@]}"
  [ "${#suggests[@]}" -gt 0 ] && classify suggest "${suggests[@]}"

  if [ "${#avail[@]}" -gt 0 ]; then
    $SUDO apt-get update -qq || true
    echo "[session-start] installing ${#avail[@]} r-cran-* package(s) from the Ubuntu archive ..."
    # Non-fatal: a single broken package must not abort session start.
    if ! $SUDO apt-get install -y --no-install-recommends "${avail[@]}"; then
      echo "[session-start] WARNING: apt-get install returned non-zero; some deps may be missing." >&2
    fi
  fi

  # Report anything we could not install. Missing Imports are prominent because
  # the package cannot load without them; missing Suggests are informational.
  if [ "${#missing_imports[@]}" -gt 0 ]; then
    echo "[session-start] ERROR: no Ubuntu package for required Imports: ${missing_imports[*]}" >&2
    echo "[session-start]        urpssim will not load until these are installed by other means." >&2
  fi
  if [ "${#missing_suggests[@]}" -gt 0 ]; then
    echo "[session-start] note: no Ubuntu package for optional Suggests: ${missing_suggests[*]}"
    echo "[session-start]       (e.g. arrow has no r-cran-* build; mufflyaccess is GitHub-only)."
  fi

  # Verify required Imports actually load, and say so plainly either way.
  Rscript --vanilla -e '
    d <- read.dcf("DESCRIPTION")
    if (!"Imports" %in% colnames(d)) quit(save = "no")
    nm <- trimws(gsub("\\(.*\\)", "", unlist(strsplit(d[, "Imports"], ","))))
    nm <- setdiff(nm[nzchar(nm) & nm != "R"], rownames(installed.packages(priority = "base")))
    ok <- vapply(nm, requireNamespace, logical(1), quietly = TRUE)
    if (all(ok)) {
      cat(sprintf("[session-start] all %d required Imports load.\n", length(nm)))
    } else {
      cat("[session-start] ERROR: required Imports failed to load:",
          paste(nm[!ok], collapse = ", "), "\n", file = stderr())
    }
  ' || true
fi

echo "[session-start] R $(Rscript --vanilla -e 'cat(as.character(getRversion()))' 2>/dev/null || echo '?') ready."
