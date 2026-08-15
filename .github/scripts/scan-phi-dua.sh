#!/usr/bin/env bash
# PHI / DUA leak scanner for a PUBLIC repository.
#
# This repo is public and works with CHIA Case Mix (DUA-restricted). PHI has
# existed on the same machine (a cuff-study .RData carrying PatientName,
# PAT_ADDR_1, CITY, ZIP_CODE, MRN, BirthDate). None of it belongs here, and a
# public repo leaks through HISTORY as much as through HEAD.
#
# TWO SEVERITIES, DELIBERATELY.
#
#   CRITICAL  a reportable disclosure. Fails immediately, no baseline, no
#             ratchet, no way to grandfather it.
#   HIGH      hygiene that is real but pre-existing and widespread (personal
#             paths leak a username and local layout; large tracked files bloat
#             a public clone). Enforced as a RATCHET against
#             .github/phi-dua-baseline.txt: the count may fall, never rise.
#
# The ratchet matters. Failing night one on ~20 pre-existing findings would get
# this gate disabled within a week, which is worse than not having it. A ratchet
# stops the bleeding immediately and lets the backlog drain on its own schedule.
set -uo pipefail
MODE="${1:-worktree}"                       # worktree | history
BASELINE_FILE="${2:-.github/phi-dua-baseline.txt}"
CRIT=0
HIGH=0

crit() { printf '::error::[CRITICAL] %s :: %s\n' "$1" "$2"; CRIT=$((CRIT+1)); }
high() { printf '::warning::[HIGH] %s :: %s\n' "$1" "$2"; HIGH=$((HIGH+1)); }

# Identifier column names that only appear in real patient extracts.
PHI_COLS='PatientName|PAT_ADDR_[0-9]|PAT_CITY|BirthDate|DateOfBirth|\bMRN\b|MedicalRecordNum|SocialSecurity|PAT_ZIP'
# DUA-restricted CHIA artifacts, by filename.
DUA_FILES='\.mdb$|\.accdb$|casemix.*\.zip$|HIDD.*\.txt$|chia.*\.duckdb$'
# SSN-shaped. Public-use fixed-width survey files (NAMCS/NHAMCS/MCBS/MEPS/BRFSS)
# are excluded: they are de-identified by construction and their digit runs
# match this pattern by coincidence.
SSN='[0-9]{3}-[0-9]{2}-[0-9]{4}'
PUBLIC_USE_EXCLUDE=":!data-raw/nhamcs/* :!data-raw/mcbs/* :!data-raw/meps/* :!data-raw/brfss/* :!data-raw/nhanes/*"

echo "== PHI/DUA scan: $MODE =="

if [ "$MODE" = "worktree" ]; then
  # CRITICAL 1: DUA-restricted files tracked by name
  while IFS= read -r f; do
    [ -n "$f" ] && crit "DUA-restricted file is tracked" "$f"
  done < <(git ls-files | grep -E "$DUA_FILES" || true)

  # CRITICAL 2: PHI column names in tracked content
  while IFS= read -r m; do
    [ -n "$m" ] && crit "PHI column name in tracked content" "${m:0:200}"
  done < <(git grep -InE "$PHI_COLS" -- ':!*.Rd' ':!docs/*' \
             ':!.github/scripts/*' ':!tests/testthat/test-phi-dua-guard.R' \
             $PUBLIC_USE_EXCLUDE 2>/dev/null | head -40 || true)

  # CRITICAL 3: SSN-shaped strings outside public-use survey extracts
  while IFS= read -r m; do
    [ -n "$m" ] && crit "SSN-shaped string" "${m:0:200}"
  done < <(git grep -InE "$SSN" -- ':!*.lock' ':!.github/scripts/*' \
             $PUBLIC_USE_EXCLUDE 2>/dev/null | head -20 || true)

  # HIGH 1: absolute personal paths (leak username + local layout)
  while IFS= read -r m; do
    [ -n "$m" ] && high "absolute personal path" "${m:0:160}"
  done < <(git grep -InE '/Users/[a-z]+|/home/[a-z]+|/Volumes/[A-Za-z]' -- \
             'R/*' 'tests/*' 'scripts/*' ':!.github/scripts/*' 2>/dev/null || true)

  # HIGH 2: oversized tracked files
  while IFS= read -r f; do
    [ -f "$f" ] || continue
    sz=$(wc -c < "$f" 2>/dev/null | tr -d ' ')
    case "$sz" in ''|*[!0-9]*) continue ;; esac
    [ "$sz" -gt 10485760 ] && high "tracked file over 10MB" "$f ($((sz/1048576))MB)"
  done < <(git ls-files || true)

else
  # HISTORY: deleted blobs still ship in a public clone.
  while IFS= read -r f; do
    [ -n "$f" ] && crit "DUA-restricted file exists in git history" "$f"
  done < <(git log --all --pretty=format: --name-only --diff-filter=A 2>/dev/null \
             | sort -u | grep -E "$DUA_FILES" || true)

  for needle in PatientName PAT_ADDR_1 MedicalRecordNum; do
    while IFS= read -r m; do
      [ -n "$m" ] && crit "PHI identifier introduced in history ($needle)" "$m"
    done < <(git log --all --oneline -S"$needle" --pretty=format:'%h %ad %s' \
               --date=short 2>/dev/null | head -5 || true)
  done
fi

echo "---"
echo "CRITICAL: $CRIT"
echo "HIGH:     $HIGH"

# --- ratchet -----------------------------------------------------------------
if [ "$MODE" = "worktree" ]; then
  BASE=0
  [ -f "$BASELINE_FILE" ] && BASE=$(grep -E '^high=' "$BASELINE_FILE" | cut -d= -f2 | tr -d ' ')
  case "$BASE" in ''|*[!0-9]*) BASE=0 ;; esac
  echo "baseline HIGH: $BASE"
  if [ "$HIGH" -gt "$BASE" ]; then
    echo "::error::HIGH findings rose from $BASE to $HIGH. The ratchet only moves down."
    echo "::error::Fix the new finding, or if it is legitimate update $BASELINE_FILE deliberately."
    CRIT=$((CRIT+1))
  elif [ "$HIGH" -lt "$BASE" ]; then
    echo "::notice::HIGH findings fell from $BASE to $HIGH. Tighten the baseline: high=$HIGH"
  fi
fi

if [ "$CRIT" -gt 0 ]; then
  echo "::error::PHI/DUA scan FAILED ($MODE). A public repository must not carry this."
  exit 1
fi
echo "PASS ($MODE)."
