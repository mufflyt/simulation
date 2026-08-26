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
# Runtime output directories that several R/data-chia_*.R / R/geography-chia_*.R
# generators write real (non-synthetic) small-cell discharge/facility counts
# into. These directories should never contain a tracked file at all -- the
# generators' own save_dir defaults live under artifacts/ or
# tests/testthat/artifacts/, both meant to be gitignored, so anything tracked
# here means either a `git add -f` bypass or pre-existing exposure. Found by
# discovering tests/testthat/artifacts/chia_inpatient/*.csv and
# tests/testthat/artifacts/chia_capacity/*.csv already tracked and pushed:
# small-cell aggregate counts (e.g. a single-digit case count for one
# year/age-band/procedure_family stratum) derived from real DUA-restricted
# CHIA discharge data, indistinguishable from synthetic fixture output by
# content alone because the synthetic generators intentionally mimic the same
# category vocabulary. Only build_chia_inpatient_urps_series() and
# build_chia_hospital_surgical_volume_map() currently mark synthetic output
# with a `synthetic_` filename prefix (redirected to tempdir(), never
# reaching a tracked path); build_chia_ub04_setting_evidence(),
# build_chia_surgical_travel_kernel(), and validate_chia_inpatient_demand()
# have no such distinction at all and always write to the tracked-risk path
# regardless of whether the input was real or synthetic. A filename check
# for "real vs synthetic" is therefore not reliable across all five
# generators; treat ANY tracked file under these directories as a finding.
DUA_ARTIFACT_DIRS='^(artifacts|tests/testthat/artifacts)/chia_(capacity|inpatient|revenue_setting|travel|validation)/'
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

  # CRITICAL 1b: anything tracked under a CHIA runtime-output directory. See
  # the DUA_ARTIFACT_DIRS comment above -- content alone cannot reliably
  # distinguish real from synthetic output here, so any tracked file in these
  # directories is a finding UNLESS individually allowlisted (see
  # chia-artifact-allowlist.txt's own header for what "verified" means here).
  CHIA_ALLOW=".github/chia-artifact-allowlist.txt"
  chia_allowed() {
    [ -f "$CHIA_ALLOW" ] || return 1
    grep -qxF "$1" "$CHIA_ALLOW"
  }
  while IFS= read -r f; do
    [ -n "$f" ] || continue
    if chia_allowed "$f"; then
      printf '::notice::[allowlisted] verified-synthetic CHIA artifact: %s -- see %s\n' "$f" "$CHIA_ALLOW"
      continue
    fi
    crit "tracked file under a CHIA runtime-output directory, not on the verified allowlist" "$f"
  done < <(git ls-files | grep -E "$DUA_ARTIFACT_DIRS" || true)

  # CRITICAL 2: PHI column names in tracked content
  while IFS= read -r m; do
    [ -n "$m" ] && crit "PHI column name in tracked content" "${m:0:200}"
  done < <(git grep -InE "$PHI_COLS" -- ':!*.Rd' ':!docs/*' \
             ':!.github/scripts/*' ':!.github/phi-history-allowlist.txt' \
             ':!tests/testthat/test-phi-dua-guard.R' \
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

  # Same as the worktree DUA_ARTIFACT_DIRS check, but history: a file removed
  # from the tree still ships as a blob in a public clone. Deliberately still
  # flags the 480 files removed from the tree during the 2026-08-23
  # remediation -- their blobs remain fetchable from history until it is
  # rewritten, which this finding exists to make visible, not to silence.
  CHIA_ALLOW=".github/chia-artifact-allowlist.txt"
  chia_allowed() {
    [ -f "$CHIA_ALLOW" ] || return 1
    grep -qxF "$1" "$CHIA_ALLOW"
  }
  while IFS= read -r f; do
    [ -n "$f" ] || continue
    chia_allowed "$f" && continue
    crit "CHIA runtime-output file exists in git history, not on the verified allowlist" "$f"
  done < <(git log --all --pretty=format: --name-only --diff-filter=A 2>/dev/null \
             | sort -u | grep -E "$DUA_ARTIFACT_DIRS" || true)

  ALLOW=".github/phi-history-allowlist.txt"
  allowed() {
    [ -f "$ALLOW" ] || return 1
    grep -qE "^[[:space:]]*$1[[:space:]]*$" "$ALLOW"
  }
  for needle in PatientName PAT_ADDR_1 MedicalRecordNum; do
    while IFS= read -r m; do
      [ -n "$m" ] || continue
      sha=${m%% *}
      if allowed "$sha"; then
        printf '::notice::[allowlisted] synthetic PHI-pattern match in %s -- see %s\n' "$sha" "$ALLOW"
        continue
      fi
      crit "PHI identifier introduced in history ($needle)" "$m"
    # tformat: NOT format: -- `format:` omits the trailing newline, so
    # `while read` silently DROPS THE LAST LINE. With a single matching commit
    # (the usual case) the loop body never ran at all and the scan reported
    # clean. A leak scanner that misses its only finding is worse than none.
    # PATH EXCLUSIONS, mirroring the worktree scan. Without them every commit
    # that TOUCHES the allowlist or the scanner -- both of which necessarily
    # contain PHI patterns as data -- becomes a new pickaxe hit needing its own
    # allowlist entry. That is a treadmill, not a gate: each fix generates the
    # next finding.
    done < <(git log --all -S"$needle" --pretty=tformat:'%h %ad %s' --date=short \
               -- ':!.github/phi-history-allowlist.txt' ':!.github/scripts/*' \
               2>/dev/null | head -5 || true)
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
