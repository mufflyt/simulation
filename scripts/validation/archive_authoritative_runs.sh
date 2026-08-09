#!/bin/bash
# Durable archive of authoritative validation runs ----
#
#   bash scripts/validation/archive_authoritative_runs.sh [outdir]
#
# WHY. artifacts/validation/ is gitignored and local. Reproducibility means the
# evidence CAN be regenerated; provenance benefits from preserving what actually
# produced the manuscript numbers. Generated CSVs do not belong on main, so this
# builds an immutable bundle instead -- manifest, COMPLETED marker, input
# hashes, result tables, plus a SHA-256 of every file in the bundle so the
# archive can itself be verified later.
#
# EXPLORATORY RUNS ARE EXCLUDED BY CONSTRUCTION. A bundle is manuscript
# evidence; an exploratory run is not, and must never end up inside one.
set -euo pipefail
OUT="${1:-artifacts/validation_archive}"
STAMP=$(date +%Y%m%dT%H%M%S)
BUNDLE="$OUT/validation_bundle_$STAMP"
mkdir -p "$BUNDLE"

n=0
for d in artifacts/validation/*/; do
  id=$(basename "$d")
  case "$id" in *EXPLORATORY*) continue;; esac
  [ -f "$d/COMPLETED" ] || continue
  [ -f "$d/FAILED" ] && continue
  cp -R "$d" "$BUNDLE/$id"
  n=$((n+1))
done

# Hash every archived file so the bundle is self-verifying.
( cd "$BUNDLE" && find . -type f ! -name SHA256SUMS -exec shasum -a 256 {} \; \
    | sort -k2 > SHA256SUMS )

cat > "$BUNDLE/README.txt" <<EOF
Authoritative validation runs, archived $STAMP.

$n run(s). Exploratory runs are excluded by construction.

Each directory is one validation run and contains:
  MANIFEST.txt  identity written BEFORE computation -- model_sha,
                validation_sha, contract_sha, validation code digest, input
                SHA-256s, parameters, seeds, environment, clean-state assertion
  COMPLETED     finish time and input hashes RECHECKED at completion; absent
                if the run did not finish, so a partial run cannot masquerade
                as evidence
  *.csv         result tables

SHA256SUMS covers every file above; verify with:
  cd <bundle> && shasum -a 256 -c SHA256SUMS

Cite a result by run_id, not by git SHA. A commit identifies code; only the
manifest identifies the computation -- code, data, parameters and environment
together.
EOF

echo "bundle: $BUNDLE"
echo "runs archived: $n"
tar -czf "$BUNDLE.tar.gz" -C "$OUT" "$(basename "$BUNDLE")"
echo "tarball: $BUNDLE.tar.gz ($(du -h "$BUNDLE.tar.gz" | cut -f1))"
