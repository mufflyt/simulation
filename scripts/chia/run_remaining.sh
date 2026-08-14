#!/usr/bin/env bash
# Everything that still has to go into chia_cadr.duckdb, in order.
#
# Strictly sequential: DuckDB allows one writer per database file, so these
# cannot be parallelised. Each loader is restartable -- already-loaded tables are
# skipped via meta._load_manifest -- so re-running this script after an
# interruption picks up where it stopped.
set -euo pipefail

BUILD=/Volumes/MufflySamsung/chia_cadr_build
DB=/Volumes/MufflySamsung/DuckDB/chia_cadr.duckdb
STAGING="$HOME/ma_casemix_staging"
PULL=/Volumes/MufflySamsung/chia_dropbox_pull
CADR=/Volumes/MufflySamsung/cadr_extracted

cd "$BUILD"

echo "=== 1/6 CHIA Access files from the Dropbox pull (2015-2018 + dedupe) ==="
python3 -u load_chia_mdb.py --src "$PULL" --db "$DB" --schema chia_casemix

echo "=== 2/6 CHIA provider rosters (staging) ==="
python3 -u load_tabular.py --src "$STAGING" --db "$DB" --schema chia_provider \
        --match '(BORIM|LegHashPhysician|GOBA)'

echo "=== 3/6 CHIA reference / crosswalk tables (staging) ==="
python3 -u load_tabular.py --src "$STAGING" --db "$DB" --schema chia_ref \
        --match '(Race|V27LONG|cpt codes|Cadish_ideal)'

echo "=== 4/6 CHIA project files from the Dropbox pull (CSV + XLSX) ==="
python3 -u load_tabular.py --src "$PULL" --db "$DB" --schema chia_project

echo "=== 5/6 CADR Medicare claims cohort ==="
python3 -u load_tabular.py --src "$CADR" --db "$DB" --schema cadr --strip data

echo "=== 6/6 finalize: rehash, year-union views, table catalog ==="
python3 -u finalize_db.py --db "$DB" --rehash

echo "=== ALL DONE ==="
