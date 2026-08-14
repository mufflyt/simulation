# CHIA Case Mix — Massachusetts all-payer hospital discharge data

Regional external validation source for inpatient surgical utilisation and
surgical travel. **Not** a supply input and **not** a total-volume denominator —
see `docs/CHIA_TECHNICAL_APPENDIX.md` for why.

## Acquire (licensed — no free API)

CHIA Case Mix requires an approved Data Use Agreement with the Massachusetts
Center for Health Information and Analysis: <https://www.chiamass.gov/case-mix-data>.
Files arrive as Microsoft Access `.mdb` databases, one per fiscal year and
database. Documentation guides (HIDD, OOD) are published per fiscal year and are
needed to decode payer, admission-source, ethnicity and condition-present fields.

Regulation: **957 CMR 8.00** (APCD and Case Mix Data Submission), which binds
*acute care hospitals only* — freestanding ambulatory surgery centres never
submit.

## Build

The database is ~4.5 GB and lives outside this repo (external drive). Rebuild:

```sh
python3 scripts/chia/load_chia_mdb.py     --db /path/to/chia_cadr.duckdb --src /path/to/mdb
python3 scripts/chia/load_reference.py    --db /path/to/chia_cadr.duckdb
python3 scripts/chia/finalize_db.py       --db /path/to/chia_cadr.duckdb
python3 scripts/chia/test_chia.py         --db /path/to/chia_cadr.duckdb   # 71 checks, 63 gates
```

`test_chia.py` exits non-zero on any gate failure and must pass before any CHIA
number is used. The gates encode every repair described in the appendix —
schema drift across 15 fiscal years, the operative-procedure classification, the
newborn-attribution leak, the `-` sentinel, and the physician reporting cliffs.

## Derived artefacts tracked here

| File | Contents |
|---|---|
| `travel_distance_bands.csv` | **measured** share of operations by mile band |
| `travel_distance_quantiles.csv` | **measured** distance quantiles, miles |
| `travel_drivetime_speed_sensitivity.csv` | drive-time bands across 30–50 mph — shows how much the speed assumption moves the answer |
| `travel_bands_overall.csv` | drive-time bands, 40 mph central case |
| `travel_bands_by_age.csv` | the same, by age band |
| `travel_bands_by_year.csv` | the same, by fiscal year |
| `travel_kernel_vs_luoqi.csv` | observed vs available vs Luo/Qi weights |

Distance is measured; drive time is not. There is no routing engine in this
pipeline — see appendix §5.2 before using any minute-based figure.

Regenerate with `scripts/chia/build_chia_surgical_travel_kernel.R`.
