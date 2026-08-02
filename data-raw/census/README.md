# Census 2023 National Population Projections (NPP)

Real demand-driver inputs for the URPS workforce microsimulation.

## Files

| File | Series | SHA-256 (first 12) |
|---|---|---|
| `np2023_d1_mid.csv` | Main / middle series (primary) | `153282275 1d0` |
| `np2023_d1_low.csv` | Low-immigration series | `7b780edb10a0` |
| `np2023_d1_hi.csv`  | High-immigration series | `d3ff9d25cc0f` |

Each is the single-year-of-age detail file: columns `SEX, ORIGIN, RACE, YEAR,
TOTAL_POP, POP_0 … POP_100` (106 columns), years 2022–2100.

## Source

U.S. Census Bureau, **2023 National Population Projections**, Main projections
series — <https://www.census.gov/programs-surveys/popproj.html>. These copies
were carried over from the vetted `cliff` repository
(`cliff/data/census/`), which uses the identical file for its demand
denominator.

## How it is consumed

Registered in `config/canonical_sources.yml` as
`census_npp_female_{mid,low,hi}` (path + SHA-256). Loaded through the canonical
resolver — never by a hardcoded path:

```r
# Female population by demand age band (20-39 / 40-59 / 60-64 / 65-79 / 80+):
npp_female_by_band("mid", years = 2025:2050)

# Women 65+ (crude single-denominator path):
npp_women_65plus("mid", years = 2025:2050)
```

The SSOT female filter is `SEX == 2 & ORIGIN == 0 & RACE == 0`
(all origins, all races) — see `R/13-demand_urps.R::npp_total_female()`. A wrong
`ORIGIN`/`RACE` code silently narrows the denominator to a subgroup, so it is
guarded, not inlined.

## Refreshing the checksums

If a file is replaced, recompute and update `config/canonical_sources.yml`:

```bash
sha256sum data-raw/census/np2023_d1_*.csv
```

Under `REPRODUCIBILITY_MODE=strict` a checksum mismatch stops the run; in
relaxed mode it warns.

## Build note

`data-raw/` is `.Rbuildignore`d, so these ~2.9 MB files ship with the git
repository (for source-checkout runs and provenance) but are excluded from the
built/installed package. Installed-package users get the example population
fallback unless they point `pop_by_band` at their own series.
