# Workforce sensitivity-analysis datasets

Vetted sensitivity tables carried over from the `cliff` repository (`cliff/data/`,
sourced from `main`). Each is registered in `config/canonical_sources.yml`
(path + SHA-256) and reached through the resolver, never a hardcoded path:

```r
sensitivity_registry()                       # available logical names
load_sensitivity_table("departure_rate")     # one table, checksum-verified
```

| Logical name | File | What it varies |
|---|---|---|
| `abu_pathway` | `abu_pathway_sensitivity.csv` | ABU (urology-pathway) inclusion |
| `age_shift` | `age_shift_sensitivity.csv` | retirement age ± shift |
| `consistent_definition_baseline` | `consistent_definition_baseline_sensitivity.csv` | active-definition baseline |
| `departure_rate` | `departure_rate_sensitivity.csv` | ± retirement/departure rate |
| `departure_window` | `departure_window_sensitivity.csv` | observation-window bounds |
| `feminization` | `feminization_sensitivity.csv` | sex-composition trend |
| `inactivity_threshold` | `inactivity_threshold_sensitivity.csv` | inactivity claim threshold |
| `mortality` | `mortality_sensitivity.csv` | mortality adjustment |
| `open_payments` | `open_payments_sensitivity.csv` | Open Payments signal on/off |
| `retirement` | `retirement_sensitivity.csv` | retirement threshold ± band |
| `grid` | `sensitivity_grid.csv` | full multi-way sensitivity grid |
| `grid_summary` | `sensitivity_grid_summary.csv` | grid summary |
| `demand_denominators` | `urps_demand_denominators_sensitivity.csv` | D1/D2/D3 demand denominator |

## Source & provenance

U.S. board / claims-derived workforce analyses, produced by the `cliff`
workforce-projection pipeline (ABOG/ABU/ABMS certification, Medicare Part B/D,
CMS Open Payments, PECOS). Refresh checksums after replacing any file:

```bash
sha256sum data-raw/sensitivity/*.csv
```

Under `REPRODUCIBILITY_MODE=strict` a checksum mismatch stops the run; relaxed warns.

## Build note

`data-raw/` is `.Rbuildignore`d, so these files travel with the git repo (for
source-checkout runs and provenance) but are excluded from the built package.
