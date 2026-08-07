# Scientific benchmark datasets

These are the small, version-controlled reference values the model must
reproduce on **every commit**. They are regression tests for **scientific
correctness**, not just software correctness: if a change moves any of them, CI
fails and the diff names the quantity that drifted and the published source it
drifted away from.

All of them run in CI as a **hard gate** — each value comes from bundled data or
a pure function, so there is no external file to fetch and no `mufflyaccess`
dependency, and none of the assertions skip.

**Guard:** [`tests/testthat/test-scientific-benchmarks.R`](../tests/testthat/test-scientific-benchmarks.R)

| # | Benchmark | Value | Source | Accessor |
|---|-----------|-------|--------|----------|
| 1 | Physician retirement survival, of 100 active at 50 | S(60)=0.80, S(65)=0.55, S(70)=0.30, S(75)=0.12, S(80)=0.03 (charted); exact curve 0.796 / 0.545 / 0.296 / 0.117 / 0.030 | HWSM v5.19.20 Exhibit 17 + Fraher & Knapton FutureDocs | `retirement_survival(50, ..., sex = "male")` |
| 2 | Provider capacity-survey base-year adequacy | adequacy 0.948, gap 0.052 | Zarek 2025 PTJ (published four-category example) | `capacity_survey_adequacy(example_capacity_survey())` |
| 3 | Published Dall-family base-year shortfalls | Dall 2021 physiatry 940 FTE / 10.6%; Zarek 2025 PT 12,070 / 5.2%; Dall 2013 neurology 1,814 / 11.0% | the three source studies | `published_baseline_gaps()` |
| 4 | Validated 2023 URPS workforce total | national ABOG+ABU 1,306; CONUS 1,303; ABOG-only 1,027 | mufflyaccess URPS contract v3.0.0 | `backtest_target_candidates()` |
| 5 | FutureDocs categorical participation | female expected FTE peaks 0.675 at age 50, collapses by 80; male ~flat; every row sums to 1 | Fraher & Knapton FutureDocs Fig 9 (digitised) | `participation_fte()`, `validate_participation_table()` |

## Why these five

Each pins a different load-bearing input:

- **#1 retirement survival** is the supply attrition curve. A physician curve is
  far longer than an allied-health one (see `RETIREMENT_HAZARD_ALLIED`); using
  the wrong one overstates attrition badly, so the anchors are locked.
- **#2 capacity survey** is the base-year adequacy method. The whole point of the
  base-year-gap module is that assuming equilibrium forces the starting shortfall
  to zero; reproducing Zarek's 94.8% proves the arithmetic is intact.
- **#3 published shortfalls** are the external comparators the model is judged
  against — the numbers a reviewer will check first.
- **#4 workforce total** is the single most leveraged number in the model: the
  base-year level every projected year is scaled from. 1,306 (not the roster
  snapshot 1,339, not the ABOG-only 1,027) is the validated target.
- **#5 participation** is the sex divergence in clinical FTE that grows in
  importance as the URPS workforce feminises.

## Running them

```bash
Rscript -e 'testthat::test_file("tests/testthat/test-scientific-benchmarks.R")'
```

## Adding a benchmark

When a new quantity reaches a decision or the manuscript, add a row here and an
assertion in the guard, pinned to a **named source** and reached through the
**canonical accessor** (never a hand-rolled re-derivation). Prefer a value that
runs without external data so it stays a hard CI gate; if it genuinely needs a
restricted file, `skip_if` on the file's presence and say so in the row.
