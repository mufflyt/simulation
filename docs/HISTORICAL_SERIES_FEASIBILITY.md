# Can we build a historical active-workforce series that is not survivor-filtered?

**Verdict: no — not from anything in this repository, and the blocker is not
the activity data. It is that every URPS-identifying source available here is a
CURRENT-STATUS directory.**

The survivor-filter bias is nonetheless quantifiable from this repo's own vetted
tables, and it is large enough to change how the back-test should be read. That
quantification is section 3; nothing was recalibrated and no target moved.

> **UPDATE — survivor conditioning is now DIRECTLY OBSERVED, not inferred.**
> Section 3's constant-hazard calculation was the best available evidence when
> this document was written. It has since been superseded: linking the excluded
> physicians to Medicare Part B shows them billing, by name and year, with no
> hazard assumed. See section 9. Section 3 is retained as a sensitivity
> analysis and should no longer be cited as the primary evidence.

---

## 1. Source inventory — what actually exists here

Reported from `config/canonical_sources.yml`, `data-raw/`, and
`scripts/data_acquisition/`. Availability is not assumed anywhere.

| Source | Present here? | Identifies URPS? | Longitudinal? | Verdict |
|---|---|---|---|---|
| `mufflyaccess` contract counts | Yes | Yes | 2013–2023, but **back-projected from the 2025 roster** | The survivor-filtered series itself |
| `fpmrs_baseline_roster` (`data/fpmrs_baseline_roster.csv`) | **No** — registered, file absent | — | — | Registry entry with an empty SHA-256 |
| Sanitized URPS roster (`data-raw/urps_roster/`) | Provenance only; **CSV is gitignored** | Yes | Carries `last_confirmed_active_year`, `urogyn_services_through_2023` | 2025 survivors only — cannot recover the missing |
| Sensitivity tables (`data-raw/sensitivity/`, 13 files) | Yes | Aggregate | Derived summaries, incl. an observed 2016–2021 departure window | **Enough to bound the bias; not to rebuild the series** |
| NRMP series + track split | Yes | Entry only | 2010–2025 | Training inflow, not workforce stock |
| ACGME fellow series | Yes | Entry only | 2015–2024 | Training inflow, not workforce stock |
| Medicare Part B/D, PECOS, Open Payments | **No** | No (provider type only, not subspecialty) | Yes, upstream | Used by the `cliff` pipeline; only derived outputs travel here |
| NPPES | **No** | No | Yes | Not fetched by any script in this repo |

There is **no annual, provider-level, URPS-identified file in this repository**.
`scripts/data_acquisition/` fetches BRFSS, ACS, MCBS, NHAMCS/NAMCS, MEPS,
NHANES, SWAN, HCUP, NRMP and ACGME — population and training sources. None
enumerates the URPS workforce by historical year.

## 2. The three estimands

| | Numerator | Exclusions | Time reference | Identifiable from a source we have? |
|---|---|---|---|---|
| **Ever certified** | Everyone who ever passed ABOG or ABU URPS/FPMRS | None | Cumulative to date | **No.** Needs board registries. ABOG/ABU's Oct-2023 "~1,700" is the only glimpse, and it is a press statement, not data |
| **Roster-observable stock** | NPI-identified, deduplicated, non-retired providers with URPS cert year ≤ Y | Retired, unmatched, dual-boarded urology-primary; ABU only if net-new to the ABOG NPI set | **The 2025 snapshot, back-projected** | **Yes** — this is the 1,306 series, and it is the only one we have |
| **Active clinical workforce** | Providers delivering URPS care in year Y | Retired, deceased, non-practising, out-of-scope practice | Contemporaneous with Y | **No.** Needs annual activity linked to a historical certification list |

The second is what the contract serves. The third is what the supply
microsimulation is meant to represent. They are not the same quantity and the
contract cannot distinguish them, because `n_active == n_ever_certified` in
every row.

## 3. Magnitude of the survivor-filter bias — a SENSITIVITY ANALYSIS

**These numbers are not observations, and they are not estimates of truth
independent of the hazard assumption.** Under a constant annual departure hazard
`d`, they state what the historical stock would have been *before* conditioning
on survival to the 2025 roster. Nothing here recovers the historical workforce;
a different hazard, or a non-constant one, gives different numbers.

If a provider present in year `Y` departed before the 2025 snapshot, they are
absent from year `Y`. Writing `d` for the assumed constant annual hazard:

```
observed_S(Y) = adjusted_S(Y) * (1 - d)^(2025 - Y)
adjusted_S(Y) = observed_S(Y) / (1 - d)^(2025 - Y)
```

Both bounds are published cells in this repo's own vetted tables — no rate is
invented, and neither is fitted here:

- **Lower, d = 0.0050/yr** — `data-raw/sensitivity/departure_rate_sensitivity.csv`,
  row `subspecialty_abbrev == "URPS"`, column `rate_min` (the directly observed
  rate, before any mortality adjustment).
- **Upper, d = 0.0151/yr** — `data-raw/sensitivity/mortality_sensitivity.csv`,
  row `subspecialty_abbrev == "URPS"`, column `rate_adj_all_pct` (adjusted for
  all missed deaths).

Both tables are `cliff`-pipeline outputs registered in
`config/canonical_sources.yml` and reached through the checksum-verified
resolver.

| Year | Observed | Implied stock, d = 0.0050 | Implied stock, d = 0.0151 | Difference (low) | Difference (high) |
|---:|---:|---:|---:|---:|---:|
| 2013 | 655 | 696 | 786 | 41 | **131** |
| 2015 | 932 | 980 | 1,085 | 48 | 153 |
| 2018 | 1,041 | 1,078 | 1,158 | 37 | 117 |
| 2020 | 1,099 | 1,127 | 1,186 | 28 | 87 |
| 2023 | 1,306 | 1,319 | 1,346 | 13 | 40 |

"Implied stock" is the departure-adjusted quantity `adjusted_S(Y)`, not a
measurement. The **observed series is unchanged** and remains the only series
this project has.

**The implied bias is monotone in look-back depth**, exactly as the mechanism
predicts: largest in 2013–2015, smallest at the snapshot.

### What that does to the back-test

| | Growth 2020→2023 | Per year |
|---|---:|---:|
| Observed (survivor-filtered) | **+207** | 69.0 |
| Implied, d = 0.0050 | +192 | 64.1 |
| Implied, d = 0.0151 | **+160** | 53.5 |

**Under these hazard assumptions** the growth the model was scored against is
inflated by 15–47 providers, or 7–22%, because early years are depressed more
than late ones. That is a sensitivity result, not a measured correction.

### Two mechanisms, only one of which is quantified

- **Retirement/death before 2025** is depth-dependent, and the table above
  bounds it. This is the confound the formula addresses.
- **Identity failure (no NPI match, never in the source directories)** affects
  the level for certain. Whether it also distorts growth is **UNIDENTIFIED from
  this repository**, and it must not be called harmless.

An earlier draft of this document claimed identity failure was time-invariant
and therefore harmless for growth. That does not follow. A fixed set of missing
*people* is not a fixed missing *count in every year*: someone certified in 2019
contributes nothing to 2013 and contributes to 2020-2023. Identity exclusions
distort growth unless their certification years are distributed like the
included population, which nothing here establishes.

What the provenance actually gives about excluded providers:

| Exclusion | Count known? | Certification years known? |
|---|---|---|
| ABU records without an NPI match | Yes - 24 (`270 of 294`) | **No** |
| "retired/unmatched dropped" (ABOG) | **No count published** | **No** |
| "dual-boarded urology-primary held out" | **No count published** | **No** |
| Reinstated dual-boarded, kept | Yes - n=38 | No |
| Cert year > 2023, excluded from 2023 | Yes - 33 | Implicitly > 2023 |

So the only excluded group with a published count is the 24 unmatched ABU
records, and even they carry no certification year. **The growth effect of
identity exclusion is unidentified and is reported as such.**

## 4. Could NPPES or Medicare recover the missing people?

**In principle yes; from this repository, no.** And two distinctions must not be
blurred:

- **"Has an NPI" ≠ "clinically active."** NPPES enumeration is close to
  permanent and deactivation is inconsistently filed, so NPPES presence is an
  identity fact, not an activity fact.
- **"Billed Medicare" ≠ "active workforce."** Medicare fee-for-service misses
  Medicare Advantage, commercial, Medicaid and uninsured care entirely — the
  same estimand caveat `aggregate_medicare_realized_care()` already carries.

The blocking problem is **identification, not activity**. Annual Medicare files
tell you a provider billed in year Y; they carry `Rndrng_Prvdr_Type`
("Obstetrics & Gynecology"), never a subspecialty. To know a 2015 biller was
URPS you need a certification list **as it stood in 2015** — and the ABOG Verify
directory and ABU portal, which are the contract's sources, are *current-status*
directories. Someone who retired in 2018 may simply not appear in either today.

So the missing population is unrecoverable from current directories by
construction. What would work, none of which is here:

1. ABOG/ABU diplomate lists with certification dates **and status history** (the
   ~1,700 population).
2. Archived snapshots of the ABOG Verify directory.
3. The `cliff` pipeline's intermediate certification x Medicare/PECOS linkage --
   which demonstrably exists upstream, since this repo carries its *outputs*
   (`departure_window_sensitivity.csv` reports a "fully_obs" 2016–2021 window,
   and `departure_rate_sensitivity.csv` a `rate_directly_observed`), but not its
   inputs.

Option 3 is the cheapest real path: the linkage has already been built once.

## 5. What the back-test may now claim

Unchanged: the target (1,306), every entrant rate, and every conversion.

Changed: the claim. The back-test scores a model initialised on a
survivor-filtered 2020 stock against a survivor-filtered 2023 target, where the
2020 side carries five years of filtering and the 2023 side two. It therefore
validates **reproduction of the roster-construction process**, not reconstruction
of the historical active workforce.

`docs/BACKTEST_2020_TO_2023.md` currently says the observed series applies "no
attrition" and that the model will "structurally under-predict" against it.
Both need correcting: the series applies attrition **retroactively and
uniformly at 2025**, and the resulting bias runs the other way for the growth
the model is scored on.

## 6. Does Arm 5 remain the best arm? — demonstrated, not asserted

**No. Arm 5 was never the best arm, and the ranking is not robust.**

Two corrections, both of which reverse an earlier claim in this document.

**Arm 5 does not hold the accuracy crown.** `b0a3d61` had already established
that and this document repeated the stale claim without checking the frozen
record. **Arm 1 (no-attrition) is the most accurate at −3.14%**; Arm 5 is third
at −4.36%.

**Scenario A — correct the target only.** Ranking by absolute error across the
no-attrition family:

| `d` | 2023 target | Ordering |
|---|---:|---|
| 0 (observed) | 1306.0 | 1 < 3 < 5 < 2 = 4 |
| 0.0050 | 1319.2 | 1 < 3 < 5 < 2 = 4 |
| 0.0110 | 1335.2 | 1 < 3 < 5 < 2 = 4 |
| 0.0151 | 1346.4 | 1 < 3 < 5 < 2 = 4 |

Stable — **but only because every prediction stays below every corrected
target**, a condition that is now asserted in a test rather than assumed. Note
also that the correction *raises* the target, so every arm's error gets
**worse**, not better. The earlier claim that "every arm's reported error is
overstated" had the sign backwards, conflating a growth bias with a level bias.

**Scenario B — correct both endpoints.** The model is initialised on the 2020
stock, which carries five years of filtering against the target's two (+87 vs
+40 at high `d`), so correcting only the target is not the honest comparison:

| `d` | Ordering (signed error) |
|---|---|
| 0 | 1(−41) 3(−42) 5(−57) 2(−108) 4(−108) |
| 0.0050 | 1(−26) 3(−27) 5(−42) 2(−93) 4(−93) |
| 0.0151 | **3(+5) 1(+6)** 5(−10) 2(−61) 4(−61) |

**The ranking crosses.** At the high hazard the signs flip positive —
predictions cross the target — and Arms 1 and 3 swap. So the ordering is robust
under target-only correction and **not** robust once both endpoints are
corrected. Arm 5 is third throughout and never best.

Arm 5's apparent accuracy is also window-sensitive: `b0a3d61` found −2.53% on
2017–2020 versus −4.36% on 2010–2020. An arm whose error moves 1.8 points on a
window choice, scored against a target whose correction can reorder the field,
should not be described as accurate at all.

---

## Reproducing

```r
sens <- function(n) read.csv(sprintf("data-raw/sensitivity/%s_sensitivity.csv", n))
sens("departure_rate")[sens("departure_rate")$subspecialty_abbrev == "URPS", ]
sens("mortality")[sens("mortality")$subspecialty_abbrev == "URPS", ]
# observed_S(Y) = adjusted_S(Y) * (1 - d)^(2025 - Y)
```


## 7. The cheapest route to a real validation series -- traced

This is not a lost artifact. **`build_urps_exit_hazard()`
(`R/supply-retirement_hazard.R:146`) already reads it**, and falls back to an
HWSM Weibull analogy when the path is unset -- which it currently is.

| | |
|---|---|
| **Artifact** | a cliff DuckDB; the function accepts `cliff_duckdb_path` |
| **Table** | first of `physician_retirement_signals`, `retirement_signals`, `cliff_results` |
| **Columns used** | `retirement_confidence_score` / `confidence_score`, filtered at `min_confidence` |
| **Creating pipeline** | `mufflyt/cliff` (also named at `R/supply-roster_capacity.R:45` as where the roster "is derived") |
| **Source inputs** | ABOG/ABU/ABMS certification x Medicare Part B/D, CMS Open Payments, PECOS |
| **Still exists?** | **Unverifiable from here.** The path is a parameter, currently unset; no copy is in this repo |
| **Related port** | `R/supply-partial_pooling_hazard.R` is a port of `cliff/scripts/hierarchical_hazard_partial_pooling.R`, so cliff script paths are known |

**Would recovering it give an independent annual active-workforce series?**
Partly, and the answer turns on one question. The table holds retirement
*events* with confidence scores -- exit timing. An annual active count for
2016-2021 needs entry as well as exit:

- if the DuckDB carries **entry (certification/first-activity) dates as well as
  exit**, then annual active counts may be constructible for that interval,
  which would make it a candidate for independent longitudinal validation;
- if it carries **only retirement signals**, it corrects the departure side of
  the existing series but cannot recover providers who were never in the 2025
  roster, so the survivor filter survives.

WHAT `fully_obs` DOES AND DOES NOT ESTABLISH. The
`departure_window_sensitivity.csv` row `2016-2021 / fully_obs / URPS / 0.78`
establishes that the interval was fully observed **for the departure-hazard
calculation**. It does NOT establish that the annual active URPS workforce stock
is observed over that interval -- observing exits requires knowing who was
present and when they left, not necessarily when everyone entered. 2016-2021 is
therefore a **promising interval for constructing independent longitudinal
validation**, conditional on the DuckDB carrying enough provider-level entry and
identity information. It is not the primary validation set, and must not be
described as one until that artifact is inspected.

**Recommended next step, not taken here:** obtain the cliff DuckDB and inspect
its schema for entry dates. That is one file and one query, against
reconstructing a workforce from raw CMS data.

## 8. Reproducing the bias bounds exactly

```r
sens <- function(n) read.csv(sprintf("data-raw/sensitivity/%s_sensitivity.csv", n))
d_lo <- sens("departure_rate")[sens("departure_rate")$subspecialty_abbrev == "URPS", "rate_min"] / 100
d_hi <- sens("mortality")[sens("mortality")$subspecialty_abbrev == "URPS", "rate_adj_all_pct"] / 100
# d_lo = 0.0050, d_hi = 0.0151

SNAP <- 2025
x <- as.data.frame(mufflyaccess::urps_counts_long())
n <- x[x$geography == "national" & x$board_pathway == "ABOG_PLUS_ABU" &
         x$measure == "board_certified_active", c("year", "n_active")]

# Departure-adjusted stock under an ASSUMED constant hazard d. Not a measurement.
adjusted_S <- function(obs, y, d) obs / (1 - d)^(SNAP - y)
difference <- function(obs, y, d) adjusted_S(obs, y, d) - obs
```

`adjusted_S(1099, 2020, 0.0151) = 1186`; difference = 87.
`adjusted_S(1306, 2023, 0.0151) = 1346`; difference = 40.

Both are implied values under the stated hazard, not observations.

---

## 9. Direct falsification supersedes the hazard sensitivity

Section 3 asks what the historical stock *would have been* under an assumed
departure hazard. That question is no longer necessary for establishing that
survivor conditioning is present, because the conditioning can be observed
directly.

The 2025 roster adjudication excludes 161 of the 1,500 identified
urogynecologists. 155 of them carry a usable NPI and can be linked to Medicare
Part B. They were not gone:

| Evidence | Window | n | of 155 |
|---|---|---:|---:|
| Any Part B billing | 2013–2023 | 129 | 83.2% |
| Any Part B billing | 2016–2021 | 121 | 78.1% |
| **Part B billing in ALL SIX validation years** | 2016–2021 | **69** | **44.5%** |
| No Part B billing anywhere | 2013–2023 | 26 | 16.8% |

**69 physicians billed Medicare in every year of the validation window — 414
directly observed provider-years that the retrospective series records as
zero.** No hazard, no back-projection, no modelling assumption: three facts per
physician (URPS-identified, excluded from the later roster, observed billing).

Of the 26 with no Part B billing, 9 nonetheless hold a sustained Medicare
clinician-directory listing. That is enrolment, not billed care, and it is
reported as a separate weaker tier — it is never added to the Part B counts, and
the directory covers only 2018 onward, so it is silent on 2016–2017.

**What this changes for the methodological conclusion.** The 2013–2023
`board_certified_active` series must no longer be described as an independently
observed historical workforce series. It is a 2025 roster back-projected by
certification year, and at least 69 physicians it reports as absent were
demonstrably practising. It remains a legitimate target for *reproducing the
roster-construction process*; it is not a measurement of the historical active
workforce, and the two must not be swapped in a sentence.

Counts are generated from `inst/extdata/survivor_falsification.json` (built by
`scripts/data_acquisition/09_build_survivor_falsification.R`), frozen by
`tests/testthat/test-validation-survivor.R`, and rendered by
`scripts/plot_survivor_conditioning.R`. They appear as literals in the test file
and nowhere else.
