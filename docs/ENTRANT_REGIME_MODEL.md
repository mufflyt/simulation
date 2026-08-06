# The entrant regime model

`docs/BACKTEST_2020_TO_2023.md` records two separate failures. This module
addresses both, and the fix for one does nothing for the other.

| Failure | Cause | Fix |
|---|---|---|
| Point estimate −8.5% | A cancelled board examination averaged into the steady-state entrant rate | Screen disrupted years out of the fit; defer their deficit |
| 95% interval 6.5× too narrow | Every coefficient held fixed; intervals described Monte Carlo noise | Draw trend coefficients, overdispersion, and regime breaks per iteration |

Code: `R/supply-entrant_regime.R`. Tests: `tests/testthat/test-entrant-regime.R`.

**Read `R/validation-backtest_run.R` arm 5 first.** It attacks the same defect using the
NRMP fellowship match — a leading indicator of entry rather than a repaired
lagging one — and §5 scores this module against it rather than against the
superseded shipped assumption.

---

## 1. What the 2020 collapse actually was

The combined entrant series is not the whole story. `urps_entry_counts()` splits
it by board, and the split is decisive:

| Year | ABOG | ABU | Combined |
|---:|---:|---:|---:|
| 2018 | 34 | 6 | 40 |
| 2019 | 35 | 13 | 48 |
| **2020** | **3** | **7** | **10** |
| 2021 | 72 | 9 | 81 |
| 2022 | 42 | 12 | 54 |
| 2023 | 61 | 11 | 72 |

ABOG certifications fell 35 → 3 while ABU held at 13 → 7. A fellowship pipeline
that had genuinely contracted would have taken both down together. What happened
instead is that one board could not administer its examination.

That distinction is the whole model. **A cancelled examination does not destroy
candidates; they sit the next administration.** The deficit is *deferred*, not
lost. Note that ABOG 2020 + 2021 = 75, against 34 and 35 in the two preceding
years — the 2021 bulge is very close to the 2020 shortfall arriving late.

Averaging 40, 48, 10 to 32.7/yr treats a cancelled exam as evidence about the
size of the pipeline. It is not.

## 2. Three regimes

`classify_certification_regimes()` labels every year, using only years at or
before the cutoff:

- **`backlog`** — the initial certification of an already-practising pool
  (2013–2015). A *leading* year whose count exceeds twice the median of every
  later year. Backlog is a **prefix** regime: the run ends at the first year that
  fails the test and never resumes, because the initial pool is certified once.
- **`disrupted`** — a year below the lower tail of what the other non-backlog
  years predict for it. Screened **leave-one-out**, so the year under test
  contributes nothing to the baseline it is judged against, and against a
  **negative-binomial** tail at the fitted dispersion, so ordinary year-to-year
  variation does not trip it. At `screen_alpha = 0.01` the 2016–2020 window flags
  2020 and nothing else (10 observed against a lower bound of 35).
- **`release`** — a year *after* a disruption that exceeds the upper tail: the
  deferred candidates arriving. Excluded from the trend fit, and its excess is
  **credited against the outstanding deficit**.

That last rule fixes a double-count that a first draft of this module had. Fitted
through 2021, the model saw the 2021 bulge inflate the steady trend **and** still
scheduled the 2020 deficit as a future release — the same candidates twice. The
2021→2022 fold was +4.6% before the fix and +0.4% after.

## 3. What is drawn each iteration

`draw_entrant_paths()` layers four sources, each switchable so its contribution
is visible:

| Component | What it represents | 95% width of the 2023 cumulative count |
|---|---|---:|
| `dispersion` | NB count noise at the fitted dispersion | 62 |
| `+ trend` | Trend coefficients from `coef`/`vcov` via `.param_draw()` | 281 |
| `+ release_timing` | Deferred deficit clears over 1–2× `release_years` | 276 |
| `+ break` | Per-year Bernoulli break, deficit deferred | **305** |

For comparison, the shipped engine's width was **35**, and the addendum's
entrant-rate draw widened it to **131**. Neither covered.

**Coefficient uncertainty is the dominant term**, not the exotic one. Extrapolating
a log-linear trend three years from four points fans out considerably, and the
shipped engine omitted this entirely.

### The shape of a break

A break multiplies that year's entrants by a share drawn from a Beta prior and
**carries the remainder into the following year**. There is no mechanism that
multiplies a certification cohort *upward* except the release of an earlier
deferral, which the carry term already produces.

This matters more than it sounds. An earlier draft drew the break magnitude from
a symmetric lognormal fitted to `|log(observed / expected)|`. Because the single
observed break was a 5× *suppression*, the symmetric treatment implied 5×
*inflation* was equally likely, and the 95% band came out as **[1232, 3084]** — a
band wide enough to cover anything and therefore worth nothing.

The deferral form also produces the correct asymmetry: a break late in the
horizon pushes entrants *past the end of the window*, so the break term **lowers
the median as well as widening the band** (1,331 → 1,311 at the 2020 cutoff,
against a target of 1,306). A test pins this direction.

### Frequency

`break_probability` uses the Jeffreys-style `(k + 0.5) / (n + 1)` — one break in
five screened years gives 0.25/yr, not 0.20. A bare `k/n` would state 0.20 with
false precision and, in a window containing no break, would state that a break is
impossible.

## 4. The blind spot, stated rather than hidden

**A fitting window containing no disruption yields no break depth, so no break is
simulated.** `fit_entrant_regime_model()` says so on the console, and
`supply_parameter_spec()` reports `entrant_regime_break` as unquantified.

This is not hypothetical. In rolling-origin validation the fold fitted through
2019 has never seen a break, simulates none, and **misses the 2020 collapse** —
the one event it most needed to allow for. It is the only fold that misses.

The alternative would be to invent a depth from no data, which is exactly what
this package refuses to do for the retirement hazard and the hours schedule.
`break_surviving_share_prior` exists for a caller who wants to make that
assumption explicitly; it defaults to `NA`.

## 5. Scored at the 2020 cutoff, against arm 5

The comparator is **arm 5**, the NRMP fellowship-match arm in
`R/validation-backtest_run.R` — not the original shipped assumption, which arm 5 already
supersedes. 600 iterations, seed 20260802, target 1,306.

Definition-matched (no attrition on either side):

| Arm | Median | 95% range | % error | In 80% | In 95% | Calib. slope |
|---|---:|---|---:|:-:|:-:|---:|
| NRMP mean, fixed | 1,248 | 1,247–1,249 | −4.44% | ✗ | ✗ | 1.27 |
| NRMP mean + entrant draw (arm 5) | 1,249 | 1,231–1,265 | −4.36% | ✗ | ✗ | 1.26 |
| Regime model | **1,305** | 1,182–1,474 | **−0.08%** | ✓ | ✓ | **0.90** |

With attrition applied: NRMP −11.18% (slope 3.21, misses both bands), regime
model −6.28% (slope 1.50, covers both). The residual −6.28% is the attrition
mismatch of §7, not a modelling error.

### Why arm 5 and this module are complements, not rivals

Arm 5 reaches the same diagnosis from the other direction, and its reasoning is
the stronger one: NRMP counts fellows at **appointment** and publishes each
report in its own appointment year, so it is a *leading* indicator that was never
corrupted, where the certification series is a *lagging* one that has to be
repaired. Where a clean leading indicator exists, prefer it.

Two things this module still supplies that arm 5 does not:

1. **Regime-break uncertainty.** Arm 5 draws a scalar rate from the NRMP series'
   sampling distribution, which is why its interval is 34 wide and misses. It has
   no representation of a future disruption.
2. **The deferral mechanism**, which is what moves the point estimate: the 2020
   deficit is scheduled to return rather than being averaged away or ignored.

### Two objections this module has to answer, not dodge

**1. The screen is a cut made with the answer in hand.** Arm 5's rate fell from
58.0 to 49.73 when its series was extended to 2010–2020, because the mean now
spans the establishment ramp. Restricting it to the 2015+ plateau would score
better, and `b0a3d61` deliberately refused to do that: *"would be a choice made
with the answer in hand. It is not made here."*

That refusal is the right standard, and this module does not obviously meet it.
The leave-one-out screen, `screen_alpha = 0.01`, the two-year release window, and
the log-linear family are all choices made by someone who had read 2021–2023.
The screen is at least a general rule rather than a hand-picked year range, and
`classify_certification_regimes()` flags 2020 on a mechanism visible before the
cutoff — but that is a difference of degree, not a clean exemption. §6 is the
honest accounting.

**2. Coverage here tracks interval width, as it does everywhere else in this
back-test.** `b0a3d61` makes the point precisely: the two covering arms have
widths 145 and 138, while the four-times-sharper arm 5 misses. This module's
definition-matched interval is **292 wide** — the widest in the comparison — so
its coverage is partly bought, not purely earned.

What distinguishes it from "just wider" is that the point estimate moved too:
−0.08% against the best other arm's −3.14%. Width alone does not do that; the
deferral term does. But a reader should hold both facts at once, and should not
read ✓✓ in the coverage columns as vindication of the interval.

## 6. Honest status of these numbers

**The 2020 cutoff no longer tests this estimator.** It was written after the 2023
miss was examined, so the table above is a *refit*.
`run_entrant_regime_backtest()` stamps every row with `out_of_sample = FALSE` for
that reason, and `run_backtest()` is deliberately left alone so the frozen
prespecified record still means what it says.

`entrant_regime_rolling_validation()` refits at every admissible cutoff:

| Horizon | Folds | 95% coverage | Median abs. % error | Naive | Clears `assert_interval_coverage_publishable()` |
|---:|---:|---:|---:|---:|---|
| 3 | 2 | 2/2 | 1.36% | 5.15% | **No** — 2 folds, minimum is 3 |
| 2 | 3 | 3/3 | 0.54% | 2.55% | Yes |
| 1 | 4 | 3/4 | 1.23% | 2.29% | **No** — coverage ratio 1.27 > 1.25 ceiling |

The estimator beats the naive flat mean on point error at every horizon. But two
things must be said plainly about the coverage column:

1. **The horizon-2 pass is the weakest possible pass.** It is three folds from an
   eleven-year series, clearing a three-fold minimum with 3/3. One more miss at
   any cutoff would fail it.
2. **Rolling-origin controls for leakage into the *fit*, not for leakage into the
   *design*.** These folds hold out data from the model, but they cannot hold out
   the 2021–2023 series from the person who wrote the screening rule, the
   deferral mechanism, and the break form after reading it. No fold here is
   out-of-sample in the sense that matters most.

The horizon-1 failure is the informative one, and §4 explains it: the 2019 fold
has seen no break, so it simulates none, and misses 2020.

**A genuine test of this estimator requires a cutoff whose validation window
nobody has looked at yet** — the 2024 or 2025 count, projected from 2023. Until
then the published intervals stay gated, and `backtest_status()` still reports
`FAILED (coverage)` with 0 of 8 arms covered.

## 7. What is still blocked, and on what

`backtest_attrition_requirement()` reports this as data rather than prose, so the
verdict flips automatically when the upstream artifact changes:

```
Attrition ascertainment: NOT AVAILABLE
  contract status            not_ascertained
  n_retired populated        FALSE
  n_active net of departures FALSE
```

The primary arm scores an active headcount, net of retirement, against a series
that retires nobody. **No change to this engine can close that gap** — it needs
the contract to ascertain retirement. Until then the definition-matched arm is
the comparison the observed series can actually support, and the primary arm is
biased low by roughly the annual departure rate.

## 8. Reproducing

```r
series <- urps_entrant_series(2020L)
model  <- fit_entrant_regime_model(as.data.frame(series[, c("year", "count")]), 2020L)
model                                        # regimes, deferral, break terms
project_entrant_path(model, 2021:2023)       # deterministic path

spec <- supply_parameter_spec(entrant_regime = model)
run_entrant_regime_backtest()                # refit, labelled as one

entrant_regime_rolling_validation(full_series, cumulative_series, horizon = 3L)
```
