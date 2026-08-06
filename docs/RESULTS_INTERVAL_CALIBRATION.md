# Results — Interval calibration and the source of forecast bias

Manuscript-ready results text for the forecast-validation section, with the
numbers and figure it cites. Everything here is reproducible from committed
artifacts (no restricted data):

- `scripts/diagnostics/interval_honesty_scorecard.R` → `artifacts/diagnostics/interval_honesty_scorecard.csv`
- `scripts/diagnostics/entrant_regime_bias_decomposition.R` → `artifacts/diagnostics/entrant_regime_bias_decomposition.csv`
- `scripts/figures/fig_interval_calibration.R` → `artifacts/figures/interval_calibration.{png,pdf}` (**Figure X**)

---

## Results paragraph (interval calibration)

We evaluated forecast intervals for the board-certified URPS stock out of sample
over 2021–2023, and found that empirical coverage alone is a misleading measure
of interval quality. Three real out-of-sample forecasts illustrate the point
(**Figure X**). A leakage-free rolling-origin forecast achieved perfect nominal
coverage (4/4, 100%), but only because its 95% intervals were degenerate — mean
width ≈ 1,466 providers, with one lower bound falling below zero, an impossible
value for a cumulative certification count. The stock-flow microsimulation, by
contrast, produced sharp intervals (mean width ≈ 92–97 providers, ~15× narrower)
and its coverage depended entirely on how the target was defined. When career
attrition was applied to the *cumulative* certification count — a stock from which
providers do not exit — the forecast covered 0% of years and under-predicted every
year; when the model was matched to the cumulative definition (no attrition), the
same forecast covered 67% (2/3). Scored by the interval score (Gneiting & Raftery,
2007; interval width plus a shortfall penalty of `(2/α)` per unit of miss), the
definition-matched forecast was an order of magnitude better than the wide
rolling-origin forecast (137 vs 1,466) despite lower nominal coverage. Coverage
ranked the forecasts in exactly the reverse order of the interval score, ranking
the least informative (widest) forecast first. Because the interval score charges
both excess width and misses on a single scale, it cannot be improved by widening
intervals to force coverage, nor by narrowing them without correcting the point
forecast; we therefore report it alongside coverage throughout.

## Results paragraph (source of the bias)

The remaining error was a property of the point forecast, not the interval. The
2020→2023 backtest under-predicted the 2023 stock by 127 providers (−9.7%;
observed 1,306 vs 1,179), and this miss decomposed into two distinct components
(`entrant_regime_bias_decomposition.csv`). Sixty-eight percent of it (+86
providers, +28.7/yr) was a measurement-definition error: applying career attrition
to a cumulative certification count removes providers the count never loses.
Correcting the definition alone raised coverage from 0% to 67%, cut the interval
score from 1,732 to 137, and reduced the signed bias from −87 to −31 providers.
The residual 32% (+41 providers, +13.7/yr) reflects a genuine acceleration in net
entry: the observed stock grew by +69/yr, above every entrant assumption that used
only pre-cutoff information (32.7–55/yr). This entrant-regime acceleration was
identified only after observing the 2021–2023 miss; specifying a model component
on the strength of the error it is scored against is model selection on the test
set, so we do not report it as a validated result. Instead we preregistered the
entrant-regime specification (frozen functional form and evaluation protocol,
hashed before the next data vintage) and will evaluate it prospectively by
leakage-free rolling origin (`scripts/run_preregistered_rolling_origin.R`), so that
the accelerated-entry hypothesis is tested on data that played no part in forming
it.

## Figure X — caption

**Coverage rewards the wrong model; the interval score does not.** Three real
out-of-sample forecasts of the URPS certification stock (2021–2023) in
(empirical coverage, mean interval score) space; lower interval score is better.
Empirical coverage ranks the degenerate wide-interval rolling-origin forecast
first (100%), whereas the interval score — which penalizes both width and misses
(Gneiting & Raftery, 2007) — ranks the sharp, definition-matched forecast first
(interval score 137 at 67% coverage vs 1,466 at 100%). The attrition-on forecast
(0% coverage) is a definition mismatch: career attrition applied to a cumulative
count. Reproducible from `artifacts/diagnostics/interval_honesty_scorecard.csv`.

## Numbers behind the figure

| evaluation | coverage | mean width | mean interval score | signed bias |
|---|---:|---:|---:|---:|
| rolling-origin (wide) | 100% | 1466 | 1466 | −88 |
| sharp, attrition ON (definition mismatch) | 0% | 92 | 1732 | −87 |
| sharp, no-attrition (definition-matched) | 67% | 97 | **137** | −31 |

Miss decomposition (2020→2023, shipped arm → observed 1,306):

| step | 2023 level | Δ | share of miss | annualized |
|---|---:|---:|---:|---:|
| shipped forecast (attrition ON, entrants = 55) | 1179 | — | — | — |
| + fix definition error (attrition OFF) | 1265 | +86 | 68% | +28.7/yr |
| + close entrant-regime residual | 1306 | +41 | 32% | +13.7/yr |
