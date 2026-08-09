# Validation results

Produced by `scripts/validation/01_temporal_validation.R` and
`scripts/validation/02_monte_carlo_convergence.R`.

## Provenance

Both scripts emit a provenance block (`scripts/validation/_provenance.R`) so the
figures below remain attributable after the model moves on. A results document
without this is a claim about a state of the world that no longer exists.

| field | value |
|---|---|
| analysis date | 2026-08-08 |
| git SHA | `6781098` |
| package sources clean | yes |
| R | 4.4.2 |
| urpssim | 0.5.0 |
| mufflyaccess | 0.10.0 (pinned by commit) |
| contract version | 3.0.0 |
| roster snapshot | 2026-07-22 (n = 1,339) |
| contract source commit | `74085a9e695eec5350275a29d8655512ad57422b` |
| baseline supply 2023 | 1,306 |
| RNG seeds | 20260801, 11, 202 |
| iteration counts | 250, 500, 1,000, 2,000 |

## Evidentiary status

Three labels, kept distinct so "reproduced" stays an evidentiary claim rather
than a workflow status:

| label | meaning |
|---|---|
| **Provisional historical** | Generated while `main` was changing. No single repository state corresponds to the complete analysis, so it cannot be reproduced even in principle. |
| **Authoritative pinned** | First complete run from an immutable pinned model state, with the manifest written BEFORE computation. |
| **Reproduced** | A second independent run against that exact pinned specification regenerates the authoritative numbers. |

### Why the numbers below are Provisional historical

Not an administrative oversight, and not merely "the SHA was not recorded."

**Concurrent commits modified supply-model source files during execution**, so
no single repository state corresponds to the complete original analysis. The
R/-touching commits landed at 12:24:45, 12:42:23 and 12:47:09 while the supply
runs were in flight; `23af29e` ("refuse an impossible conversion, and stop the
lag from drifting") changed `R/supply-acgme_fellows.R` and
`R/supply-review_followups.R` — supply-side files, while supply-side numbers
were being produced.

The code stayed syntactically valid and the numbers stayed plausible throughout.
What changed was the scientific object being evaluated. That failure mode is
more dangerous than a merge conflict precisely because nothing looks wrong.

If a pinned run reproduces these values exactly, that demonstrates robustness —
it does **not** retroactively confer provenance on them.

| analysis | status | authoritative run_id | reproduction run_id |
|---|---|---|---|
| `01_temporal_validation.R` | **Reproduced** | `20260808T154509_temporal_validation_1e24ac8` | `20260808T154514_…` |
| `02_monte_carlo_convergence.R` | **Reproduced** | `20260808T133320_mc_convergence_1e24ac8` | `20260808T140238_…` |
| `04_delegation_claims_evidence.R` | **Reproduced** | `20260808T193315_delegation_claims_evidence_c471388` | `20260808T193322_…` |
| `03_utilization_fte_triangulation.R` | **Exploratory** | — | — |

All reproduced pairs matched every table at **zero tolerance**, from independent
worktrees pinned to a single commit, each run passing `read_validation_run()`.

`03` is exploratory for a different reason from the others: not provenance, but
**unresolved parameters** — FPMRS-specific productivity and the URPS share among
physician-delivered care. Provenance work cannot fix that.

Sections 5–6 below record what the earlier provisional analysis produced; the
authoritative runs reproduce those values.

### Run identity is established before computation

`begin_validation_run()` writes the manifest **first**, refuses to start from a
dirty model tree, and returns a run directory the analysis writes its tables
into, so a number and its provenance share a directory and cannot be separated.
Manifests live in `artifacts/validation/<timestamp>_<analysis>_<model_sha>/`.

Manuscript analyses run from an **isolated pinned worktree by default**, not as
a precaution for important runs:

```sh
git worktree add --detach /tmp/urpssim-validation <model-sha>
# data-raw/, artifacts/ and config/ are gitignored or .Rbuildignore'd, so link
# them in: the worktree pins CODE, and data identity is recorded by checksum.
```

### What the four SHA fields mean

They are not expected to agree.

| field | identifies |
|---|---|
| `head_sha` | repository state when provenance was inspected |
| `model_sha` | the model implementation that generated the analysis |
| `validation_sha` | the validation implementation that measured it |
| `contract_sha` | the governing data contract (mufflyaccess artifact) |

## Recommended production default

**n = 1,000 iterations.** Not chosen conventionally: it is the smallest
simulation size satisfying a criterion declared before the multi-seed results
were viewed. Use n = 2,000 for final manuscript sensitivity analyses where the
extra numerical stability is cheap.

These are **validation of the calculation, not validation of the calibration**.
Everything here tests whether the engine forecasts and converges properly. None
of it establishes that the base-year adequacy calibration is correct for URPS —
see the gate note at the end.

---

## 1. Primary: rolling-origin validation, prespecified contemporary origins

Origins 2017–2020, horizon 3 years, strictly out-of-time: a training window is
admitted only when its outcome was observable at the origin. These origins are
`backtest_multi_window()`'s own defaults, not a set chosen after seeing
performance.

| Origin | Target | Observed | Predicted | \|% error\| | Covered | Interval width |
|---:|---:|---:|---:|---:|:--:|---:|
| 2017 | 2020 | 1099 | 997.9 | 9.20% | yes | 3185 |
| 2018 | 2021 | 1180 | 1094.4 | 7.25% | yes | 1172 |
| 2019 | 2022 | 1234 | 1169.2 | 5.25% | yes | 818 |
| 2020 | 2023 | 1306 | 1203.6 | 7.84% | yes | 687 |
| **median** | | | | **7.5%** | **4/4** | **995** |

Coverage is reported **with** width. An interval can cover by being uselessly
wide, and the 2017 interval does exactly that.

In these results the widths fall monotonically as the usable training record
lengthens (3185 → 1172 → 818 → 687). Accumulating history is a plausible
explanation; this is an observed pattern, not a mathematical property.

## 2. The 2017 origin: two limitations compounding

Its interval is enormous and its lower bound is **negative**. Deliberately not
truncated at zero.

* Only two prior errors are available: `df = 1`, so `t(0.975) = 12.71`.
* Both training windows fall in the backlog regime: mean relative error **139%**.

`lower factor = 1 + μ − t·s = 1 + 1.390 − 12.71 × 0.448 = −3.304`

The interval is reporting that the empirical error model is **essentially
unidentified** at that origin. Truncating would conceal the most informative
thing it says. A log-scale construction, `exp(μ_log − t·s_log) = 0.216`, has
positive support and is the principled improvement — reported as a **secondary**
construction rather than swapped in after seeing the negative value.

## 3. Historical stress test: crossing a structural break

`classify_certification_regimes()` labels years from the certification series'
own structure, with no reference to forecast error:

| Years | Certifications | Regime |
|---|---|---|
| 2013–2015 | 655, 175, 102 | **backlog** |
| 2016–2019 | 36–48 | steady |
| 2020 | 10 | **disrupted** (cancelled examination) |
| 2021–2023 | 54–81 | steady |

Extending the origins back to 2013 forces the model across that break, and point
error degrades accordingly: +171%, +107%, +79% at the earliest cutoffs against
−8.3% to +6.6% for the contemporary four.

This is a finding about **temporal transportability**, not a defect. The model
performs well inside the contemporary data-generating regime and predictably
poorly when extrapolated across a documented discontinuity.

Note that 2020 is itself abnormal, so "2017–2020" should be described as the
*prespecified contemporary validation origins* — **not** as a steady regime.

## 4. Leakage experiment: matched origins

Leave-one-out is **not a competing validation method**. It is here to quantify
what temporal leakage buys. Both methods on the **same four origins**:

| | median \|% error\| | coverage | median width | median Winkler | future windows used |
|---|---:|---:|---:|---:|---:|
| Rolling-origin | **7.5%** | 4/4 | **995** | 995 | **0** |
| Leave-one-out | **2.8%** | 4/4 | **488** | 488 | **14** |

Excluding the unstable 2017 origin, the effect persists: 7.25% vs 2.94% error,
818 vs 499 width.

Every interval covers, so the Winkler score reduces to width — there is no
hidden miss penalty driving the comparison.

**Using future information would have made the model appear ~2.7× more accurate
and ~2× sharper on identical forecast origins.** LOO interval widths are also
roughly flat across origins (438–508) while rolling-origin widths contract,
because every LOO fit sees nearly the whole record and so never faces the
early-history handicap. Leakage does not merely improve estimates; it erases
uncertainty an investigator would genuinely have faced at the time.

## 5. Monte Carlo convergence

Criterion **declared before the run**: across independent seeds the 2050 median
must vary by < 0.5%, and the 2.5th percentile, 97.5th percentile and interval
width by ≤ 5%.

Three independent seeds, 2050 supply FTE, range as % of mean:

| n | median | median range | 2.5% range | 97.5% range | width mean | width range | Verdict |
|---:|---:|---:|---:|---:|---:|---:|:--|
| 250 | 2075 | 0.158% | 0.62% | 1.36% | 227.4 | 14.91% | FAIL |
| 500 | 2076 | 0.162% | 0.52% | 1.01% | 228.9 | 8.82% | FAIL |
| 1,000 | 2075 | 0.144% | 0.36% | 0.72% | 226.6 | 3.80% | **PASS** |
| 2,000 | 2072 | 0.119% | 0.14% | 0.30% | 227.8 | 2.54% | **PASS** |

**n = 1,000 is the smallest passing count.**

The median is stable everywhere (range ≤ 0.16%). Mean width shows **no
systematic dependence on n**; what improves is the *reproducibility* of the
endpoints (14.9% → 2.5%).

> A single-seed sweep of this same design produced widths of 249 → 242 → 232 →
> 229 and reads as convergence. It is not: across three seeds the mean width is
> flat, and that sequence was one seed sitting at the high end at every count.
> Monte Carlo error moves an estimated quantile in either direction. Do not
> report a single-seed width trend.

## 6. Parameter-uncertainty sensitivity: retirement hazard

The engine draws the entrant rate but holds the retirement hazard fixed, because
it is published without standard errors. Fixing it is not the same as knowing
it. At n = 2,000, single seed:

| Retirement treatment | 2050 median | 2.5% | 97.5% | Width | Width inflation | Median shift |
|---|---:|---:|---:|---:|---:|---:|
| Fixed | 2073.11 | 1964.10 | 2193.45 | 229.35 | — | — |
| Moderate (CV 0.15) | 2075.89 | 1931.54 | 2212.85 | 281.31 | **+22.7%** | +0.13% |
| High (CV 0.30) | 2083.96 | 1880.38 | 2252.52 | 372.14 | **+62.3%** | +0.52% |

Interval-width inflation is `100 × (W_uncertain / W_fixed − 1)`. This is **not**
a variance decomposition, and it should not be described as "uncertainty
previously hidden" — a related but different quantity,
`100 × (1 − W_fixed / W_uncertain)`, gives 18.5% and 38.4%. Do not interchange
them.

Propagating plausible retirement-hazard variation had little effect on the
median 2050 projection but substantially widened the conditional simulation
interval. That is *median insensitivity to the assumed variation*, not a
demonstration that fixing retirement is unbiased.

**CV 0.15 and 0.30 are declared sensitivity assumptions**, labelled moderate and
high. They are not estimated uncertainty distributions and are not confidence
bounds on retirement rates.

Seed-to-seed width noise at n = 2,000 is ±2.5%, an order of magnitude below both
inflation effects, so the comparison is not seed-driven.

---

## 7. Claims-attributed provider mix (analysis 04)

**Status: REPRODUCED.** Independent authoritative runs from clean pinned
worktrees produced identical six-table outputs at zero tolerance. The analysis
estimates claims-attributed physician versus non-physician provider mix for
sling and pessary episodes, 2008–2016. It does **not** identify URPS versus
other physicians, nor actual hands-on provider type where incident-to billing
obscures delivery.

Four external objects are hashed into the manifest and rechecked at completion:
the CADR provider-specialty extract, its data dictionary, and the two
version-controlled mapping tables (`scripts/validation/mappings/`).

| Service | Model physician share | Claims-attributed | Difference | wRVU effect |
|---|---:|---:|---:|---:|
| `sling_procedure` | 0.979 | 0.961 | **−1.9 pp** | −55,896 |
| `pessary_care` | 0.653 | 0.849 | **+19.6 pp** | +201,520 |

**Sling** — the claims estimate **closely agreed with** the modelled physician
share. That is strong external support where claims attribution resists
incident-to misclassification; it is not validation in isolation, because the
claims source carries its own attribution limitations. Zero APP-attributed
slings across 4,608 episodes.

**Pessary** — claims attribution is substantially more physician-heavy than the
model. Treat as an **upper-bound sensitivity arm**, not a corrected value: this
is exactly where nurse-delivered care billed under a supervising physician is
indistinguishable in claims.

**Weighting** — pooled physician share is **94.6% wRVU-weighted against 88.5%
episode-weighted** (APP 1.7% vs 8.6%). A sling carries 12.29 wRVU and a pessary
fitting 0.89, so episode weighting silently up-weights the cheaper service. This
empirically demonstrates why **workload-weighted delegation is the relevant
quantity for an FTE model**, rather than asserting the choice.

**Trend** — pessary 85.4% → 84.5%, sling 96.2% → 95.7% across 2008–2011 vs
2012–2016: no material temporal trend in *claims-attributed* physician share.
This does not establish that actual APP delivery was stable, since rising
incident-to billing would present as exactly this flatness.

**Unmapped-code audit** — 37 observed CMS specialty codes, 37 mapped, 0
unmapped. Unknown codes stop the run rather than falling to "other", verified
by deleting code 16 and confirming the failure.

### What deterministic A/B does and does not prove

Reproduction here demonstrates **computational reproducibility and
declared-input completeness** — it is what caught an undeclared `/tmp/sv.rds`
intermediate feeding the matrix comparison. It is **not** statistical
replication, and it does not show the claims attribution is unbiased. The
incident-to bound is a property of the data source; no amount of reproduction
touches it.

### Preserved audit trail

The earlier exploratory runs are retained, not relabelled or deleted. The pair
`20260808T192755` / `20260808T192802` reproduced all six tables under the
exploratory gate and is the evidence that promotion was warranted. Keeping them
documents the transition from an analysis with assumptions hidden in code and
temp files to a fully declared specification.

## 8. The productivity comparison is specified in advance (analysis 03)

`03` is exploratory and will stay so until the AUGS/MGMA urogynecology
productivity report arrives. What is unusual is that its **interpretation was
fixed before the external values were known**, at commit `33a9759` on
**2026-08-08**. That commit is the evidence of timing; preserve it.

Frozen in advance:

| element | value |
|---|---|
| primary comparison | R = AUGS/MGMA median wRVU/FTE ÷ **5,193.1** |
| denominator | model-implied raw wRVU/FTE on 2026-08-08, guarded against drift |
| R = 1.00 | exact agreement in the utilisation-to-FTE conversion |
| R > 1.00 | reference model requires MORE FTE for the same physician-attributed workload |
| R < 1.00 | reference model requires FEWER FTE |
| sensitivity | AUGS/MGMA p25 and p75 |
| pass/fail threshold | **none declared** — direction and magnitude are the information |
| future inputs | licensed report and extracted percentile CSV, both already declared and hashed |

### Wording for the manuscript

This was **not** registered in a formal public registry. Do **not** describe it
as a *preregistered* analysis. The defensible phrasing is:

> prospectively specified before obtaining the external productivity benchmark

with the commit SHA and date available as evidence if challenged.

### Two ratios, kept separate on purpose

If the model legitimately evolves before the report arrives, `03` refuses
authoritative status rather than silently recomputing against a new denominator,
and reports both:

* **R_prespecified** = AUGS/MGMA ÷ 5,193.1 — tests the frozen model state
* **R_current** = AUGS/MGMA ÷ current implied — describes the current model

Keeping them separate means later model development cannot erase an unfavourable
prospective result.

### What the test can and cannot show

It tests one link in the chain — **physician-attributed workload → FPMRS FTE**.
A close result gives independent support for the model's absolute
utilisation-to-FTE conversion; a divergent one localises the disagreement to the
productivity denominator; a p25–p75 range spanning the model calibration makes
real-world productivity heterogeneity part of the explanation rather than an
inconvenience. **No outcome establishes workforce adequacy or unmet need.**

## 9. URPS share among physician-delivered care (analysis 05)

**Reproduced.** Runs `20260808T211711` / `20260808T211844`, identical at zero
tolerance across all six tables. Specification frozen in
`docs/PRESPEC_URPS_SHARE.md` at commit `faf72dc`, before any roster-linked
quantity was computed; implementation at `82de574`.

Analysis `04` established the physician-versus-nonphysician split and could not
address URPS-versus-other-physician, because the CADR archive carries no
provider identifier. The 2024 Medicare Physician & Other Practitioners file
does, so the split is reachable by joining `Rndrng_NPI` to the frozen roster
(1,492 NPIs, `cert_year <= 2024`). CMS itself has no FPMRS provider type — it
pools subspecialists with generalists exactly as carrier specialty code 16
does — so the roster join is the only route, not a convenience.

### Partial identification, not a point estimate

CMS suppresses every NPI × HCPCS × POS cell under 11 beneficiaries, which
deletes the low-volume tail specifically. Rather than rescale by `1/capture` —
which would assume the suppressed volume resembles the retained volume, the one
thing the suppression mechanism rules out — the unidentified remainder `M` is
carried as unidentified and the result is an interval.

Primary tier: the 13 anatomically female-specific codes, covering 36.0% of the
model's physician work RVU.

| service | capture | L | H | observed-cell | model |
|---|---:|---:|---:|---:|---:|
| `sling_procedure` | 54.3% | **36.0%** | 86.5% | 72.7% | 30.5% |
| `prolapse_procedure` | 40.2% | 26.9% | 91.1% | 75.1% | 30.5% |
| `pessary_care` | 49.4% | 18.3% | 82.6% | 51.2% | 37.3% |
| **wRVU-weighted** | 44.3% | **28.8%** | **89.6%** | 73.4% | — |

### The interval is too wide to propagate; the lower bound is not

`[28.8%, 89.6%]` does not resolve `P(URPS | physician)`. Propagating it through
`03` would produce an FTE range spanning most of the plausible space, which is
the correct answer to the question asked and not a useful input.

The **lower bound is informative on its own**, and it bites once. On
`sling_procedure` the model assumes 30.5% of physician-delivered work is URPS,
while at least **36.0%** of Medicare sling services demonstrably were —
in the configuration *least* favourable to URPS, assigning every suppressed
service to some other physician. The model under-attributes sling work to
subspecialists on the highest-work-RVU service in the basket. `prolapse` and
`pessary` assumptions sit inside their intervals and are not contradicted.

### What this does not license

The bound is a valid lower bound **for Medicare FFS 2024**. Transporting it to
the model's all-payer estimand requires an assumption that is probably false in
a known direction: Medicare sling patients are older, and older patients
plausibly reach subspecialists more often, so the Medicare URPS share is likely
*above* the all-payer share. The finding is therefore that the model's sling
assumption is below a Medicare-specific floor, not that it is wrong nationally.

Unmatched NPIs are **non-roster physicians**, never "generalists" — non-match
is equally consistent with a roster miss. Roster ascertainment for 2024 is
**undocumented**: the CSV holds 1,500 rows and 1,495 NPIs, its provenance
sidecar states 1,100 and 1,092, and the companion coordinate extract holds
1,552. Three artifacts, three counts. No completeness figure may be quoted
until that is reconciled, and any roster incompleteness biases `L` downward,
which makes the sling finding conservative rather than fragile.

The observed-cell share (73.4% weighted) is **not** a national share. It is the
size of the selection: it exceeds the lower bound by 45 points because
suppression removes low-volume providers and retains high-volume ones.

The E/M component — 45.6% of physician work RVU — remains unidentified. The PUF
carries no diagnosis field, so `99213` cannot be restricted to urogynaecologic
care, and no share was computed against a 179.8M-service all-specialty
denominator.

## What none of this establishes

Every result above validates the **calculation**. None validates the
**calibration**.

`balance_reversal_threshold()` computes a tipping point of **1.294×** the
reference adequacy calibration, reproduced independently. `balance_reversal_sentence()`
nevertheless refuses to emit it, because the demand calibration behind it is
below the tier required for a manuscript-ready threshold. It is a
software-validation result: it verifies the machinery, not the workforce.

The base-year adequacy figure (`REFERENCE_ADEQUACY_CALIBRATION`, 0.948) is a
calibration choice adopted by analogy from a physical-therapy workforce model.
The three published donor anchors span 1.00–1.065× the reference, and the model
uses the **lowest** of them. That range is legitimate to report as externally
motivated sensitivity; it does not establish where URPS adequacy actually lies.

Conditional simulation intervals are **not** empirical prediction intervals: in
the frozen 2020→2023 back-test the observed value fell outside the 95% interval
in 8 of 10 arms. Report stochastic and forecast uncertainty separately.
