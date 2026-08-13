# urpssim 0.5.0

## What changed in how results may be read

* **Intervals are labelled for what they are.** In the 2020→2023 back-test, an
  interval built from individual stochasticity alone covered the observation in
  0 of 8 arms and was 0–40 providers wide on a count near 1,300 — two arms had
  zero width. Drawing the entrant rate per iteration widens them to 129–148.
  `supply_parameter_spec()` makes that draw, and any run without one now says so
  and reports the band through `interval_label()` as a Monte Carlo range rather
  than a forecast interval.

* **Every input declares a calibration tier.** `baseline_gap()` requires
  `calibration_status` instead of inferring it: the same arithmetic is
  *calibrated* from a fielded URPS survey and *derived by analogy* from another
  specialty's published distribution, and the function refuses to guess which
  one it was handed. `allow_analogy` must be declared by the caller rather than
  inherited silently.

* **Headline gaps carry their provenance.** `baseline_gap_claim()` reports
  method, calibration status, population and year, source specialty, whether the
  figure was externally measured or derived, its uncertainty interval, and its
  sensitivity to the baseline anchor. The distinction between "the current
  shortage is X" and "the model-implied gap under the specified calibration is
  X" is enforced rather than left to the writer.

* **The base-year capacity anchor is still unresolved**, and `capacity_status()`
  says so in the result rather than in a footnote. It is a published
  physical-therapy distribution standing in for a URPS practice-capacity survey
  that has not been fielded. Note that `required_fte(t) = anchor × wRVU(t) /
  wRVU(base)`, so the delegation matrix, the demand calibration and the workload
  levels all cancel — a 2.1× change in calibration moved 2050 required FTE by
  0.25%. Only the anchor and the shape of demand growth move the answer.

## Supply

* Fixed an entrant double-count that inflated the rate to 93/yr. Stock now
  reconciles against `cumsum(flow)` on the live contract. The shipped model had
  been running at 86.9 entrants/yr against a documented 55.
* Entrant scenarios are no longer inert. A single `param_spec` was shared across
  scenarios and `entrant_mean` took precedence over `entrants_per_year`, so
  "Fellowship output +10%" and "−10%" returned results identical to baseline to
  the last digit — the most policy-relevant lever in the model did nothing.
* The certification lag defaults to the documented three-year fellowship.
* `entrant_trajectory()` supports a per-year entrant path; the naive filled
  variant is labelled NAIVE in its own scenario name.

## Calibration

* **Fielded URPS access anchor (Lizeth).** `build_lizeth_access_anchor()` reads
  the Lizeth national mystery-caller REDCap export and reports realized
  base-year access — appointment obtainment, the wait-time distribution, and
  insurance and clinical-scenario strata — so current URPS access can be
  described from fielded observation rather than borrowed evidence. It
  deliberately does **not** invert wait time into an adequacy ratio:
  `capacity_status_with_lizeth()` records that access is now measured while
  leaving `capacity_status()$resolved` FALSE, because identifying latent
  productive capacity from access needs a separately validated response
  function. `lizeth_adequacy_evidence()` registers the observation as a
  calibration *target*, not a calibrated level.

* **Adequacy → access response bridge.** `adequacy_access_load()` and
  `simulate_access_for_adequacy()` connect two engines that never touched: the
  FTE-denominated base-year gap (`required = base_supply_fte / adequacy`) and
  the demand-vs-capacity access queue (`clear_access()`). Because the queue
  depends on demand and capacity only through their ratio, the map is exact in
  FTE units — `rho = 1 / adequacy` — and is the faithful composition of shipped
  functions, not a new assumption. This is the forward "adequacy → simulated
  appointment wait" path the model previously lacked. It surfaces, rather than
  hides, that a single national adequacy below 1 saturates the steady-state
  queue (infinite wait), so the finite observed wait implies catchment
  heterogeneity — the tension a later fit against Rabice/Lizeth would resolve.
  It stays `assumed_illustrative`, is not wired into
  `run_workforce_microsimulation()`, and does not resolve `capacity_status()`.

## Demand

* NAMCS demand calibration (`namcs_demand_calibration()`), scaling only the
  services NAMCS actually measures.
* The NAMCS `SEX` coding was verified empirically rather than assumed — it is
  inverted relative to the documentation, confirmed against sex-specific
  diagnoses (N40 0/136, C61 0/68, Z34 108/0, N81 18/0). MEPS was checked the
  same way and found correct.
* Dynamic multistate disease model, with an open-population extension.

## Validation

* Rolling-origin validation alongside leave-one-out. The distinction is the
  point: rolling-origin admits a training year only when its outcome is
  observable at the origin (`target_year <= origin`), and LOO is retained
  explicitly as the leaky comparator, reporting `n_train_future`.
* The frozen back-test record is protected by a SHA-256 drift gate. Do not
  regenerate the artifact unless `BACKTEST_RECORD_2020_2023` and its
  reproduction test are updated together.
* Coverage improved from 0/8 to 2/10 arms. This is **not** presented as a
  modelling win: arm 5 (NRMP) is most accurate on 2017–2020 and least accurate
  after extending to 2010–2020, so an untuned change made it worse.

## Geography

* Provider point locations imported, with coverage reported **by pathway**.
  Overall coverage alone was 72% and would have read as reassuring while the
  urology pathway sat at 0% — an access surface built on it would have run,
  produced plausible ratios, and omitted 23% of the workforce. Now 1,336 of
  1,339 (99.8%): 99.9% ABOG, 99.4% ABU.
* `safe_rbind()` refuses silent coercion when merging geocoding runs. Merging
  five runs with `rbind()` coerced `retrieved_on` to Date and NA'd 364 of 1,540
  rows while leaving coordinates and `source_run` intact, so nothing downstream
  would have noticed.
* `screen_new_coordinates()` checks a candidate point against the address its
  own source recorded. One recovered candidate sat 131 km from its ZIP, in the
  wrong state, with finite coordinates inside the US bounding box and complete
  provenance — every structural check passed it. State agreement is documented
  and tested as an *invalid* screen: `state` is the certifying board's mailing
  state, and 20.5% of physicians practise elsewhere.

## Reproducibility and gates

* `mufflyaccess` is pinned by **commit**, not version. Two materially different
  builds both reported version 0.10.0 in one session — 56 exports versus 98 —
  disagreeing about whether `n_retired` is `NA` or zero, which is the field the
  back-test attrition guard reads. No version constraint can express that.
* `scripts/ci/check_suite.R` runs the full suite from the repository root and
  enforces `tests/skip-budget.csv`. `FAIL 0 | SKIP 66 | PASS 2337` reads exactly
  like a clean build; 36 gates that only exist when the source tree does —
  including the back-test drift gate and the contract pin — had been reported as
  passing while running nothing.
* `tests/export-registry.csv` enumerates the 67 of 403 exports that reach no
  pipeline, each with a declared kind (`api`, `dormant`, `unwired_gate`). Ten
  unwired gates are pinned by name so the list can only shrink. This is the
  package's most persistent defect class — `assert_demand_calibrated()`,
  `opportunity_placement_shares()`, `hours_coef` and others were each
  implemented, tested, documented, and reachable from nothing.
* CI installs the Suggests it can reach, so the spatial-access, MEPS
  care-seeking and coordinate-screen guards actually run.

## Documentation

* Runnable examples on the entry points and the user-facing analysis functions.
