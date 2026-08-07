# Guards

Every guard in this package exists because of a specific defect that reached
`main`. None is hypothetical. This document is the index: what each one checks,
the failure that motivated it, and — where it matters — what it deliberately
does *not* check.

The recurring theme is that the dangerous failures are **quiet**. A crash gets
fixed the day it happens. What survives is the run that completes, produces
plausible numbers, and is wrong in a way no summary statistic shows.

---

## 1. A capability that is implemented and connected to nothing

**The defect.** `assert_demand_calibrated()` was defined, tested, documented —
and called by nothing. The orchestrator accepted a `calibration` argument,
stored it in the run metadata, and never checked it, so demand anchored to no
observed quantity passed silently. The geography layer was the same shape:
`opportunity_placement_shares()`, entrant placement and mid-career migration
were reachable only by calling the engine directly, because the orchestrator
never passed `placement_shares`. Every run was national-headcount-only.
`hours_coef`, `apply_hrsa_surgical_fte()`, `apply_calibration_scalars()` and
`param_spec` in `run_backtest()` were each found the same way, one at a time,
by accident.

**Why tests cannot catch it.** A test calls the function — that is what makes it
a test — so it proves the function works and nothing about whether the *package*
ever reaches it. Passing tests are exactly what this failure looks like.

**The guard.** `tests/export-registry.csv` and `tests/testthat/test-export-wiring.R`.
Every export that appears nowhere in `R/`, `scripts/` or `vignettes/` must be
registered with a declared kind:

| kind | meaning |
|---|---|
| `api` | A user calls it directly. Orphan status is expected. |
| `unwired_gate` | An assertion nothing invokes. A guard nobody calls does not guard anything — this is real debt. |
| `dormant` | Implemented, connected to no pipeline. Wire it or drop it. |

67 of 403 exports are currently in this position. The gate fails on a new
unregistered orphan, on a stale row, and on the total growing. The ten
`unwired_gate` entries are pinned **by name**, not by count, so the list can only
shrink and one cannot be silently swapped for another.

---

## 2. A gate that is present but never runs

**The defect.** `R CMD check` runs tests inside `<pkg>.Rcheck/`, where `config/`,
`artifacts/` and `data-raw/` do not exist — they are `.Rbuildignore`d, and
`data-raw/urps_roster` deliberately so, because the extract carries NPIs. Every
test that looks for the repository root therefore skipped itself. That silently
disabled the frozen back-test drift gate and the `mufflyaccess` contract pin in
CI for as long as they had existed. Separately, `dependencies: "hard"` installed
no Suggests, so twelve more tests skipped on `sf`, `survey`, `zipcodeR`,
`ggplot2` and `blme`/`lme4`.

The roster-dependent coordinate tests — including the by-pathway coverage
assertion that keeps the urology-at-0% hole closed — skipped for months.

**Why it is quiet.** `FAIL 0 | SKIP 66 | PASS 2337` reads exactly like
`FAIL 0 | SKIP 0 | PASS 2403`. The difference is 66 assertions nobody is making.

**The guard.** `scripts/ci/check_suite.R` runs the whole suite **from the
repository root**, where those gates execute, and enforces
`tests/skip-budget.csv`: every skip must match a declared reason with a budgeted
count. A skip is legitimate; an *undeclared* skip is a gate going dark.

Two lessons are built into the budget file itself:

* **A new skip is usually not a row to add.** "sf not installed" in CI meant
  install `sf`, not declare that the spatial guards do not run. Those reasons
  are budgeted at **0** on purpose.
* **A malformed pattern matches nothing and reports `0/4`**, which reads as a
  satisfied budget rather than a broken row — the same silence the script
  exists to break. Two rows shipped with `\\(`, which `read.csv` passes through
  as a literal backslash. The script now rejects an invalid pattern outright. It
  still cannot catch a pattern that is valid and wrong.

---

## 3. A merge that damages a column nobody computes with

**The defect.** Merging five geocoding runs with `rbind()` coerced
`retrieved_on` from character to Date, because one input had parsed it as a
date, and silently `NA`'d 364 of 1,540 rows. The coordinates and `source_run`
were untouched, so every downstream number stayed correct while a quarter of the
file lost its provenance. The repair attempt then did it in reverse — assigning
a character into the now-Date column `NA`'d all 1,540.

**The guard.** `safe_rbind()` harmonises differing column classes to character
*before* binding — deliberately, in one direction, because `rbind`'s choice
depends on argument order, so the same merge in a different order produces
different `NA`s — then errors if a listed column gains missing values.
`load_urps_provider_coordinates()` is the second layer: it refuses any extract
with a missing `source_run` or `retrieved_on`, whatever route the damage took.
A point whose origin is unrecorded cannot be audited.

Nothing about this is specific to coordinates. Any `rbind` of independently read
CSVs can do it.

---

## 4. A merge that is clean, where the point is wrong

**The defect.** Recovering the last 15 uncoordinated providers turned up 13
candidates. Twelve were right. One — recorded address Glen Dale, WV 26038 — was
geocoded to 40.95, −81.54, which is Ohio, 131 km away. It had finite coordinates
inside the US bounding box, a valid NPI, complete provenance and a real address.
`safe_rbind()` would have bound it without complaint and the loader would have
accepted it, because nothing about it is *structurally* wrong. It was the only
candidate matched on name rather than identifier.

A wrong point does not announce itself downstream either: it moves one provider
between markets, which changes an access surface by an amount no summary
statistic would flag.

**The guard.** `screen_new_coordinates()` compares each candidate against the ZIP
its own source recorded. The observed separation was total — eleven good points
within 7.4 km, the bad one at 130.7 km — so the 25 km threshold is not
load-bearing. The test pins **both halves**: the bad point is rejected *and* the
twelve good ones pass. A screen that also rejects good points is a coverage cap,
and the pressure is then to loosen it until the number comes back.

**What this deliberately does not check.** The obvious screen is "does the point
fall in the recorded state?" It fires on 10% of the extract, and that is not
error: `state` is the certifying board's mailing state, and in the source
carrying both, 1,481 of 7,208 physicians (20.5%) practise in a different state.
Screening on agreement rejects roughly one correct point in eight. It is
documented and tested as invalid, because it is tempting enough that someone
would otherwise adopt it and then "fix" the data to satisfy it.

---

## 5. A dependency that changes underneath a version number

**The defect.** Two materially different `mufflyaccess` builds both reported
version 0.10.0 during one working session: 56 exports without
`urps_retirement_status()`, and 98 exports with it. They also disagreed about
how `n_retired` is served — integer zeros versus `NA` — which is the field the
back-test attrition guard reads. Any check of the form
`packageVersion("mufflyaccess") >= "0.10.0"` passes for both.

**The guard.** `R/core-contract_pin.R` pins a 40-character commit SHA, asserted
identical in `DESCRIPTION`'s `Remotes:` and in the CI workflow's
`extra-packages`. Three copies of a SHA is two chances to drift;
`test-contract-pin.R` removes both. Capability is checked **before** identity: a
build missing a required export is refused whatever version it claims, while a
usable build on a different commit warns rather than failing, so a legitimate
contract upgrade is not blocked.

`backtest_retirement_regime()` is version-tolerant by design — it reports the
same semantic state through either representation, which is what let this
repository absorb the build swap mid-session without a code change.

---

## 6. A frozen artifact that drifts from the claim about it

**The defect.** Back-test coverage is quoted in warnings, documentation and the
manuscript. If the artifact is regenerated without updating the recorded
summary, every one of those statements silently becomes a claim about a file
that no longer exists.

**The guard.** `BACKTEST_RECORD_SHA256` in `R/validation-backtest_status.R`, with
`verify_backtest_record()` and `assert_backtest_record_current()`. **Do not
regenerate the artifact unless `BACKTEST_RECORD_2020_2023` and its reproduction
test are updated together.**

---

## 7. Guards that check shape rather than substance

Five holes found by attacking the guards rather than the model:

* a **constant** entrant series passed the uncertainty gate — it has a variance
  of zero and quantifies nothing
* `NA`, `Inf`, `0` and `−1` were accepted as calibration scalars
* a `urps_baseline_gap` could be forged with a bare `structure()` call, bypassing
  the constructor that enforces the calibration tier
* `interval_label()` crashed on `NaN` coverage — `sprintf("%d", NaN)` is an error
* an all-`NA` scoring table scored as passing

The lesson generalises: a guard that validates the *type* of a thing has not
validated the *claim* it stands for.

---

## Running the guards

```sh
# Everything, from the repository root, with the skip budget enforced.
Rscript scripts/ci/check_suite.R
```

`R CMD check` remains the packaging gate. It is not a substitute for the above,
for the reason in §2: it cannot see the source tree.
