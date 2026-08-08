# Denominator audit: what 1,306 is, and why it is not ~1,700

**Verdict: 1,306 and ~1,700 measure different populations, and neither is
wrong. The back-test target is not invalidated — but the observed series it
scores against is not the quantity its own column name claims, and that does
change what the back-test can be said to establish.**

Nothing was recalibrated. The target remains 1,306.

---

## 1. Exactly what 1,306 is

Traced to `mufflyaccess`'s shipped manifest
(`inst/extdata/urps_manifest.json`) and reproduced arithmetically:

```
2025 roster snapshot                            1,339
  less providers whose URPS cert year > 2023      −33
                                                ------
board_certified_active, national, 2023          1,306
```

That identity is exact, and it is the whole construction. **The 2013–2023
series is a single 2025 roster back-projected by certification year.** It is
not eleven annual observations; it is one population filtered eleven ways.

Three consequences follow directly, and all three are visible in the data:

- `n_active == n_ever_certified` in **every row**, for every pathway. Verified:
  the stated stock equals the running cumulative sum of the annual flows
  exactly, for ABOG, ABU_NET_NEW, and the combined series.
- `n_retired` is `NA` throughout, and `urps_retirement_status()` returns
  `"not_ascertained"`.
- No attrition process exists anywhere in the series. What looks like attrition
  having been applied is instead a **survivor filter**: only providers still
  resolvable in the 2025 roster appear in *any* year.

### The definition, verbatim from the manifest

> `board_certified_active`: urps_subspecialty_cert_year <= Y and not retired by
> Y, within the geography. Keyed on the URPS SUBSPECIALTY certification year
> (training-accurate, post-fellowship), NOT the primary cert_year. ABOG uses the
> real sub1startdate (2013+); ABU and unmatched ABOG use a fellowship proxy
> (primary + 2 urology / + 3 OB-GYN). 33 providers whose URPS cert/proxy
> postdates 2023 are excluded from the 2023 active count.

> **Deduplication:** ABOG = OB/GYN board-certification (FPMRS/URPS) pathway. ABU
> enters only as NET-NEW to the ABOG NPI set; dual-boarded
> general-urology-primary providers held out; reinstated dual-boarded (n=38)
> kept; **retired/unmatched dropped**. Active ABOG and ABU NPI sets are provably
> disjoint (asserted).

So, answering the audit question directly — 1,306 is:

| Candidate | Verdict |
|---|---|
| ever certified | **No.** Retired and unmatched providers are dropped. |
| currently certified | **No.** Lapsed certification is not tracked; nothing re-checks certification status. |
| active clinically | **No.** No clinical-activity screen; `n_retired` is unascertained. |
| ABOG only | No — that is the 1,027 cell. |
| ABOG + ABU | Only in a **deduplicated** sense: ABOG + ABU *net-new*, never a sum of both boards' diplomates. |

**It is: the count of NPI-identified, roster-resolvable, deduplicated,
non-retired providers whose URPS subspecialty certification year (real for 73%,
fellowship-proxied for 27%) is ≤ 2023.** It is a *roster stock*, not a
certification stock and not an active-workforce stock.

### How much of the cert year is inferred rather than observed

| Basis | n | Share |
|---|---:|---:|
| ABOG real `sub1startdate` | 984 | 73% |
| ABOG primary + 3 yr fellowship proxy | 47 | 4% |
| **ABU primary + 2 yr fellowship proxy (all ABU)** | **308** | **23%** |
| Total | 1,339 | |

**196 proxy-derived cert years land before 2013**, which is before FPMRS
certification existed. The manifest records this
(`values_before_2013_from_fallback: 196`). Those necessarily pile into the
2013 bucket, which is exactly where the 655 "initial backlog" sits.

Note also that **every ABU cert year is a proxy** — `primary urology cert year
+ 2`, because "ABU subspecialty date not yet sourced". The 2-year offset is at
least correct: urology URPS fellowship is two years.

## 2. The ~394 discrepancy

ABOG/ABU, October 2023: *"Approximately 1,700 subspecialists have achieved
FPMRS certification by the two boards."*

That is a **certification-registry** count: everyone who ever passed, from the
boards' own records. 1,306 is a **roster** count. The manifest names four
exclusions that a registry count would not apply, and they are additive:

| Mechanism | Direction | Evidence |
|---|---|---|
| **Retired / unmatched dropped** | −, likely the largest | Manifest dedup rule, explicit |
| **No NPI identity** | − | Roster built by intersecting ABOG/ABU public directories with CMS NPPES; no NPI, no row |
| **Dual-boarded urology-primary held out** | − | Manifest dedup rule, explicit |
| **ABU counted net-new only** | − | A dual-certified provider is one row here, plausibly two certifications in "by the two boards" |
| **Deceased** | − | NPPES-resolvable population only |

Against that, one mechanism pushes the roster count *up* relative to a
2013-onward certification count: **196 pre-2013 proxy cert years** are people
whose real URPS certification, if any, is not what the proxy says.

**Best explanation: the gap is dominated by "retired/unmatched dropped" plus
the NPI-identifiability requirement, with dual-board deduplication contributing
a smaller share.** A rough sanity check supports this: ~394 over a
2013-certified population of ~1,700 is ~23% lost to retirement, death, and
non-identifiability across 10–12 years, which is unremarkable for a workforce
whose earliest cohort certified at mid-career (the 2013 backlog cohort was
already in practice).

**This cannot be resolved further from inside this repository.** Doing so needs
a board-supplied denominator: ABOG and ABU counts of diplomates ever certified,
by year, with retirement/death status. `backtest_attrition_requirement()`
already reports the same blocker from the other direction.

## 3. Does the back-test remain scientifically valid?

**Partly — and less than its own documentation currently claims.**

What survives: the target is internally consistent, correctly deduplicated,
correctly keyed on the subspecialty cert year, and `validate_backtest_target()`
guards every dimension that could make the comparison invalid. Scoring a
subspecialty-cert-year cohort against 1,306 is coherent.

What does not survive, and is **new information from this audit**:

- **The observed series is survivor-filtered, so it is biased downward in
  early years and unbiased only at the snapshot.** A provider who certified in
  2015 and left practice before 2025 appears in no year of the series — not even
  2015. The 2013–2020 values are therefore *lower* than the true contemporaneous
  counts, by an amount that grows the further back you look.
- `docs/BACKTEST_2020_TO_2023.md` says the observed series "applies **no**
  attrition" and that the model will "structurally under-predict" against it.
  That is the wrong sign for the early years. The series applies **one round of
  attrition retroactively, at 2025**, uniformly across all years. A model that
  applies attrition forward from 2020 is not obviously biased low against it;
  the comparison is confounded, not merely mismatched.
- The 2020 cutoff and 2023 target sit only 2 and 5 years before the snapshot,
  so both are relatively lightly filtered. **The direction and size of the
  residual bias are unquantified**, and cannot be quantified without the
  retirement data the contract does not carry.

**Recommendation: keep the back-test, keep the target, and downgrade the
claim.** It tests whether the entrant model reproduces the growth of a
survivor-filtered roster stock. It does not test the active workforce, and it
should not be described as validating a workforce projection.

## 4. Two denominators, not one

The audit supports the hypothesis this work started from. These are separate
quantities and the codebase currently has only one of them:

| Estimand | Correct denominator | Available today? |
|---|---|---|
| Certification **flow** validation (entrant model, conversions) | Ever-certified stock, by cert year, undeduplicated by board | **No.** Needs ABOG/ABU registry counts. ~1,700 is the only glimpse. |
| Supply microsimulation (**active workforce**) | Active clinical stock, net of retirement/death/exit | **No.** `n_retired` unascertained; no activity screen. |
| Roster-resolvable stock (what we have) | 1,306 | Yes — and it is neither of the above |

Until both exist, **1,306 is the right target for what the back-test actually
does**, and the entrant-to-certification conversions calibrated against it
inherit its survivor filter. That is a bias of unknown size and is now recorded
rather than assumed absent.

## 5. What must never be conflated

Enforced by `assert_denominator_estimand()` and its tests:

- **"ever certified" is not "active workforce".** In this contract they are the
  same numbers, which is precisely why the distinction has to be asserted rather
  than trusted — `n_active == n_ever_certified` makes the error invisible.
- **No field named a probability, rate, share, or completion may exceed 1.**
  Already enforced for conversions by `.assert_possible_conversion()`; extended
  here to any denominator-derived proportion.

---

## Reproducing

```r
m <- jsonlite::read_json(system.file("extdata", "urps_manifest.json",
                                     package = "mufflyaccess"), simplifyVector = TRUE)
m$active_in_year_definition          # the estimand, verbatim
m$deduplication_rule                 # retired/unmatched dropped; ABU net-new
m$urps_subspecialty_cert_year        # 73% real dates, 27% fellowship proxy
sum(unlist(m$urps_subspecialty_cert_year$source_counts))   # 1339 = roster
```
