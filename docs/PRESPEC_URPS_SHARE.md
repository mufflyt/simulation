# Prospective specification: URPS share among physician-delivered care

**Frozen 2026-08-08, before any roster-linked share was computed.**

This is a *prospective specification*, not a preregistration — nothing here was
lodged with a public registry. The defensible claim is that the estimand,
the bounds, the code sets, the terminology and the failure conditions were
fixed in a commit that predates the first computed value, and the commit date
is the evidence. Use the same wording discipline as
`docs/VALIDATION_RESULTS.md` §8.

What had been observed when this was written: the CMS file's *structure* —
column names, row counts, provider-type vocabulary, per-code national totals,
the suppression capture rates, and the fact that 1,176 of 1,495 roster NPIs
appear somewhere in the basket. What had **not** been observed: any quantity
in which roster membership and service volume are crossed. No value of
`U_s`, `O_s`, `N_s`, `M_s`, or any share derived from them existed.

---

## 1. Why this parameter

`03_utilization_fte_triangulation.R` cannot close without

$$P_{\mathrm{URPS}\mid\mathrm{phys}} = \text{share of physician-delivered URPS
workload delivered by URPS subspecialists.}$$

The delegation matrix currently splits workload into `urps_share`,
`app_share` and `other_clinician_share`, where the third is *other physicians*.
Script `04` established the physician-vs-nonphysician split from CADR and
explicitly could not address the URPS-vs-other-physician split, because the
archive carries no provider identifier. The CMS file does carry one.

---

## 2. Estimand

For a service $s$, among care delivered by **physicians**, the fraction
delivered by physicians on the frozen URPS roster.

Population: Medicare fee-for-service Part B, data year 2024, United States.
This is a scope statement that must travel with every number. Sling and
prolapse patients skew younger than the Medicare population, so the Medicare
mix is not the national mix, and no result here licenses a claim about
commercially insured care.

---

## 3. Code sets — and two exclusions decided before computing

`URPS_CPT_BASKET` was built to convert modelled service volumes into work
RVUs. It was never a case-finding definition, and two parts of it cannot serve
as one.

### Excluded: evaluation and management

`new_consultation` (99203/99204/99205) and `return_visit`
(99212/99213/99214) have national totals of 24.4M and 179.8M services. Those
are all of Medicare outpatient E/M, across all of medicine. A denominator of
179.8M return visits makes "URPS share" a statement about how large
urogynecology is relative to American medicine, not about who delivers
urogynecologic care. The PUF has no diagnosis field, so the visits cannot be
condition-restricted. **No E/M share is computed.** Reporting an
uninterpretable number invites its misuse more than omitting it does.

### Excluded from the primary: sex-neutral procedural codes

Cystoscopy (52000), bladder instillation (51700), urodynamics
(51726/51728/51729/51741/51784), botox (52287) and PTNS (64566) are performed
on men and for conditions outside the pelvic floor — haematuria surveillance,
BPH, neurogenic bladder. Their denominators import a urologic workload the
model does not represent.

### Tier A — primary. Female pelvic-floor-specific, 13 codes

| service | codes | $w_s$ (work RVU) |
|---|---|---:|
| `pessary_care` | 57160 | 0.89 |
| `sling_procedure` | 57288, 51992, 57287 | 12.2864 |
| `prolapse_procedure` | 57240, 57250, 57260, 57265, 57282, 57283, 57425, 57120, 57268 | 12.1155 |

These cover **36.0%** of the model's 2025 physician work RVU
(1,877,882 of 5,214,311). Every code is anatomically female-specific.

### Tier B — secondary. Adds the 9 sex-neutral procedural codes

Covers 54.4% of physician work RVU, and is reported only with the statement
that its denominator includes male and non-pelvic-floor utilisation, so it
bounds a broader population than the model's.

---

## 4. Quantities

Per service $s$, summing over that service's codes and over both places of
service:

| symbol | definition | source |
|---|---|---|
| $T_s$ | national service total | Geography file, `Rndrng_Prvdr_Geo_Lvl == "National"` |
| $U_s$ | services on provider rows whose NPI is on the frozen roster | Provider file |
| $O_s$ | services on retained **physician** NPIs not on the roster | Provider file + type mapping |
| $N_s$ | retained services from APP or facility types | Provider file + type mapping |
| $M_s$ | $T_s - U_s - O_s - N_s$ | derived |

$M_s$ is volume whose provider identity is unavailable, overwhelmingly because
of the <11-beneficiary cell suppression. It is not modelled away and it is not
inflated away: **no naive $1/\text{capture}$ rescaling.**

---

## 5. Primary result — suppression-robust bounds

$$L_s=\frac{U_s}{T_s-N_s} \qquad H_s=\frac{U_s+M_s}{T_s-N_s}$$

$L_s$ assigns every unidentified service to some other physician; $H_s$
assigns all of it to URPS. The truth is inside.

**Both bounds are valid but neither is sharp**, and the reason is worth
recording because it will be asked. $M_s$ contains suppressed *nonphysician*
volume as well as suppressed physician volume, since only *retained*
nonphysician services enter $N_s$. So the denominator $T_s - N_s$ exceeds the
true physician-delivered total: $L_s$ is conservative twice over, and $H_s$
over-assigns. Widening in the safe direction is the intended behaviour. On
`prolapse_procedure`, where only 40.2% of national volume survives
suppression, the interval will be wide. That is the answer, not a failure of
the method.

## 6. Secondary — observed-cell share

$$\frac{U_s}{U_s+O_s}$$

Labelled **"URPS share among observable physician provider–service cells."**
Never "the national URPS share." Its purpose is to quantify selection: it is
upwardly biased whenever subspecialists carry larger per-NPI volumes and
generalists are preferentially suppressed, which is exactly the situation the
capture table describes. Report it beside $[L_s, H_s]$ so the size of the
selection is visible.

## 7. Model-relevant aggregate — wRVU-weighted bounds

Aggregated from summed workloads, **not** by averaging service-specific
percentages:

$$L_{\mathrm{wRVU}}=\frac{\sum_s U_s w_s}{\sum_s (T_s-N_s)w_s}
\qquad
H_{\mathrm{wRVU}}=\frac{\sum_s (U_s+M_s) w_s}{\sum_s (T_s-N_s)w_s}$$

with $w_s$ the service's work RVU from `urps_service_workload()`. Computed over
Tier A for the primary and Tier B for the secondary.

If $[L_{\mathrm{wRVU}}, H_{\mathrm{wRVU}}]$ is narrow enough to be useful, the
**entire interval** propagates through `03`. A point estimate is not required
and will not be manufactured.

---

## 8. Terminology, frozen

An NPI that fails to match the roster is a **non-roster physician**, never a
"generalist." Non-match is consistent with: a URPS physician the roster missed,
certification outside ABOG/ABU, certification after the roster's vintage, or a
genuine generalist. Treating every non-match as definitely non-URPS biases the
URPS share **downward**, which is why $H_s$ exists and why the roster's
ascertainment must be reported beside the result.

Likewise, the 78.7% of roster NPIs appearing in the basket is **not roster
sensitivity**. Absence is consistent with no Medicare billing, no basket
service in 2024, retirement, practice outside FFS, or suppression. It is a
linkage-feasibility statistic and nothing more.

---

## 9. Frozen inputs

| role | path | sha256 |
|---|---|---|
| numerator / components | `data-raw/cms_psps/PHY_R26_P05_V10_D24_Prov_Svc.csv` | `509dc7ce4cd02d8dd160d50d33ce5d942cd120ea306ff1eb2b6ece4f59cb2c23` |
| denominators $T_s$ | `data-raw/cms_psps/MUP_PHY_R26_P05_V10_D24_Geo.csv` | `c26956788333d03c0080017121c19e8e4d9990e9fa8ff385d7e1a2849c45074a` |
| roster | `data-raw/urps_roster/urps_roster_2026-07-22.csv` | recorded by the run manifest |
| provider-type map | `scripts/validation/mappings/cms_provider_type_class.csv` | recorded by the run manifest |

All four are declared `inputs` to `begin_validation_run()` and hashed into the
run identity. Neither CMS file is committed.

### The roster, and an unresolved discrepancy

Inclusion rule, fixed here: rows of `urps_roster_2026-07-22.csv` with a
non-blank NPI and `cert_year <= 2024`, deduplicated on NPI.

**Its provenance sidecar does not describe it.**
`urps_roster_2026-07-22_PROVENANCE.txt` states "Rows: 1100 / Unique NPIs:
1092"; the companion coordinates extract holds 1,552 NPIs. Three artifacts,
three counts. Until that is reconciled, **roster ascertainment for 2024 is
undocumented**, and every result must say so rather than quoting a completeness
figure. This is a reason the lower bound is the conservative one to lead with.

> ### Erratum, 2026-08-08 — descriptive counts, not the rule
>
> This section originally read "1,500 rows, 1,495 distinct NPIs, **5 duplicate
> NPIs**, 6 blank NPIs, and 1,498 rows with `cert_year <= 2024`." Two of those
> figures were wrong and are corrected here rather than silently edited, since
> the document is the freeze record.
>
> There are **zero duplicate NPIs**. The five "duplicates" were duplicate
> *missing* values — `duplicated()` counting six `NA`s as one distinct value
> plus five repeats. Likewise `cert_year <= 2024` holds for **1,492** rows, not
> 1,498, because 1,498 was computed over all 1,500 rows including the six with
> no NPI.
>
> **The specified rule is unchanged and the frozen population is unchanged.**
> Deduplication on NPI was specified, remains specified, and removes zero rows
> rather than five. The correction is to a description of the input, not to the
> inclusion criteria, and no bound moves: 1,500 − 6 missing − 2 certified after
> 2024 = **1,492**, which is the count analysis 05 has used throughout.

### The reconciliation, and the frozen linkage roster

`scripts/validation/06_roster_reconciliation.R` settles the discrepancy by
comparing identifiers rather than reasoning about which count looks right. It
assigns every row of both data files exactly one disposition and emits the
population 05 consumes.

| step | n |
|---|---:|
| Raw canonical roster rows | 1,500 |
| less rows with no NPI | −6 |
| less rows failing the NPI check digit | 0 |
| less duplicate NPI rows | 0 |
| less rows with no certification year | 0 |
| less certified after 2024 | −2 |
| **Final 2024 linkage roster** | **1,492** |

All 1,494 non-missing NPIs are ten digits and pass the Luhn check over the
80840 prefix. The six missing-NPI rows are all ABU (urology) and all
`in_model_baseline = FALSE`; the two post-2024 certifications are also ABU.

`data-raw/urps_roster/urps_linkage_roster_2024.csv`, sha256
`fbdd8332a8de6f4870b65c83cefccfec3990ccca912d53165c3333c09934132c`, 1,492 NPIs.
Analysis 05 asserts that hash and stops on mismatch — the roster is gitignored,
so a pinned worktree protects the code and nothing protects the numerator.

**Activity in 2024 is deliberately not a criterion.** `U_s` is formed by
intersecting with services actually billed in 2024, so a provider who did not
bill contributes zero regardless. An activity filter cannot remove a spurious
match; it can only discard a real one — someone flagged retired in a 2026
snapshot who was practising in 2024 — which lowers `U`, lowers `L`, and weakens
the bound the analysis leads with.

**The sidecar is a superseded generation, not a filter.** All five of its
checkable assertions disagree with the file it accompanies: 1,100 vs 1,500
rows, 1,092 vs 1,495 distinct NPI values, 830 vs 1,135 ABOG rows, 270-of-294 vs
359-of-365 ABU rows with an NPI, and `has_medicare_2024` asserted FALSE against
1,208 rows where it is TRUE. No subset of the current file should be
constructed to match it.

**115 valid NPIs remain an unresolved ascertainment gap.** They appear in the
coordinate extract, pass every validity check, and no roster row carries them.
They are not added, because the coordinate file mixes source runs — one is a
general obstetrics-and-gynaecology geocode file — and nothing reachable from
this repository establishes that these particular records are URPS
subspecialists. If any are, `U` and every lower bound rise, so the exclusion is
conservative in the direction that matters.

---

## 10. Failure conditions — the run stops, it does not degrade

1. A `Rndrng_Prvdr_Type` absent from the mapping. Named, run stopped. Never
   swept into a default.
2. A basket HCPCS absent from the Geography national file.
3. $M_s < 0$ for any service, which would mean the provider file exceeds the
   national total and invalidates the identity.
4. Either CMS file's SHA-256 not matching §9.
5. A roster-matched NPI whose CMS type is nonphysician — not fatal, but the
   count is reported; roster membership wins and the row enters $U_s$.

---

## 11. What no outcome establishes

No value of these bounds establishes workforce adequacy, unmet need, or that
the delegation matrix is right or wrong. It supplies one conditional
probability inside an FTE identity whose other terms — the productivity
denominator and the base-year anchor — remain the binding uncertainties
recorded in `docs/VALIDATION_RESULTS.md`. A narrow interval near the matrix's
assumed `urps_share` is corroboration of one term, not validation of the model.

The E/M component of $P_{\mathrm{URPS}\mid\mathrm{phys}}$ — 45.6% of physician
work RVU — remains unidentified after this analysis, for the reason in §3. Any
propagation through `03` must apply the interval to the procedural component
and state the office-based component as unresolved.

---

## 12. Deviations from the specification as given

Recorded because the spec came from the analyst, and silent amendment would
defeat the point of freezing it.

1. **E/M excluded** (§3). As specified, $T_s$ for `return_visit` would have
   been 179.8M all-specialty visits. The resulting share would be arithmetically
   valid and scientifically meaningless.
2. **Tier A restricted to female-specific codes** (§3). As specified, the
   procedural denominator would have imported male urologic utilisation through
   cystoscopy, urodynamics, botox, PTNS and instillation.
3. **Non-sharpness of both bounds made explicit** (§5). The specification
   presented $L_s$ and $H_s$ as the least- and most-favourable configurations;
   they are also loosened by suppressed nonphysician volume sitting in $M_s$.

Everything else — the five quantities, both bounds, the observed-cell
secondary, the wRVU-weighted aggregation from summed workloads, the "non-roster
physicians" terminology, the refusal to rescale by capture rate, and the
loud-failure mapping requirement — is implemented as specified.
