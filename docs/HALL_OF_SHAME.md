# Hall of shame

`GUARDS.md` catalogues the guards and the defect that motivated each. This file
catalogues the **mistakes themselves** — including the ones made while building
those guards, by the people and agents building them. It exists because the
defect classes below recur, and because a repository that records only its
successes teaches nothing about how the failures got in.

Rules for this file:

- Every entry is real, dated and attributable. Nothing hypothetical.
- Entries are kept even when the fix was fast. Speed of repair says nothing
  about how the defect entered.
- The most valuable column is **what caught it** — and the most valuable
  entries are the ones where the honest answer is *nothing did*.

Sorted by the lesson, not by date, because the lessons repeat.

---

## I. Guards that did not guard

The worst class. A guard that is silently inert is worse than no guard, because
the absence of an alarm gets read as evidence of health.

### 1. A gate that refused everything looked safe and delivered nothing

**2026-08-08, mine.** The first manuscript-eligibility gate free-text scanned
`MANIFEST.txt` for the tokens `exploratory`, `fallback`, `failed`. Every
manifest `begin_validation_run()` writes carries the line:

```
exploratory          FALSE
```

The scan matched its own **field name** and refused all five authoritative run
directories — a declaration of eligibility read as evidence of ineligibility.

Dangerous because the failure mode is invisible in the direction that matters.
A gate that refuses everything produces no bad output, looks maximally strict,
and would have been "fixed" by whoever next needed a table — by relaxing it.

**Caught by:** running it. It failed loudly on all five sources at once, which
is the only reason the design flaw surfaced instead of a token being quietly
narrowed.

**Rule:** a structured field must be read as structure. `parse_manifest()` now
parses key/value and asserts `exploratory` is present and `FALSE`; only the
*values* are scanned. Pinned in
`tests/testthat/test-manuscript-eligibility.R` in **both** directions, because
"admits what is eligible" is the direction that actually broke.

### 2. The export registry a stray row had silently disabled

**2026-08-08.** `demand_dimension_status,api` had been appended **above** the
`"export","category"` header in `tests/export-registry.csv`. `read.csv` took
the data row as the header, `reg$export` came back `NULL`, and **all four
export-wiring gates collapsed at once** — reporting 449/449 exports as
unregistered while asserting nothing.

**Caught by:** reading the gate's output because a number looked implausible.
Not by the gate. Not by the suite.

**Rule:** the file now carries an ordering comment naming this incident. A
registry whose schema can be destroyed by an append is a registry that needs a
schema assertion, not a comment — that remains open.

### 3. A guard wired to nothing, and a fix that hid itself from the detector

**2026-08-08, mine.** `assert_no_coverage_rate_claim()` was implemented, tested
and called by nothing. Wiring it into `interval_label()`, I wrote:

```r
say <- function(x) { assert_no_coverage_rate_claim(x, status); x }
```

The orphan detector drops any line matching `name <- function`, so the fix
**hid the call from the very gate meant to notice it**. Folding it onto one
line made the wiring invisible to the detector that motivated the wiring.

**Caught by:** the gate, on the first attempt. The one entry in this file where
the machinery worked exactly as designed against its own author.

### 4. A gitignore rule that swallowed source, silently — for the second time

**2026-08-08, mine.** `*/manuscript/`, written for simulation *output* folders,
matched `scripts/manuscript/`. `git status` simply did not mention the two new
source files. Nothing errored; the work just would not have been committed.

The aggravating fact: `.gitignore` **already documents an identical incident**
twenty lines above, where `*_projections*` swallowed
`man/gap_projections_all_scenarios.Rd` and R CMD check reported the function as
undocumented no matter how often the docs were regenerated.

**Caught by:** listing what `git add` would stage, rather than trusting
`git status` to be complete.

**Rule:** whitelist narrowly (`!scripts/manuscript/`), never by broadening the
original pattern — whatever `*/manuscript/` was written to exclude still
exists. And a broad ignore pattern is a silent-failure generator: prefer
anchored paths.

---

## II. Primitives that are silently wrong

No error, no warning, plausible output, wrong number.

### 5. `formatC(x, format = "d")` truncates

**2026-08-08, mine.** A 95% lower bound of `1070.975` rendered as **1,070**.
Every fractional bound in the specification table was low by up to a full unit,
in the direction of the null, invisibly.

**Caught by:** eyeballing one rendered bound against its source CSV — an
accident, not a check.

**Rule:** `round()` before `formatC()`. There is still no test asserting that a
rendered value round-trips to its artifact; that is the real fix and it is open.

### 6. `nzchar(NA)` is `TRUE`

**2026-08-08, mine.** Six NA-NPI roster rows survived a blank filter and
entered the numerator's key set **while being reported as zero blanks**. It
would not have stopped the run; it would have shifted a bound.

**Rule:** never filter missingness with a string predicate. `is.na()` first,
explicitly.

### 7. `data.table::fread()` has no `comment.char`

**2026-08-08, mine.** The provider-type mapping's rationale header parsed as
data; all 25 provider types came back unmapped.

**Caught by:** the mapping's own design — unknown types **stop the run**. The
loud-failure requirement working on its author within minutes of being written.
Had the mapping defaulted unknown types to `physician`, the run would have
completed with every provider silently misclassified.

### 8. An alignment heuristic that tested row 1

**2026-08-08, mine.** Right-align inference looked at the first value only, so
the Specification column went right because it begins `1. Derived cohort`.
Cosmetic — listed because it is the same bug shape as #6: a predicate applied
to a sample and generalised to a column.

---

## III. Claims made faster than they were checked

Assertions stated with confidence, then withdrawn. Each was stated to a human
who could have acted on it.

### 9. "The URPS split is definitively not in the archive we have"

**2026-08-07, mine.** False. The Massachusetts Dropbox tree contains BORIM with
NPI and the hash→physician crosswalk. Withdrawn and corrected. The word
*definitively* was doing work that the search behind it had not earned.

### 10. "Five duplicate NPIs"

**2026-08-08, mine.** They were five duplicate `NA`s. There are **zero** true
duplicate NPIs. Corrected in the frozen prespecification by explicit erratum
rather than silent edit.

### 11. "1,498 rows with cert_year ≤ 2024"

**2026-08-08, mine.** Computed across all 1,500 rows including the six with no
NPI. Correct figure: **1,492**. Same erratum.

### 12. "CMS requires a session cookie; curl and wget cannot complete the download"

**Pre-existing, in `data-raw/cms_psps/DOWNLOAD.md`.** False, and it had been
standing instruction to download 3.1 GB by hand. The DCAT `downloadURL`s return
HTTP 200 to unauthenticated `curl` and transferred the full file at ~30 MB/s.
The likely origin: someone hit the human-facing portal page rather than the
catalogue-listed object, and wrote the conclusion into the runbook.

**Rule:** a claim that something is impossible is a claim, and belongs to the
same evidentiary standard as a result. The correction is now in the file, named
as false rather than quietly replaced.

---

## IV. Numbers that drifted from their source

### 13. The Tier A upper bound was wrong in five places

**2026-08-08.** The artifact says `89.5459`, which rounds to **89.5**.
`VALIDATION_RESULTS.md` said **89.6** in five locations, and the error had
propagated into conversation and back into two paragraphs I wrote by copying
the existing value.

**Caught by:** the first run of `scripts/manuscript/build_tables.R`. The
generator found the error on the first number it rendered — which is the
argument for the generator, made by the generator.

### 14. The manuscript's own tables have no run identity

**Standing, unresolved.** Both tables currently in `MANUSCRIPT.docx` come from
bare CSVs in `artifacts/diagnostics/`. No diagnostics script calls
`begin_validation_run()`, so those numbers carry no model SHA, no input hashes
and no `COMPLETED` marker. They are not wrong — they are **unattributable**,
which is a different problem and one a renderer cannot fix.

Recorded in `docs/submission/tables/TABLE_INDEX.md` under *Excluded, on
purpose*, so the omission reads as a decision. Converting
`entrant_regime_bias_decomposition.R` and `interval_honesty_scorecard.R` to
manifest-first is the open work.

### 15. A capture rate that is true and misleading

**2026-08-08.** Pooled provider-file capture across the URPS basket is 96.8%.
Quoted alone it suggests near-complete data. It is an E/M artifact: capture on
the *procedural* codes runs 40.2% (prolapse) to 54.3% (sling).

**Rule:** capture is now column 2 of Table 5, immediately after the service, so
a bound cannot be read off the table without the fraction of volume behind it.
Structural, not conventional.

---

## V. Reinventing what already existed

### 16. A hand-rolled GFM table renderer

**2026-08-09, mine.** `build_tables.R` hand-wrote the pipe-table emitter —
separator row, column widths, padding — when `knitr::kable(format = "pipe")`
does it, knitr is already this package's `VignetteBuilder`, and the repo
already depends on it. Same turn: a local `sha256()` wrapper when
`hash_inputs()` in `scripts/validation/_provenance.R` already takes a named
vector of paths, returns named digests, and records a missing file as `NA`.

Not merely redundant. A second hasher is a second place for "what counts as the
identity of an input" to drift from the manifests it has to agree with.

**Caught by:** the maintainer, reviewing. Not by any check.

**Rule:** search the repo and the existing dependency set before writing a
utility. What survived the swap is the part that was actually domain logic —
deciding *which columns are magnitudes* — not the rendering.

---

## What the pattern says

Counting the entries by what caught them:

| caught by | n |
|---|---:|
| a guard doing its job | 2 |
| running the thing and reading the output | 5 |
| a human reviewing | 4 |
| accident — noticed a number that looked off | 3 |
| still open | 2 |

Only two were caught by automation. The single most productive habit in this
repository is not a gate: it is **rendering the number next to its source and
looking at both**. Every gate here was written after a human did that and found
something.

The second pattern: **the same defect shape recurs across unrelated layers**. A
predicate applied to a sample and generalised (#6, #8). A broad pattern reaching
a path invented later (#2, #4). A claim about impossibility asserted without the
attempt (#9, #12). Recognising the shape is faster than rediscovering each
instance.
