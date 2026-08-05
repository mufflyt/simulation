# Known breakage

Short-lived notes about `main` being red for a reason already diagnosed, so the
next person to run the suite does not re-derive it. **Delete an entry once its
condition clears** — a stale entry here is worse than no entry, because it
trains people to ignore the file.

---

## `test-backtest.R` — 4 failures against `mufflyaccess` 0.10.0

**Status:** open
**Found:** 2026-08-04, running the full suite after `a3c6b8d`
**Owner:** whoever is working the back-test / entrant-regime line — this is not
a bug in that work, it is a dependency-version skew.

### Symptom

```
[ FAIL 4 ] in tests/testthat/test-backtest.R  (:123, :415, :416, :422)
```

Three distinct-looking errors:

| line | reported as |
|---|---|
| 123 | expected an error matching `UNASCERTAINED`, got `BACK-TEST CONTRACT MISMATCH: ... NO ATTRITION` |
| 415 | `all(is.na(s$n_retired))` was FALSE |
| 416 | `'urps_retirement_status' is not an exported object from 'namespace:mufflyaccess'` |
| 422 | same mismatch as 123 |

### Root cause — one, not three

The tests were written against a **newer `mufflyaccess` contract than the one
installed**. Verified against the installed package (0.10.0, 56 exports):

```r
mufflyaccess::urps_counts_long()
#   n_retired all NA?  FALSE
#   n_retired all 0?   TRUE      <- distinct values: 0
#   n_active == n_ever_certified everywhere?  TRUE
"urps_retirement_status" %in% getNamespaceExports("mufflyaccess")
#   FALSE                        <- nearest name is urps_retired_values
```

The tests expect retirement to be **unascertained** (`n_retired` all `NA`, plus
an accessor saying so). The installed contract instead reports it as
**ascertained and zero** (`n_retired` all `0`). So `validate_backtest_target()`
correctly takes its all-zero branch and raises `NO ATTRITION`, while the tests
assert the `UNASCERTAINED` branch. Same guard, different input contract.

### Production code is NOT affected

`R/34-backtest.R:301` already wraps the accessor in `tryCatch`, so the shipped
path degrades instead of erroring. Both branches refuse without
`acknowledge_no_attrition = TRUE`, so the safety property holds under either
contract — only the message differs. This is test-only breakage, and the guard's
tolerance is the reason.

### Fix

Publish/install the `mufflyaccess` version this work was written against — the
one whose `urps_counts_long()` returns `n_retired = NA` and which exports
`urps_retirement_status()`. That is the real fix; the tests are asserting the
contract they were designed for.

Do **not** paper over it by relaxing the assertions to accept `n_retired = 0`.
The comment at `test-backtest.R:411-413` says these are keyed on the contract's
own accessor precisely so a contract that starts ascertaining retirement fails
loudly rather than drifting out of sync with the guard. Rewriting them to pass
against 0.10.0 would delete that alarm.

If the newer `mufflyaccess` cannot be installed promptly, the narrow stopgap is
to skip — not weaken — the affected tests:

```r
skip_if_not("urps_retirement_status" %in% getNamespaceExports("mufflyaccess"),
            "mufflyaccess predates the unascertained-retirement contract")
```

which keeps the assertions intact and makes the reason legible in the skip list.

### Verifying it has cleared

```r
Rscript -e 'pkgload::load_all("."); testthat::test_file("tests/testthat/test-backtest.R")'
```

Green means the contract skew is gone — delete this section.
