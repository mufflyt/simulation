# Legacy scripts

These are the original DPMM / SWAN / early-workforce scripts. They are **not part
of the package**: they interleave function definitions with analysis code that
runs at top level (`workforce.R` alone has ~196 top-level statements, several of
them `load()` / `read_rds()` / `read_excel()` calls). Loading the package must
not run any of that.

## How to load them

```r
library(urpssim)
load_legacy()            # definitions only — safe, touches no files
load_legacy(functions_only = FALSE)   # runs them as scripts; needs the external data
```

`load_legacy()` sources in the order declared by `LEGACY_LOAD_ORDER` and reports
every function name that gets redefined along the way, so a collision is visible
instead of silent.

## Duplicate function names

Fifteen names are defined in more than one of these files. Which implementation
you get therefore depends on source order. `LEGACY_CANONICAL` declares the
intended owner of each, and `check_legacy_canonical()` verifies the load order
actually delivers it — a test enforces this.

The validation family resolves as:

```
99-unsorted-fragments.R   (oldest, superseded)
  -> dppm_validate_SWAN.R          (superseded for most names)
    -> 03-dppm_validate_SWAN_better.R   (canonical; see the top-level README)
```

Within-file duplicates have been removed. Where the two bodies were identical
the earlier one was deleted; where they diverged the earlier one was renamed to
`<name>_variant1` (or `_variant2`) and flagged with a comment, so nothing was
lost and nothing silently shadows anything else. Those `_variant*` functions are
unreviewed — check which implementation is wanted before relying on either.

## External data

No path is hardcoded. Everything routes through `swan_path()`,
`data_raw_path()` and `external_path()`, which resolve against
`SIMULATION_DATA_ROOT`, then `config/paths.local.yml`, then `config/paths.yml`.
Run `check_external_data()` to see what is reachable on this machine before
starting a long job.
