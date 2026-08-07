# Developer workflow for the urpssim R package.
#
# These targets mirror the CI gate (.github/workflows/R-CMD-check.yaml) so the
# failures that gate merges surface locally instead of on main. `make check`
# runs the SAME command CI runs, with the SAME error-on-warning policy, so a
# green `make check` means a green PR.
#
# Requires the package dependencies plus rcmdcheck + roxygen2. On Claude-on-web
# the SessionStart hook installs them; elsewhere run `make deps` once.

RSCRIPT ?= Rscript

.PHONY: help deps document test check check-fast validate-docs clean

help:
	@echo "make deps          - install package deps + dev tooling (rcmdcheck, roxygen2)"
	@echo "make document      - regenerate man/*.Rd + NAMESPACE from roxygen comments"
	@echo "make test          - run the testthat suite"
	@echo "make check         - R CMD check --as-cran, error on WARNING (the CI gate)"
	@echo "make check-fast    - check without tests/vignettes (quick doc/namespace pass)"
	@echo "make validate-docs - doc-drift tripwire, NO package deps needed (base R only)"

# Install everything needed to test + check. Reads the dependency tiers from
# DESCRIPTION so it never drifts from the manifest; mufflyaccess (private) is
# best-effort and only attempted when a GITHUB_PAT is present.
deps:
	$(RSCRIPT) --vanilla -e ' \
	  if (!requireNamespace("pak", quietly = TRUE)) install.packages("pak"); \
	  dcf <- read.dcf("DESCRIPTION"); \
	  field <- function(f) if (f %in% colnames(dcf)) setdiff(trimws(gsub("\\(.*?\\)", "", strsplit(dcf[, f], ",")[[1]])), "R") else character(); \
	  hard <- unique(c(field("Depends"), field("Imports"), field("LinkingTo"))); \
	  sugg <- setdiff(field("Suggests"), "mufflyaccess"); \
	  pak::pak(c(hard, sugg, "rcmdcheck", "roxygen2"), ask = FALSE, upgrade = FALSE)'

# Regenerate man/ + NAMESPACE. Run before committing whenever roxygen comments
# change; stale docs are a WARNING under --as-cran (codoc / Rd mismatches).
document:
	$(RSCRIPT) --vanilla -e 'roxygen2::roxygenise()'

test:
	$(RSCRIPT) --vanilla -e 'testthat::test_local()'

# The exact CI invocation: --no-manual --as-cran, error_on = "warning".
# error-on-warning is what makes broken Rd links, namespace typos, and codoc
# mismatches fail here the same way they fail the merge gate.
check: document
	$(RSCRIPT) --vanilla -e 'rcmdcheck::rcmdcheck(args = c("--no-manual", "--as-cran"), error_on = "warning")'

# Faster subset: skips tests + vignette build (which need the heavy/compiled
# deps). Still catches the documentation defect classes -- Rd cross-references,
# missing NAMESPACE exports, codoc, undeclared globals.
check-fast: document
	$(RSCRIPT) --vanilla -e 'rcmdcheck::rcmdcheck(args = c("--no-manual", "--no-tests", "--no-build-vignettes", "--as-cran"), error_on = "warning")'

# The no-dependency tripwire. Unlike `check` / `check-fast` (which build the
# package and therefore need every Import, duckdb included), this only PARSES
# the committed R/ and man/, so it runs on a bare `Rscript` with nothing
# installed -- the machine where `make deps` cannot, and `make document` will
# not, run. It catches the doc-drift subset that keeps reaching main: a stale
# NAMESPACE, an export with no man page, a codoc \usage/formals mismatch, and
# structurally invalid Rd. Not a substitute for `make check`; a pre-push gate
# for the machine that cannot run it.
validate-docs:
	$(RSCRIPT) --vanilla scripts/dev/validate_docs.R

clean:
	rm -rf ..Rcheck *.Rcheck *.tar.gz
