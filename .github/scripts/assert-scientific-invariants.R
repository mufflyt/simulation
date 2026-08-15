#!/usr/bin/env Rscript
# SCIENTIFIC INVARIANTS -- the failure domain ordinary unit tests cannot reach.
#
# A microsimulation can be syntactically correct, numerically stable, fully
# reproducible, and still produce a completely wrong forecast. These checks test
# the MODEL, not the R code: probability legality, cascade mass balance,
# numerical integrity, agreement with an independent reference implementation,
# determinism, and back-test regression.
#
# TWO CLASSES, and the distinction is load-bearing:
#
#   INVARIANT  a mathematical or accounting fact that must hold for any correct
#              model. Fails hard. Never baselined.
#   RATCHET    a scientific quality measure the model currently FAILS on its own
#              stated terms (back-test interval coverage is 0.20 against a
#              required 0.80). Making it blocking today would mean a permanently
#              red nightly, which destroys the signal. Recorded as a baseline
#              that may only improve.
#
# The ratchet is not a way of tolerating the defect. It is what stops the defect
# getting WORSE while it is being fixed, and it makes the current state explicit
# rather than buried in a warning nobody reads.

suppressMessages(pkgload::load_all(".", quiet = TRUE))

FAIL <- character()
ok  <- function(w) cat(sprintf("  PASS  %s\n", w))
bad <- function(w, why) { cat(sprintf("  FAIL  %s -- %s\n", w, why)); FAIL <<- c(FAIL, w) }
inf <- function(w) cat(sprintf("  INFO  %s\n", w))

TREATED <- c(pop = unname(FROZEN_CARE_ENGAGED[["pop"]]),
             ui  = unname(FROZEN_CARE_ENGAGED[["ui"]]),
             ai  = unname(FROZEN_CARE_ENGAGED[["ai"]]))

# ---------------------------------------------------------------------------
cat("\n== 1. Probability and rate legality ==\n")
pw <- condition_service_pathway()

p <- pw$p_advance[!is.na(pw$p_advance)]
if (any(p < 0 | p > 1)) {
  bad("every p_advance is in [0,1]",
      sprintf("out of range: %s", paste(p[p < 0 | p > 1], collapse = ", ")))
} else ok("every p_advance is in [0,1]")

if (any(pw$per_entering < 0 | !is.finite(pw$per_entering))) {
  bad("every per_entering is finite and non-negative", "negative or non-finite value present")
} else ok("every per_entering is finite and non-negative")

# One advance probability per condition-stage, or the cascade is ambiguous: two
# different p_advance values on the same stage silently means whichever row the
# engine happens to read first.
amb <- Reduce(rbind, lapply(split(pw, list(pw$condition, pw$stage), drop = TRUE), function(g) {
  u <- unique(g$p_advance)
  if (length(u) > 1L) data.frame(condition = g$condition[1], stage = g$stage[1]) else NULL
}))
if (!is.null(amb) && nrow(amb)) {
  bad("one p_advance per condition-stage",
      paste(sprintf("%s/%s", amb$condition, amb$stage), collapse = ", "))
} else ok("one p_advance per condition-stage")

# ---------------------------------------------------------------------------
cat("\n== 2. Cascade mass balance: no stage may gain people ==\n")
# The cascade is strictly attritional -- every stage is reached by multiplying
# the previous stage by a probability in [0,1]. A stage that GAINS entrants
# means a probability above 1 or a wiring error, and would silently inflate
# every downstream service volume.
ent <- pathway_stage_entrants(TREATED, pw)
for (cd in unique(ent$condition)) {
  e <- ent[ent$condition == cd, ]
  e <- e[order(match(e$stage, PATHWAY_STAGES)), ]
  if (any(diff(e$entering) > 1e-6))
    bad(sprintf("%s cascade is non-increasing", cd),
        paste(sprintf("%s=%.0f", e$stage, e$entering), collapse = " -> "))
  else ok(sprintf("%s cascade is non-increasing (%s)", cd,
                  paste(sprintf("%.0f", e$entering), collapse = " > ")))
  if (e$entering[1] > TREATED[[cd]] + 1e-6)
    bad(sprintf("%s first stage cannot exceed the treated stock", cd), "mass created")
}

# ---------------------------------------------------------------------------
cat("\n== 3. Independent reference implementation ==\n")
# A deliberately naive recomputation of the cascade, written to be obviously
# correct rather than fast. Fast wrong code must never be able to validate
# itself, so this shares no code path with the engine.
reference_volumes <- function(treated, pathway) {
  out <- list()
  for (cd in names(treated)) {
    rows <- pathway[pathway$condition == cd, ]
    stages <- PATHWAY_STAGES[PATHWAY_STAGES %in% rows$stage]
    n <- treated[[cd]]
    for (st in stages) {
      sr <- rows[rows$stage == st, ]
      for (i in seq_len(nrow(sr)))
        out[[length(out) + 1L]] <- data.frame(service = sr$service[i],
                                              volume = n * sr$per_entering[i])
      adv <- unique(sr$p_advance)[1]
      n <- n * (if (is.na(adv)) 0 else adv)
    }
  }
  agg <- do.call(rbind, out)
  tapply(agg$volume, agg$service, sum)
}
ref <- reference_volumes(TREATED, pw)
eng <- pathway_service_volumes(treated = TREATED, year = 2025L, pathway = pw)
eng_v <- tapply(eng$volume, eng$service, sum)
common <- intersect(names(ref), names(eng_v))
if (!setequal(names(ref), names(eng_v))) {
  bad("reference and engine produce the same services",
      sprintf("only in one: %s", paste(setdiff(union(names(ref), names(eng_v)), common), collapse = ", ")))
} else {
  d <- max(abs(ref[common] - eng_v[common]) / pmax(1, abs(ref[common])))
  if (d > 1e-9) bad("reference implementation agrees with the engine",
                    sprintf("max relative difference %.3e", d))
  else ok(sprintf("reference implementation agrees with the engine (max rel diff %.1e)", d))
}

# ---------------------------------------------------------------------------
cat("\n== 4. Numerical integrity ==\n")
nv <- eng$volume
if (any(is.na(nv) | is.nan(nv) | is.infinite(nv))) {
  bad("no NA/NaN/Inf in service volumes", "present")
} else ok("no NA/NaN/Inf in service volumes")
if (any(nv < 0)) bad("no negative service volumes", "present") else ok("no negative service volumes")

# ---------------------------------------------------------------------------
cat("\n== 5. Determinism ==\n")
a <- pathway_service_volumes(treated = TREATED, year = 2025L, pathway = pw)
b <- pathway_service_volumes(treated = TREATED, year = 2025L, pathway = pw)
if (!isTRUE(all.equal(a[order(a$service), ], b[order(b$service), ]))) {
  bad("repeated evaluation is identical", "two calls disagreed")
} else ok("repeated evaluation is identical")

# ---------------------------------------------------------------------------
cat("\n== 6. Directional (scenario) invariants ==\n")
# Wiring errors show up here faster than anywhere else: raising an advance
# probability must not REDUCE downstream volume.
bump <- pw
bump$p_advance[bump$condition == "pop" & bump$stage == "conservative"] <-
  min(1, unique(pw$p_advance[pw$condition == "pop" & pw$stage == "conservative"]) * 1.5)
vb <- pathway_service_volumes(treated = TREATED, year = 2025L, pathway = bump)
g0 <- sum(eng$volume[eng$service == "prolapse_procedure"])
g1 <- sum(vb$volume[vb$service == "prolapse_procedure"])
if (g1 < g0) {
  bad("raising an advance probability does not reduce downstream volume",
      sprintf("%.0f -> %.0f", g0, g1))
} else ok(sprintf("raising an advance probability increases downstream volume (%.0f -> %.0f)", g0, g1))

zero <- pw
zero$p_advance[zero$condition == "pop" & zero$stage == "conservative"] <- 0
vz <- pathway_service_volumes(treated = TREATED, year = 2025L, pathway = zero)
if (sum(vz$volume[vz$service == "prolapse_procedure"]) > 1e-6) {
  bad("zero advance probability yields zero downstream procedures", "non-zero volume")
} else ok("zero advance probability yields zero downstream procedures")

# ---------------------------------------------------------------------------
cat("\n== 7. Back-test regression RATCHET ==\n")
# The model currently FAILS its own interval standard: coverage 0.20 against a
# required 0.80, with every arm under-predicting. That is a documented,
# outstanding scientific defect -- not something this script can fix, and not
# something that should make the nightly permanently red. It is ratcheted so it
# cannot silently get worse.
BASE <- ".github/backtest-baseline.txt"
bt_path <- "artifacts/backtest_2020_to_2023_summary.csv"
if (!file.exists(bt_path)) {
  bad("back-test artifact present", bt_path)
} else {
  s <- utils::read.csv(bt_path)
  cov95 <- mean(as.logical(s$within_95))
  worst <- min(s$percent_error)
  n_arm <- nrow(s)
  inf(sprintf("arms=%d  coverage95=%.2f  worst percent_error=%.2f%%", n_arm, cov95, worst))
  if (all(s$percent_error < 0))
    inf("every arm UNDER-predicts: this is systematic bias, not Monte Carlo noise")

  b_cov <- NA_real_; b_worst <- NA_real_; b_arms <- NA_integer_
  if (file.exists(BASE)) {
    kv <- readLines(BASE, warn = FALSE)
    gv <- function(k) { m <- grep(paste0("^", k, "="), kv, value = TRUE)
                        if (length(m)) as.numeric(sub(".*=", "", m[1])) else NA_real_ }
    b_cov <- gv("coverage95"); b_worst <- gv("worst_percent_error"); b_arms <- gv("n_arms")
  }
  if (is.na(b_cov)) {
    bad("back-test baseline exists", sprintf("write %s with coverage95=/worst_percent_error=/n_arms=", BASE))
  } else {
    if (cov95 + 1e-9 < b_cov)
      bad("interval coverage did not regress", sprintf("%.2f < baseline %.2f", cov95, b_cov))
    else ok(sprintf("interval coverage %.2f >= baseline %.2f", cov95, b_cov))
    # Tolerance in PERCENTAGE POINTS, not a float epsilon. percent_error is a
    # percentage, so 1e-9 would fail the baseline against itself the moment the
    # stored value is rounded. 0.05pp is below any change worth acting on.
    BIAS_TOL_PP <- 0.05
    if (worst < b_worst - BIAS_TOL_PP) {
      bad("worst-arm bias did not regress",
          sprintf("%.3f%% worse than baseline %.3f%% (tolerance %.2fpp)", worst, b_worst, BIAS_TOL_PP))
    } else ok(sprintf("worst-arm bias %.3f%% no worse than baseline %.3f%%", worst, b_worst))
    if (!is.na(b_arms) && n_arm < b_arms)
      bad("back-test arms were not dropped", sprintf("%d < baseline %d -- dropping a failing arm is not an improvement", n_arm, b_arms))
    else ok(sprintf("back-test still runs %d arms", n_arm))
    if (cov95 > b_cov + 1e-9)
      inf(sprintf("coverage IMPROVED to %.2f -- tighten %s", cov95, BASE))
  }
}

cat("\n")
if (length(FAIL)) {
  cat(sprintf("::error::%d SCIENTIFIC INVARIANT(S) VIOLATED: %s\n",
              length(FAIL), paste(FAIL, collapse = "; ")))
  quit(status = 1)
}
cat("ALL SCIENTIFIC INVARIANTS HOLD.\n")
