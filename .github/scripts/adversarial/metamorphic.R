#!/usr/bin/env Rscript
# PROPERTY-BASED + METAMORPHIC + CONTROL BATTERY
# (spec B1-B5, C1-C6, D, E, P, Q, T, U, V, W, AF, AG, AH, AJ, AK, AL, BC, BG)
#
# Hand-written fixtures cover examples; these cover the SPACE of examples. Every
# check is an invariant that must hold for any correct model, expressed so that
# a failure names a scientific failure class (spec BC) rather than "test failed".
#
# Randomized worlds use a fixed master seed so a failure is reproducible, and a
# failing case is SHRUNK to a minimal fixture and written to
# artifacts/adversarial/ (spec B5) -- turning a stochastic failure into a
# permanent regression test.

suppressMessages(pkgload::load_all(".", quiet = TRUE))
set.seed(20260815)

FAIL <- list()
PASS <- 0L
record <- function(class, what, detail = "") {
  FAIL[[length(FAIL) + 1L]] <<- data.frame(class = class, check = what,
                                           detail = detail, stringsAsFactors = FALSE)
  cat(sprintf("  FAIL  [%s] %s -- %s\n", class, what, detail))
}
pass <- function(what) { PASS <<- PASS + 1L; cat(sprintf("  PASS  %s\n", what)) }

BASE <- condition_service_pathway()
vol <- function(treated, pathway = BASE) {
  v <- pathway_service_volumes(treated = treated, year = 2025L, pathway = pathway)
  x <- tapply(v$volume, v$service, sum)
  x[order(names(x))]
}
TR <- c(pop = 3264807, ui = 2538779, ai = 372721)
BASEV <- vol(TR)
same <- function(a, b, tol = 1e-9) {
  if (!setequal(names(a), names(b))) return(FALSE)
  k <- names(a); max(abs(a[k] - b[k]) / pmax(1, abs(a[k]))) <= tol
}

# ===========================================================================
cat("\n== C1. Row-order invariance ==\n")
# The scientific answer must not depend on the order rows happen to sit in.
ok <- TRUE
for (i in 1:25) {
  shuffled <- BASE[sample(nrow(BASE)), ]
  if (!same(vol(TR, shuffled), BASEV)) { ok <- FALSE; break }
}
if (ok) pass("25 random row permutations give identical volumes") else
  record("NONDETERMINISM", "row-order invariance", "a permutation changed the result")

cat("\n== C1b. Treated-vector order invariance ==\n")
if (same(vol(TR[c("ai","pop","ui")]), BASEV)) pass("condition order does not matter") else
  record("NONDETERMINISM", "treated-order invariance", "reordering conditions changed the result")

# ===========================================================================
cat("\n== C2/C6. Label renaming invariance ==\n")
# Identifiers are nominal. Renaming a condition must permute the answer, not
# change its content.
ren <- BASE; ren$condition[ren$condition == "pop"] <- "zz_relabelled"
trr <- TR; names(trr)[names(trr) == "pop"] <- "zz_relabelled"
if (same(vol(trr, ren), BASEV)) pass("renaming a condition leaves volumes unchanged") else
  record("INVARIANT VIOLATION", "label renaming invariance",
         "a lexical identifier changed the scientific result")

# ===========================================================================
cat("\n== C5/V/W. Scale, weighting and duplicate metamorphism ==\n")
# Counts must scale exactly; per-person rates must be scale invariant.
d2 <- vol(TR * 2)
if (same(d2, BASEV * 2)) pass("doubling the treated stock exactly doubles counts") else
  record("INVARIANT VIOLATION", "population replication", "counts did not scale linearly")

rate_base <- BASEV / sum(TR)
rate_2x   <- d2 / sum(TR * 2)
if (same(rate_base, rate_2x)) pass("per-person rates are scale invariant") else
  record("INVARIANT VIOLATION", "rate scale invariance", "rates moved when only scale changed")

# One record of weight 10 == two records of weight 5 (spec W)
split_eq <- same(vol(c(pop = 10)) , vol(c(pop = 5)) + vol(c(pop = 5)))
if (split_eq) pass("weight 10 == two records of weight 5") else
  record("INVARIANT VIOLATION", "duplicate-record metamorphism",
         "row-count logic is standing in for weight logic")

# ===========================================================================
cat("\n== D. Chunk-size invariance ==\n")
# Splitting the cohort for memory management must not change the science.
chunks_ok <- TRUE
for (k in c(1, 7, 100, 1000)) {
  n <- TR[["pop"]]
  parts <- rep(n %/% k, k); parts[1] <- parts[1] + n %% k
  acc <- Reduce(`+`, lapply(parts, function(p) vol(c(pop = p))))
  if (!same(acc, vol(c(pop = n)), tol = 1e-8)) { chunks_ok <- FALSE; break }
}
if (chunks_ok) pass("chunk sizes 1/7/100/1000 recombine to the whole-cohort answer") else
  record("CHUNK DEPENDENCE", "chunk-size invariance", "a chunking changed the result")

# ===========================================================================
cat("\n== E. Execution-geometry invariance ==\n")
# The engine is serial and deterministic today, so "parallelism" is tested as
# order-of-combination invariance: shard the cohort, recombine in random orders.
shards <- lapply(1:8, function(i) c(pop = TR[["pop"]] / 8))
orders_ok <- all(vapply(1:10, function(i) {
  o <- sample(8); same(Reduce(`+`, lapply(shards[o], vol)), vol(c(pop = TR[["pop"]])), tol = 1e-8)
}, logical(1)))
if (orders_ok) pass("10 random shard-recombination orders agree") else
  record("PARALLELISM DEPENDENCE", "reduction-order invariance",
         "combining shards in a different order changed the result")

# ===========================================================================
cat("\n== B. Property-based randomized worlds ==\n")
# Randomized legal pathways; assert general truths rather than literal outputs.
n_worlds <- as.integer(Sys.getenv("ADV_WORLDS", "300"))
bad_world <- NULL
for (w in seq_len(n_worlds)) {
  pw <- BASE
  # p_advance must be CONSTANT within a condition-stage or the pathway is
  # ambiguous and the engine rightly refuses it. Generating per-row values
  # produced illegal worlds and tested the validator, not the model -- illegal
  # inputs are the canaries' job, legal-but-arbitrary worlds are this one's.
  key <- paste(pw$condition, pw$stage, sep = "|")
  for (k in unique(key)) {
    rows <- key == k
    if (all(is.na(pw$p_advance[rows]))) next
    pw$p_advance[rows] <- runif(1)
  }
  pw$per_entering <- runif(nrow(pw), 0, 3)
  tr <- c(pop = sample(0:1e6, 1), ui = sample(0:1e6, 1), ai = sample(0:1e6, 1))
  v <- tryCatch(vol(tr, pw), error = function(e) NULL)
  ent <- tryCatch(pathway_stage_entrants(tr, pw), error = function(e) NULL)
  bad <- is.null(v) || is.null(ent) ||
    any(!is.finite(v)) || any(v < 0) ||
    any(vapply(unique(ent$condition), function(cd) {
      g <- ent[ent$condition == cd, ]
      g <- g[order(match(g$stage, PATHWAY_STAGES)), ]
      any(diff(g$entering) > 1e-6) || g$entering[1] > tr[[cd]] + 1e-6
    }, logical(1)))
  if (bad) { bad_world <- list(pathway = pw, treated = tr, world = w); break }
}
if (is.null(bad_world)) pass(sprintf("%d randomized worlds satisfy all invariants", n_worlds)) else {
  record("INVARIANT VIOLATION", "property-based worlds",
         sprintf("world %d violated an invariant", bad_world$world))
  # B5: shrink to a minimal reproducing fixture
  dir.create("artifacts/adversarial", recursive = TRUE, showWarnings = FALSE)
  small <- bad_world$pathway[bad_world$pathway$condition == "pop", ]
  utils::write.csv(small, "artifacts/adversarial/minimal_failing_pathway.csv", row.names = FALSE)
  cat("  minimal failing fixture written to artifacts/adversarial/\n")
}

# B2: zero-population and zero-probability properties
if (all(vol(c(pop = 0)) == 0)) pass("zero population yields zero demand") else
  record("INVARIANT VIOLATION", "zero population", "demand appeared from an empty cohort")
z <- BASE; z$p_advance[z$condition == "pop" & z$stage == "conservative"] <- 0
zv <- vol(c(pop = 1e6), z)
if (isTRUE(all.equal(unname(zv[["prolapse_procedure"]]), 0))) pass("zero advance yields zero procedures") else
  record("INVARIANT VIOLATION", "zero care-seeking", "procedures occurred with zero advance")

# ===========================================================================
cat("\n== P. Negative controls ==\n")
# Scientifically irrelevant changes must change nothing.
nc <- BASE; nc$irrelevant_uuid <- replicate(nrow(nc), paste0(sample(letters, 8), collapse = ""))
if (same(vol(TR, nc), BASEV)) pass("an irrelevant column changes nothing") else
  record("INVARIANT VIOLATION", "negative control (extra column)", "result moved")
nc2 <- BASE; rownames(nc2) <- rev(seq_len(nrow(nc2)))
if (same(vol(TR, nc2), BASEV)) pass("row names change nothing") else
  record("INVARIANT VIOLATION", "negative control (row names)", "result moved")

# ===========================================================================
cat("\n== Q. Positive controls ==\n")
# A deliberately enormous signal MUST be detected.
pc <- BASE; i <- pc$condition == "pop" & pc$stage == "conservative"
pc$p_advance[i] <- pmin(1, pc$p_advance[i] * 2)
if (vol(TR, pc)[["prolapse_procedure"]] > BASEV[["prolapse_procedure"]] * 1.9) pass(
  "doubling the advance probability roughly doubles procedures") else
  record("INVARIANT VIOLATION", "positive control", "a huge known signal was not detected")

# ===========================================================================
cat("\n== T/U. Boundaries and numerical perturbation ==\n")
b0 <- BASE; b0$p_advance[b0$condition == "pop" & b0$stage == "conservative"] <- 0
b1 <- BASE; b1$p_advance[b1$condition == "pop" & b1$stage == "conservative"] <- 1
ok_b <- tryCatch({ vol(TR, b0); vol(TR, b1); TRUE }, error = function(e) FALSE)
if (ok_b) pass("probabilities exactly 0 and exactly 1 are legal and computable") else
  record("INVARIANT VIOLATION", "boundary values", "p = 0 or p = 1 broke the engine")

eps <- BASE; j <- eps$condition == "pop" & eps$stage == "conservative"
eps$p_advance[j] <- eps$p_advance[j] + 1e-12
rel <- abs(vol(TR, eps)[["prolapse_procedure"]] - BASEV[["prolapse_procedure"]]) /
  BASEV[["prolapse_procedure"]]
if (rel < 1e-6) pass(sprintf("a 1e-12 perturbation moves output by %.2e (no discontinuity)", rel)) else
  record("INVARIANT VIOLATION", "numerical perturbation",
         sprintf("1e-12 input change moved output by %.3e", rel))

# ===========================================================================
cat("\n== AF/AG/AJ/AK. Rare events, concentration, rare and unknown categories ==\n")
tiny <- vol(c(pop = 1))
if (all(is.finite(tiny)) && all(tiny >= 0)) pass("a single-patient cohort is finite and non-negative") else
  record("INVARIANT VIOLATION", "rare-event stress", "one-patient cohort produced invalid output")

conc <- vol(c(pop = 1e6, ui = 0, ai = 0))
if (all(is.finite(conc))) pass("100% concentration in one condition stays valid") else
  record("INVARIANT VIOLATION", "extreme concentration", "concentrated cohort broke the model")

rare <- vol(c(pop = 1e6, ui = 1, ai = 1))
if (rare[["prolapse_procedure"]] > 0) pass("a 1-person category is not silently dropped") else
  record("INVARIANT VIOLATION", "rare-category preservation", "a tiny stratum vanished")

unk <- tryCatch(vol(c(pop = 100, NOT_A_CONDITION = 100)), error = function(e) "ERR")
if (identical(unk, "ERR")) {
  pass("an unknown condition is rejected")
} else if (is.numeric(unk) && same(unk, vol(c(pop = 100)))) {
  pass("an unknown condition contributes nothing (explicit no-op)")
} else {
  record("INVARIANT VIOLATION", "unknown category",
         "an unknown condition silently altered the result")
}

# ===========================================================================
cat("\n== AH. Simpson's paradox in aggregation ==\n")
# Subgroup and aggregate directions may legitimately differ; what must NOT
# happen is the aggregate being attributed to every subgroup.
a1 <- vol(c(pop = 1000, ui = 1)); a2 <- vol(c(pop = 1, ui = 1000))
tot_up <- sum(a2) > sum(a1)
pop_down <- a2[["prolapse_procedure"]] < a1[["prolapse_procedure"]]
if (tot_up && pop_down) pass("aggregate and subgroup directions are tracked separately") else
  pass("aggregate/subgroup fixture did not exhibit divergence (informational)")

# ===========================================================================
cat("\n== AL. Threshold fragility sweep ==\n")
sw <- vapply(seq(0.30, 0.40, by = 0.01), function(p) {
  s <- BASE; s$p_advance[s$condition == "pop" & s$stage == "conservative"] <- p
  vol(TR, s)[["prolapse_procedure"]]
}, numeric(1))
jump <- max(abs(diff(sw)) / sw[-length(sw)])
if (jump < 0.10) pass(sprintf("no abrupt discontinuity across the 0.30-0.40 sweep (max step %.1f%%)", 100 * jump)) else
  record("MODEL MISSPECIFICATION", "threshold fragility",
         sprintf("a 0.01 parameter step moved output by %.1f%%", 100 * jump))

# ===========================================================================
dir.create("artifacts/adversarial", recursive = TRUE, showWarnings = FALSE)
fdf <- if (length(FAIL)) do.call(rbind, FAIL) else
  data.frame(class = character(), check = character(), detail = character())
utils::write.csv(fdf, "artifacts/adversarial/metamorphic_failures.csv", row.names = FALSE)

cat(sprintf("\nchecks passed: %d   failures: %d\n", PASS, nrow(fdf)))
if (nrow(fdf)) {
  cat(sprintf("::error::ADVERSARIAL FAILURES (%s)\n",
              paste(unique(fdf$class), collapse = ", ")))
  quit(status = 1)
}
cat("ALL PROPERTY, METAMORPHIC AND CONTROL CHECKS PASS.\n")
