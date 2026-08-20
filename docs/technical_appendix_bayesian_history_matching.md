# Technical Appendix: High-Dimensional Bayesian History Matching & Gaussian Process Emulation

**Repository**: `github.com/mufflyt/simulation`  
**Package**: `urpssim`  
**Module**: [R/calibration-bayesian_history_matching.R](file:///Users/tmuffly/simulation/R/calibration-bayesian_history_matching.R)  
**Version**: 3.0.0 (Scientific Hardening Edition)  
**Date**: August 2026  

---

## 1. Executive Overview

This technical appendix details the statistical model, algorithmic workflow, and computational implementation of the **Iterative Bayesian History Matching and Gaussian Process (GP) Emulation Engine** (`calibrate_bayesian_history_matching()`).

The engine calibrates high-dimensional (45+ parameter) stochastic microsimulations against historical benchmarks (e.g., MEPS national care-seeking estimates, CMS PSPS procedural volumes from 2015–2024) without requiring exhaustive or intractable Monte Carlo grid searches.

```
+-----------------------------------------------------------------------------------+
|               PHASE 1: GP EMULATION & ITERATIVE HISTORY MATCHING                  |
|  LHS Sampling (450+ settings) -> Matérn 5/2 GP Emulators -> Implausibility I(x)<=3|
+-----------------------------------------------------------------------------------+
                                          |
                                          v (NROY Region)
+-----------------------------------------------------------------------------------+
|               PHASE 2: JOINT BAYESIAN POSTERIOR IMPORTANCE SAMPLING              |
|  Likelihood over NROY Space -> Joint Parameter Posterior Draws (posterior_draws)  |
+-----------------------------------------------------------------------------------+
                                          |
                                          v
+-----------------------------------------------------------------------------------+
|            PHASE 3: FULL-SIMULATOR 2025-2050 POSTERIOR PREDICTIVE PROJECTIONS     |
|  Full Simulator Runs -> Annual Means +/- SD & Medians (p25, p75) -> CSV Exports   |
+-----------------------------------------------------------------------------------+
```

---

## 2. Phase 1: GP Emulation & Implausibility Filtering

### 2.1 Implausibility Metric $I_j(x)$
For historical calibration target $j$ (e.g. MEPS UI visits or CMS PSPS sling volume in 2015) and candidate parameter vector $x \in \mathbb{R}^p$:

$$I_j(x) = \frac{\left| m_j(x) - z_j \right|}{\sqrt{s_j^2(x) + \sigma_{e,j}^2 + \sigma_{\delta,j}^2}}$$

where:
- $m_j(x)$: GP emulator predicted mean output.
- $s_j^2(x)$: GP emulator variance (emulation uncertainty).
- $z_j$: Historical benchmark observation (MEPS or CMS PSPS).
- $\sigma_{e,j}$: Benchmark sampling uncertainty (`observation_se`).
- $\sigma_{\delta,j}$: Model discrepancy uncertainty (`discrepancy_sd`).

### 2.2 Ranked Cutoff & Non-Implausible Region ($\mathcal{X}_{\text{NROY}}$)
Parameter settings where $I_j(x) > 3.0$ are excluded. To prevent over-restriction when evaluating many correlated historical targets, the cutoff uses the $k$-th maximum implausibility (default rank $k=1$ or $k=2$):

$$\mathcal{X}_{\text{NROY}} = \left\{ x \in \mathcal{X} : I_{(k)}(x) \le 3.0 \right\}$$

### 2.3 Refocused Iterative Waves
Across $N_{\text{waves}}$ (default 4), the parameter domain is refocused onto the bounding hyperbox of $\mathcal{X}_{\text{NROY}}$:

$$x_p^{\text{lower, wave}+1} = \min_{x \in \mathcal{X}_{\text{NROY}}} x_p, \quad x_p^{\text{upper, wave}+1} = \max_{x \in \mathcal{X}_{\text{NROY}}} x_p$$

---

## 3. Phase 2: Joint Bayesian Posterior Sampling

History matching identifies the non-implausible region $\mathcal{X}_{\text{NROY}}$, but does not itself produce a probability density. Phase 2 constructs the joint parameter posterior distribution within $\mathcal{X}_{\text{NROY}}$:

$$p(x \mid z, x \in \mathcal{X}_{\text{NROY}}) \propto p(x) \times \mathcal{N}\left( z; \, m(x), \, \Sigma_{\text{benchmark}} + \Sigma_{\text{GP}}(x) \right)$$

Joint parameter draws are generated via importance sampling over $\mathcal{X}_{\text{NROY}}$, preserving non-linear parameter correlations (e.g., care-seeking rate vs. provider exit hazards).

---

## 4. Phase 3: Full-Simulator 2025–2050 Posterior Predictive Projections

For each posterior parameter draw $x^{(d)} \sim p(x \mid z)$, the full stochastic microsimulation engine `workforce_simulator(x^{(d)}, \text{years}=2025:2050)` is executed.

Posterior predictive summaries report:
- **Central Tendency**: Mean and Median.
- **Uncertainty Bounds**: Standard Deviation (SD), 25th percentile (p25), and 75th percentile (p75).

---

## 5. Identification & CMS Transportability Constraints

1. **Parameter Identifiability**: Historical MEPS and CMS PSPS targets cannot independently identify all 45+ parameters. Informative priors are retained for unidentifiable parameters.
2. **CMS FFS Transportability Discrepancy**: CMS PSPS reflects Medicare FFS (5% sample). It must not be treated as a national all-payer target without an explicit transportability discrepancy term $\sigma_{\delta, \text{CMS}}$:

$$\sigma_{\delta, \text{CMS}}^2 = \sigma_{\text{FFS\_to\_AllPayer}}^2 + \sigma_{\text{Coding}}^2$$

---

## 6. Execution Command

```r
library(urpssim)

inputs <- load_default_history_matching_inputs()
results <- calibrate_bayesian_history_matching(
  parameter_spec = inputs$parameter_spec,
  benchmark_table = inputs$benchmark_table,
  workforce_simulator = my_workforce_simulator,
  n_waves = 4L,
  initial_samples = 450L,
  implausibility_cutoff = 3.0,
  n_posterior_draws = 200L,
  save_directory = "artifacts/calibration"
)
```
