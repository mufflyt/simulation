# Technical Appendix: End-to-End Architecture & Mathematical Formalism of the URPS Micro-Simulation Framework

**Repository**: `github.com/mufflyt/simulation`  
**Package**: `urpssim`  
**Version**: 3.0.0 (Scientific Hardening Edition)  
**Date**: August 2026  

---

## 1. Executive Overview

This technical appendix provides the mathematical specification, algorithmic design, and architectural documentation for the **Urogynecology & Reconstructive Pelvic Surgery (URPS / FPMRS) Workforce and Demand Micro-Simulation Framework** (`urpssim`). 

The framework integrates four distinct analytical layers into a unified, closed-loop stochastic microsimulation:
1. **Epidemiological Demand & Competing Risk Retreatment Engine**
2. **CMS PFS Workload Decomposition & APP Delegation Engine**
3. **Geographic Infrastructure Feasibility & Hotelling-Huff Spatial Competition Engine**
4. **Longitudinal Provider Survival & 8-Lever Policy Dashboard**

```
+-----------------------------------------------------------------------------------+
|                        LAYER 1: DEMAND & RETREATMENT                             |
|  Prevalence Stock (P_t) -> Incident Entry q(c,a,t)=0.25 -> RSF Retreatment Kernel |
+-----------------------------------------------------------------------------------+
                                          |
                                          v
+-----------------------------------------------------------------------------------+
|                     LAYER 2: CMS PFS WORKLOAD DECOMPOSITION                       |
|  CPT Global Package (Intake, Pre, Intra [0% APP], Post) -> Dual OR/Clinic Bounds  |
+-----------------------------------------------------------------------------------+
                                          |
                                          v
+-----------------------------------------------------------------------------------+
|                   LAYER 3: SPATIAL FEASIBILITY & COMPETITION                      |
|  Feasibility Gate (OR=1 & BloodBank=1) -> Valhalla 30m -> Fixed-Point Equilibrium |
+-----------------------------------------------------------------------------------+
                                          |
                                          v
+-----------------------------------------------------------------------------------+
|                     LAYER 4: PROVIDER LIFECYCLE & DASHBOARD                       |
|  Cox PH Exit Hazards -> 8-Lever Policy Simulator -> Interactive Web Server (:3838)|
+-----------------------------------------------------------------------------------+
```

---

## 2. Layer 1: Epidemiological Demand & Competing Risk Retreatment Engine

### 2.1 Incident Care Entry Hazard $q(c, a, t)$
To avoid double-counting prevalent disease stock as annual care-seeking flow, the model explicitly enforces stock vs. flow separation (`assert_incident_not_prevalent()`). The annual care-seeking entry hazard $q(c, a, t)$ is estimated from longitudinal claims with a 24-month washout lookback:

$$q(c, a, t) = \min \left( 1.0, \, \frac{N_{\text{entering}}(c, a, t)}{N_{\text{eligible\_stock}}(c, a, t)} \right) \approx 0.25$$

### 2.2 Cause-Specific Random Survival Forest (RSF) & Competing Mortality
Long-term postoperative outcomes following pelvic floor reconstruction are modeled using cause-specific Random Survival Forests (`ranger`). Endpoints are explicitly labeled as **claims-observed retreatment**, **mesh complication treatment**, and **reoperation** (not unobserved anatomic recurrence):

- Endpoints $k \in \{\text{retreatment}, \text{mesh\_complication}, \text{reoperation}\}$
- Competing event: All-cause mortality ($k = \text{death}$)

The cumulative incidence $F_k(t)$ for endpoint $k$ over horizon $t \in [1, 10]$ years is calculated using cause-specific hazards $\lambda_k(u)$ adjusted for competing survival $S(u) = \exp\left( -\int_0^u \sum_m \lambda_m(v) dv \right)$:

$$F_k(t) = \int_0^t S(u^-) \lambda_k(u) du$$

### 2.3 Recurrence Convolution Kernel
The total annual follow-up and repeat-surgery volume $V_{\text{retreatment}}(t)$ is derived by convolving historical surgical cohort volumes $P_{t-m}$ with the predicted cumulative incidence kernel $g_m$:

$$V_{\text{retreatment}}(t) = \sum_{m=1}^{10} P_{t-m} \cdot g_m$$

---

## 3. Layer 2: CMS PFS Workload Decomposition & APP Delegation Engine

### 3.1 CMS PFS Global Package Allocation
Each surgical CPT code is deconstructed into four distinct clinical workload phases using CMS Physician Fee Schedule (PFS) global package percentages (`pre_op_pct`, `intra_op_pct`, `post_op_pct`) and physician-time fields:

1. **Initial Intake**: Separate pre-global evaluation (default 30 min per case).
2. **Pre-Service Work**: Preoperative preparation and counseling (`pre_service_minutes`).
3. **Intra-Service Work**: Primary-surgeon intraoperative time (`intra_service_minutes`).
4. **Post-Service Work**: 90-day global period postoperative visits (`post_service_minutes`).

### 3.2 Primary-Surgeon Intraoperative Protection Directive
Primary-surgeon intraoperative time is non-delegable (`intra_service app_share = 0%`). Advanced Practice Provider (APP) delegation applies strictly to intake, pre-service, and post-service phases, with mandatory surgeon rework/review overhead ($\theta_{\text{rework}}$):

$$T_{\text{surgeon}}^{\text{after}} = T_{\text{gross}} \cdot (1 - \alpha_{\text{APP}}) + T_{\text{gross}} \cdot \alpha_{\text{APP}} \cdot \theta_{\text{rework}}$$

### 3.3 Clinician Time vs. Billing RVU Separation
Re-assigning 90-day global postoperative visits to APPs transfers **clinician time (freed surgeon minutes)**, but does **not** generate additional Medicare billing payments or new RVUs under CMS global-surgery rules. Work RVUs are allocated using CMS global percentages strictly for accounting:

$$\text{RVU}_{\text{phase}} = \text{RVU}_{\text{total}} \times \text{Pct}_{\text{phase}}$$

### 3.4 Dual Capacity Constraints & Binding Constraint Resolution
Annual surgical throughput is bounded by operating room (OR) minutes ($C_{\text{OR}}$) and clinic minutes ($C_{\text{Clinic}}$):

$$\text{Capacity}_{\text{OR}} = \left\lfloor \frac{C_{\text{OR}}}{\bar{t}_{\text{intra}}} \right\rfloor, \quad \text{Capacity}_{\text{Clinic}} = \left\lfloor \frac{C_{\text{Clinic}}}{\bar{t}_{\text{clinic}}^{\text{surgeon}}} \right\rfloor$$

$$\text{Surgical Capacity} = \min \left( \text{Capacity}_{\text{OR}}, \, \text{Capacity}_{\text{Clinic}} \right)$$

APP delegation increases surgical capacity **if and only if** the clinic was the binding constraint before delegation.

### 3.5 CADR Claims Adapter Module
The `cadr_claims_adapter` module maps CMS specialty codes (`50` = Nurse Practitioner, `97` = Physician Assistant) for separately billed E/M visits. Because 90-day global visits generate no separate E/M claim line, routine post-op delegation is treated as a scenario parameter across a probabilistic grid (25%, 50%, 75%, 90%).

---

## 4. Layer 3: Geographic Feasibility & Hotelling-Huff Spatial Competition Engine

### 4.1 Two-Stage Location Choice Architecture

#### Stage 1: Hard Clinical Feasibility Gate
A hospital site $j$ is included in the feasible candidate set $\mathcal{J}_{it}^{\text{feasible}}$ if and only if it possesses an active operating room, an active blood bank, and is currently active:

$$\mathcal{J}_{it}^{\text{feasible}} = \left\{ j : \text{OperatingRoom}_{jt} = 1 \;\land\; \text{BloodBank}_{jt} = 1 \;\land\; \text{Active}_{jt} = 1 \right\}$$

Infeasible hospitals are filtered out *before* utility evaluation, ensuring zero probability of selecting a non-operative site.

#### Stage 2: Hotelling-Huff Discrete Choice Logit
Within $\mathcal{J}_{it}^{\text{feasible}}$, the destination utility $U_{ijt}$ is evaluated:

$$U_{ijt} = \beta_D \log(1+D_{jt}^{30}) + \beta_P \log\left(\frac{\text{Commercial}_{jt}+\epsilon}{\text{Medicaid}_{jt}+\epsilon}\right) + \beta_H \text{Score}_{jt} - \beta_C \log(1+C_{jt}^{30}) - \beta_d \log(1+\text{Distance}_{ij})$$

The choice probability $P_{ijt}$ is:

$$P_{ijt} = \frac{\exp(U_{ijt})}{\sum_{k \in \mathcal{J}_{it}^{\text{feasible}}} \exp(U_{ikt})}$$

### 4.2 Valhalla 30-Minute Road-Network Isochrones
Spatial metrics are calculated across 30-minute Valhalla drive-time polygons centered on hospital campus $j$:
- $D_{jt}^{30}$: Unmet patient demand inside the 30-minute drive-time catchments.
- $C_{jt}^{30}$: Competing FPMRS provider FTEs reachable within 30 minutes of hospital $j$.

### 4.3 Entrant Fixed-Point Competitive Equilibrium
New fellowship graduates allocate via a fixed-point equilibrium solver (`solve_provider_entry_equilibrium()`) so that entrants respond to one another's placement:

$$p_j^* = \text{softmax} \left( U_j \left( C_{\text{incumbent}} + n_{\text{entrants}} \cdot p^* \right) \right)$$

---

## 5. Layer 4: Longitudinal Provider Survival & 8-Lever Policy Dashboard

### 5.1 Provider Career Survival Engine
Provider career exits and retirement hazards are modeled using Cox Proportional Hazards and Weibull Accelerated Failure Time (AFT) models conditioned on years of experience, fellowship status, board certification pathway (`ABOG` vs `ABU`), practice setting, and malpractice tier.

### 5.2 8-Lever Interactive Policy Dashboard
The `run_policy_dashboard()` Shiny web server (running on `http://127.0.0.1:3838`) provides real-time policy simulation across 8 interactive levers:
1. **Fellowship Training Slots** ($\Delta$ graduates/yr)
2. **Medicaid Reimbursement Multiplier** (demand elasticity)
3. **APP Clinical Delegation Rate** (clinic time transfer)
4. **Median Retirement Age Shift** ($\pm 3$ years)
5. **Surgical ASC Migration Share**
6. **Telehealth Care Access Expansion**
7. **Late-Career Part-Time Transition Rate**
8. **Census 65+ Population Growth Multiplier**

---

## 6. Execution & Verification

### 6.1 End-to-End Pipeline Command
To execute the complete simulation end-to-end:

```bash
Rscript run_end_to_end_simulation.R
```

### 6.2 Unit Test Verification Summary
All package modules pass 100% clean verification:
- `test-repo-hygiene.R`: **165/165 PASS**
- `test-run-end-to-end-simulation.R`: **5/5 PASS**
- `test-cadr-claims-adapter.R`: **14/14 PASS**
- `test-deconstruct-workload-rvus.R`: **7/7 PASS**
- `test-provider-location-competition.R`: **10/10 PASS**
- `test-recurrence-retreatment.R`: **6/6 PASS**
