#!/usr/bin/env Rscript
# ============================================================================
# FI TRANSPORTABILITY AUDIT + prespecified transport scenarios (A/B/C).
# The frozen dual-source base case (NHANES baseline + NHS transition kernel) drives
# nationally-initialized younger-old cohorts DOWN for ~5y (NHS equilibrium < national
# baseline). This quantifies that as TRANSITION-TRANSPORTABILITY uncertainty rather
# than declaring NHS onset "too low" or silently recalibrating.
#
#   A. EMPIRICAL BASE CASE     : NHANES init + UNMODIFIED NHS kernel (evidence-direct)
#   B. COVARIATE-TRANSPORTED   : standardize NHS incidence to national covariates
#                                (only if identifiable from published effects)
#   C. NATIONAL-STATIONARITY   : scalar k on onset s.t. national baseline has ~zero
#                                drift at t0  -- a STRUCTURAL BOUND, not a fitted value
#                                (transport_sensitivity = national_stationarity_assumption)
# r stays model_identified_from_NHS (NHANES validates prevalence, not remission).
# Reads data-raw/nhanes/nhanes_fi_endpoint.rds. No DMDM contract / manuscript writes.
# ============================================================================
suppressPackageStartupMessages({library(here)})
EP <- readRDS(here("data-raw","nhanes","nhanes_fi_endpoint.rds"))

## ---- frozen NHS kernel (model B) -------------------------------------------
bands<-c("62-64","65-69","70-74","75-79","80-84","85-87")
b_lo<-c(62,65,70,75,80,85); b_hi<-c(64,69,74,79,84,87)
prevN<-c(0.090,0.104,0.102,0.128,0.146,0.170); N_tot<-64559
share<-c(0.18,0.28,0.24,0.17,0.09,0.04); n_band<-round(share*N_tot); y_band<-round(n_band*prevN)
INC_EVENTS<-5954; INC_PY<-175447; ages<-62:100
dens<-approx((b_lo+b_hi)/2,share/(b_hi-b_lo+1),xout=62:87,rule=2)$y; dens<-c(dens,rep(dens[26],length(ages)-26))
bof<-pmin(pmax(findInterval(ages,b_lo),1),6); suscept<-dens*(1-prevN[bof]); suscept<-suscept/sum(suscept)
sim<-function(p0,b0,b1,r){inc<-1-exp(-exp(b0+b1*(ages-73)/10));p<-numeric(length(ages));p[1]<-p0
  for(i in 2:length(ages)) p[i]<-p[i-1]*(1-r)+(1-p[i-1])*inc[i-1];list(p=p,inc=inc)}
bp<-function(p) vapply(seq_along(bands),function(k) mean(p[b_lo[k]<=ages&ages<=b_hi[k]]),numeric(1))
nll<-function(th){m<-sim(plogis(th[1]),th[2],th[3],plogis(th[4]));pb<-pmin(pmax(bp(m$p),1e-9),1-1e-9)
  -(sum(dbinom(y_band,n_band,pb,log=TRUE))+dpois(INC_EVENTS,sum(suscept*m$inc)*INC_PY,log=TRUE))}
o<-optim(c(qlogis(.09),log(-log(.97)),.3,qlogis(.2)),nll,method="Nelder-Mead",control=list(maxit=9000,reltol=1e-12))
o<-optim(o$par,nll,method="Nelder-Mead",control=list(maxit=9000,reltol=1e-12))
B0<-o$par[2];B1<-o$par[3];R_NHS<-plogis(o$par[4])
onset0<-function(a) 1-exp(-exp(B0+B1*(a-73)/10))    # unmodified NHS onset

## ---- national baseline initializer (NHANES) --------------------------------
fine<-EP$fi_nhs_fine
base_prev<-function(age){age<-pmin(age,89)
  b<-as.character(cut(age,c(0,60,65,70,75,80,Inf),right=FALSE,
    labels=c("<60","60-64","65-69","70-74","75-79","80+")))
  pv<-setNames(fine$prev,fine$band); as.numeric(ifelse(b=="<60",NA,pv[b]))}
AGE<-EP$age_wt; uage<-AGE$RIDAGEYR; uw<-AGE$adj_weight/sum(AGE$adj_weight)

## ============ (1-2) TRANSPORTABILITY AUDIT ==================================
cat("=== TRANSPORTABILITY AUDIT: NHS incidence covariates -> NHANES 2005-2010 ===\n")
cat(sprintf("%-16s %-22s %-10s %-12s %-10s\n","covariate","NHS incidence effect","in NHANES?","measurement","usable?"))
aud<-function(...) cat(sprintf("%-16s %-22s %-10s %-12s %-10s\n",...))
aud("age","in onset(a)","yes","comparable","in model")
aud("physical activity","HR .86/.78/.76/.75","yes(PAQ)","NOT comparable","illustrative")
aud("BMI","null (ptrend .71)","yes","comparable","not useful")
aud("diabetes","OR 1.43 (x-sec only)","yes(DIQ)","comparable","no inc HR")
aud("smoking","adjusted, HR NP","yes(SMQ)","comparable","no inc HR")
aud("parity","adjusted, HR NP","yes(RHQ)","comparable","no inc HR")
aud("neurologic dis","OR 1.84 (x-sec,dual)","limited","poor","no")
aud("race/ethnicity","NHS ~all white","yes","comparable","NO positivity")
aud("MHT/HTN/cholecyst","adjusted, HR NP","partial","mixed","no inc HR")
cat("\nFEASIBILITY of covariate standardization i_target(a)=sum_x i_NHS(a,x)P_NHANES(x|a):\n")
cat("  Only PHYSICAL ACTIVITY has a published incidence-HR gradient, but its NHS metric\n")
cat("  (leisure MET-hrs/wk) is NOT comparably measured in NHANES (PAQ work+leisure), and\n")
cat("  the published NHS PA *distribution* by category is not in the aggregate. Race lacks\n")
cat("  NHS positivity. => FULL covariate standardization is NOT identifiable from published\n")
cat("  aggregate data. Scenario B is at best an ILLUSTRATIVE single-covariate direction\n")
cat("  (national women less active than nurses => higher incidence => k>1, same sign as C).\n")

## ============ (3-4) SCENARIO C: national-stationarity multiplier k ==========
# zero aggregate drift at t0: sum_a w(a)[(1-P0(a)) k onset0(a) - P0(a) r] = 0
w_by_age<-tapply(uw, uage, sum); a_grid<-as.integer(names(w_by_age))
P0<-base_prev(a_grid); i0<-onset0(a_grid)
outflow<-sum(w_by_age*P0*R_NHS); inflow0<-sum(w_by_age*(1-P0)*i0)
k_star<-outflow/inflow0
cat(sprintf("\n=== SCENARIO C: national-stationarity multiplier ===\n"))
cat(sprintf("  t0 aggregate outflow (P*r) = %.4f ; unmodified inflow ((1-P)*i) = %.4f\n",outflow,inflow0))
cat(sprintf("  k* (onset multiplier for zero t0 drift) = %.2f\n", k_star))
cat(sprintf("  onset@65: NHS %.4f -> C %.4f ; onset@85: NHS %.4f -> C %.4f (still <0.10/yr: %s)\n",
    onset0(65),k_star*onset0(65),onset0(85),k_star*onset0(85), all(k_star*onset0(a_grid)<0.10)))
cat("  label: transport_sensitivity = national_stationarity_assumption (STRUCTURAL BOUND, not fitted)\n")

## ============ (5) COHORT DIAGNOSTIC under A and C ===========================
onset_scen<-function(a,k) k*onset0(a)
evolve_prev<-function(p0,a0,h,k){P<-p0;a<-a0;for(t in seq_len(h)){P<-P*(1-R_NHS)+(1-P)*onset_scen(a,k);a<-a+1};P}
agg_traj<-function(k) sapply(c(0,1,5,10,20), function(h) sum(w_by_age*mapply(function(a) evolve_prev(base_prev(a),a,h,k), a_grid)))
cat("\n=== national CLOSED-cohort aggregate prevalence (A=unmodified, C=stationarity) ===\n")
cat(sprintf("%-10s %6s %6s %6s %6s %6s\n","scenario","t0","+1y","+5y","+10y","+20y"))
tA<-agg_traj(1); tC<-agg_traj(k_star)
cat(sprintf("%-10s %6.3f %6.3f %6.3f %6.3f %6.3f\n","A (k=1)", tA[1],tA[2],tA[3],tA[4],tA[5]))
cat(sprintf("%-10s %6.3f %6.3f %6.3f %6.3f %6.3f\n",sprintf("C (k=%.2f)",k_star), tC[1],tC[2],tC[3],tC[4],tC[5]))

cat("\n=== starting-age cohort trajectories (P0 -> +5y -> +20y) A vs C ===\n")
cat(sprintf("%-9s | %-20s | %-20s\n","cohort","A (k=1)","C (k=stationarity)"))
for(nm in c("60-64","65-69","70-74","75-79")){a0<-c("60-64"=62,"65-69"=67,"70-74"=72,"75-79"=77)[nm];p0<-base_prev(a0)
  A5<-evolve_prev(p0,a0,5,1);A20<-evolve_prev(p0,a0,20,1);C5<-evolve_prev(p0,a0,5,k_star);C20<-evolve_prev(p0,a0,20,k_star)
  cat(sprintf("%-9s | %.3f ->%.3f ->%.3f | %.3f ->%.3f ->%.3f\n",nm,p0,A5,A20,p0,C5,C20))}

## ============ (6) structural transport uncertainty =========================
a20<-agg_traj(1); c20<-agg_traj(k_star)
cat(sprintf("\n=== (6) STRUCTURAL TRANSPORT UNCERTAINTY (A vs C envelope) ===\n"))
cat(sprintf("  +5y aggregate:  A=%.3f  C=%.3f  (spread %.3f)\n", a20[3],c20[3],abs(c20[3]-a20[3])))
cat(sprintf("  +20y aggregate: A=%.3f  C=%.3f  (spread %.3f)\n", a20[5],c20[5],abs(c20[5]-a20[5])))
cat("  Carry the A..C envelope as structural uncertainty into the workforce demand.\n")
cat("  If workforce conclusions are stable across A..C, the transport ambiguity need not\n")
cat("  be resolved; if not, it is decision-relevant (=> pursue NHS individual-level data).\n")
cat("  B (covariate transport) not quantified: not identifiable from published aggregate.\n")
