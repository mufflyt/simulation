#!/usr/bin/env Rscript
# ============================================================================
# FI natural-history architecture (option 5): SEPARATE national baseline state
# (NHANES) from longitudinal transitions (frozen NHS kernel), then evolve real
# cohorts and check plausibility BEFORE reopening any onset/remission structure.
#
#   C. Baseline initializer  : P(FI at t0 | age) from NHANES 2005-2010 (national).
#   D. Transition kernel      : frozen NHS model B onset i(a) + constant remission r
#                               (r is model_identified_from_NHS; NOT validated by NHANES).
#   E. Cohort diagnostic      : initialize national women, apply the SAME kernel,
#                               report prevalence at +0/1/5/10/20y + per-cohort tracks.
#   F. Gate                   : plausible -> retain; implausible -> reopen structure.
# Reads data-raw/nhanes/nhanes_fi_endpoint.rds. No DMDM contract / manuscript writes.
# ============================================================================
suppressPackageStartupMessages({library(here)})
EP <- readRDS(here("data-raw","nhanes","nhanes_fi_endpoint.rds"))

## ---- D. FREEZE NHS transition kernel (NHS data only; model B) ---------------
bands<-c("62-64","65-69","70-74","75-79","80-84","85-87")
b_lo<-c(62,65,70,75,80,85); b_hi<-c(64,69,74,79,84,87)
prevN<-c(0.090,0.104,0.102,0.128,0.146,0.170); N_tot<-64559
share<-c(0.18,0.28,0.24,0.17,0.09,0.04); n_band<-round(share*N_tot); y_band<-round(n_band*prevN)
INC_EVENTS<-5954; INC_PY<-175447
fit_ages<-62:89
dens<-approx((b_lo+b_hi)/2, share/(b_hi-b_lo+1), xout=62:87, rule=2)$y; dens<-c(dens,dens[length(dens)],dens[length(dens)])
bof<-pmin(pmax(findInterval(fit_ages,b_lo),1),6); suscept<-dens*(1-prevN[bof]); suscept<-suscept/sum(suscept)
etaB<-function(pp,a) pp[1]+pp[2]*(a-73)/10
simN<-function(p0,b,r,a=fit_ages){inc<-1-exp(-exp(etaB(b,a)));p<-numeric(length(a));p[1]<-p0
  for(i in 2:length(a)) p[i]<-p[i-1]*(1-r)+(1-p[i-1])*inc[i-1];list(p=p,inc=inc)}
bp<-function(p) vapply(seq_along(bands),function(k) mean(p[b_lo[k]<=fit_ages&fit_ages<=b_hi[k]]),numeric(1))
negll<-function(th){p0<-plogis(th[1]);r<-plogis(th[4]);m<-simN(p0,th[2:3],r)
  pb<-pmin(pmax(bp(m$p),1e-9),1-1e-9)
  -(sum(dbinom(y_band,n_band,pb,log=TRUE))+dpois(INC_EVENTS,sum(suscept*m$inc)*INC_PY,log=TRUE))}
o<-optim(c(qlogis(.09),log(-log(.97)),.3,qlogis(.2)),negll,method="Nelder-Mead",control=list(maxit=9000,reltol=1e-12))
o<-optim(o$par,negll,method="Nelder-Mead",control=list(maxit=9000,reltol=1e-12))
B0<-o$par[2]; B1<-o$par[3]; R_NHS<-plogis(o$par[4])
onset_i<-function(a) 1-exp(-exp(B0+B1*(a-73)/10))          # frozen NHS onset hazard
cat(sprintf("FROZEN NHS kernel (model B): r=%.3f  onset(65)=%.4f onset(75)=%.4f onset(85)=%.4f\n",
            R_NHS, onset_i(65), onset_i(75), onset_i(85)))
cat(sprintf("  implied equilibrium P*=i/(i+r): age65=%.3f age75=%.3f age85=%.3f\n",
            onset_i(65)/(onset_i(65)+R_NHS), onset_i(75)/(onset_i(75)+R_NHS), onset_i(85)/(onset_i(85)+R_NHS)))

## ---- C. NATIONAL baseline initializer from NHANES --------------------------
fine<-EP$fi_nhs_fine   # 60-64,65-69,70-74,75-79,80+
base_prev<-function(age){ age<-pmin(age,89)
  b<-cut(age, c(0,60,65,70,75,80,Inf), right=FALSE,
         labels=c("<60","60-64","65-69","70-74","75-79","80+"))
  pv<-setNames(fine$prev, fine$band)
  out<-ifelse(as.character(b)=="<60", NA, pv[as.character(b)]); as.numeric(out)}
cat("\n=== C. Baseline initializer verification (synthetic national women 60+) ===\n")
set.seed(1); AGE<-EP$age_wt   # survey-weighted national age structure (women 60+)
pop_age<-sample(AGE$RIDAGEYR, 300000, replace=TRUE, prob=AGE$adj_weight)
pop_fi <-rbinom(length(pop_age),1, base_prev(pop_age))
for(bd in fine$band){ rng<-switch(bd,"60-64"=60:64,"65-69"=65:69,"70-74"=70:74,"75-79"=75:79,"80+"=80:89)
  idx<-pop_age%in%rng
  cat(sprintf("  %-6s synthetic %.3f  vs NHANES %.3f\n", bd, mean(pop_fi[idx]), fine$prev[fine$band==bd]))}
cat("  (initializer carries NO transition logic; role=baseline_state_initialization)\n")

## ---- E. COHORT DIAGNOSTIC: evolve SAME women under frozen NHS kernel --------
evolve<-function(p0, a0, years){P<-numeric(years+1);P[1]<-p0;a<-a0
  onset<-numeric(years);rem<-numeric(years)
  for(t in 1:years){i<-onset_i(a);onset[t]<-(1-P[t])*i;rem[t]<-P[t]*R_NHS
    P[t+1]<-P[t]*(1-R_NHS)+(1-P[t])*i;a<-a+1}
  list(P=P,onset=onset,rem=rem)}
cat("\n=== E. Starting-age cohorts, evolved 20y under frozen kernel ===\n")
cat(sprintf("%-9s %6s %6s %6s %6s %6s | %s\n","cohort@t0","P0","+1y","+5y","+10y","+20y","note"))
starts<-list("60-64"=62,"65-69"=67,"70-74"=72,"75-79"=77)
for(nm in names(starts)){a0<-starts[[nm]]; p0<-base_prev(a0); e<-evolve(p0,a0,20)
  eqv<-onset_i(a0)/(onset_i(a0)+R_NHS)
  note<-if(p0>eqv+0.01) "starts ABOVE equilibrium -> kernel pulls DOWN" else "rises toward equilibrium"
  cat(sprintf("%-9s %6.3f %6.3f %6.3f %6.3f %6.3f | %s\n",
      nm,e$P[1],e$P[2],e$P[6],e$P[11],e$P[21],note))}

cat("\n=== E. National CLOSED cohort (all women 60+ at t0), aggregate prevalence ===\n")
# vectorize over UNIQUE ages weighted by national age structure (mortality non-
# differential -> cancels from prevalence; closed cohort, no new entrants).
uage<-sort(unique(pop_age)); uw<-as.numeric(table(factor(pop_age,levels=uage)))
evolve_prev<-function(p0,a0,h){P<-p0;a<-a0;for(t in seq_len(h)){P<-P*(1-R_NHS)+(1-P)*onset_i(a);a<-a+1};P}
horizons<-c(0,1,5,10,20)
for(h in horizons){ pv<-vapply(uage,function(a) evolve_prev(base_prev(a),a,h), numeric(1))
  cat(sprintf("  +%2dy: aggregate FI prevalence (closed cohort) = %.3f\n", h, sum(pv*uw)/sum(uw)))}

## ---- F. GATE ---------------------------------------------------------------
cat("\n=== F. GATE (plausibility of national-init + frozen-kernel trajectories) ===\n")
c60<-evolve(base_prev(62),62,20)$P; c80<-evolve(base_prev(82),82,20)$P
declines<-c60[1] > c60[21]+0.01
cat(sprintf("  younger-old (60-64) cohort: P0=%.3f -> +20y=%.3f  %s\n",
            c60[1],c60[21], ifelse(declines,"DECLINES (national baseline > NHS equilibrium)","rises")))
cat(sprintf("  oldest (80-84) cohort: P0=%.3f -> +20y=%.3f\n", c80[1], c80[21]))
cat("  all prevalences in [0,1]:", all(c(c60,c80)>=0 & c(c60,c80)<=1), "\n")
cat(sprintf("  INTERPRETATION: national baseline (~0.16 at 60s) exceeds the NHS-kernel\n  equilibrium (~%.2f at 65). If cohorts implausibly decline, that is LEVEL tension\n  between national prevalence and NHS turnover -- reopen per gate F.\n", onset_i(65)/(onset_i(65)+R_NHS)))
