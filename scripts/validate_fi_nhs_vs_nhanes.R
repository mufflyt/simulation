#!/usr/bin/env Rscript
# ============================================================================
# LOCKED EXTERNAL VALIDATION (diagnostic; preserves a FAIL result on purpose).
# Frozen NHS FI natural-history model vs independent survey-weighted NHANES
# (definition-matched liquid/solid). Reads data-raw/nhanes/nhanes_fi_endpoint.rds
# (built by scripts/data_acquisition/08_download_nhanes_bowel_fi.R).
#
# CALIBRATION uses NHS ONLY (recomputed deterministically -> the frozen result).
# VALIDATION compares to NHANES; NHANES is never fitted to. NHANES validates
# PREVALENCE, not remission (r stays model_identified_from_NHS).
# Terminology kept separate throughout. No DMDM contract / manuscript writes.
# ============================================================================
suppressPackageStartupMessages({library(here); library(MASS)})
EP <- readRDS(here("data-raw","nhanes","nhanes_fi_endpoint.rds"))

## ---- FREEZE NHS models A/B/C (NHS data only; no NHANES input) ---------------
bands<-c("62-64","65-69","70-74","75-79","80-84","85-87")
b_lo<-c(62,65,70,75,80,85); b_hi<-c(64,69,74,79,84,87)
prevN<-c(0.090,0.104,0.102,0.128,0.146,0.170); N_tot<-64559
share<-c(0.18,0.28,0.24,0.17,0.09,0.04); n_band<-round(share*N_tot); y_band<-round(n_band*prevN)
INC_EVENTS<-5954; INC_PY<-175447
ages<-62:89
dens<-approx((b_lo+b_hi)/2,share/(b_hi-b_lo+1),xout=62:87,rule=2)$y; dens<-c(dens,dens[26],dens[26])
bof<-pmin(pmax(findInterval(ages,b_lo),1),6); suscept<-dens*(1-prevN[bof]); suscept<-suscept/sum(suscept)
eta<-list(A=function(p) rep(p[1],length(ages)),
          B=function(p) p[1]+p[2]*(ages-73)/10,
          C=function(p) p[1]+p[2]*(ages-73)/10+p[3]*pmax((ages-75)/10,0))
npar<-c(A=1,B=2,C=3)
sim<-function(p0,e,r){inc<-1-exp(-exp(e));p<-numeric(length(ages));p[1]<-p0
  for(i in 2:length(ages)) p[i]<-p[i-1]*(1-r)+(1-p[i-1])*inc[i-1];list(p=p,inc=inc)}
bp<-function(p) vapply(seq_along(bands),function(k) mean(p[b_lo[k]<=ages&ages<=b_hi[k]]),numeric(1))
nll<-function(th,s){k<-npar[s];m<-sim(plogis(th[1]),eta[[s]](th[2:(1+k)]),plogis(th[length(th)]))
  pb<-pmin(pmax(bp(m$p),1e-9),1-1e-9)
  -(sum(dbinom(y_band,n_band,pb,log=TRUE))+dpois(INC_EVENTS,sum(suscept*m$inc)*INC_PY,log=TRUE))}
fitm<-function(s){k<-npar[s];st<-c(qlogis(.09),log(-log(.97)),rep(0,k-1),qlogis(.2))
  o<-optim(st,nll,s=s,method="Nelder-Mead",control=list(maxit=9000,reltol=1e-12))
  o<-optim(o$par,nll,s=s,method="Nelder-Mead",control=list(maxit=9000,reltol=1e-12))
  H<-optimHess(o$par,nll,s=s);V<-tryCatch(solve(H),error=function(e)diag(1e-6,length(o$par)))
  list(par=o$par,V=V,s=s,r=plogis(o$par[length(o$par)]),m2ll=2*o$value,k=k+2)}
FIT<-lapply(c("A","B","C"),fitm); names(FIT)<-c("A","B","C")

## ---- NHANES targets (from frozen artifact; corrected 3-cycle) ---------------
OBS<-EP$fi_nhs_ourband[EP$fi_nhs_ourband$band %in% c("60-69","70-79","80+"),]
AGE<-EP$age_wt
vbands<-list("60-69"=62:69,"70-79"=70:79,"80+"=80:85)   # model domain starts 62; NHANES top-code 85
mbp<-function(th,s,rng){k<-npar[s];m<-sim(plogis(th[1]),eta[[s]](th[2:(1+k)]),plogis(th[length(th)]))
  wj<-AGE[AGE$RIDAGEYR%in%rng,]; wv<-tapply(wj$adj_weight,factor(wj$RIDAGEYR,levels=rng),sum);wv[is.na(wv)]<-0
  if(sum(wv)==0) wv<-rep(1,length(rng)); sum(wv*m$p[match(rng,ages)])/sum(wv)}
set.seed(1)
pred<-lapply(names(FIT),function(s){draws<-MASS::mvrnorm(600,FIT[[s]]$par,FIT[[s]]$V)
  sapply(names(vbands),function(vb){v<-apply(draws,1,function(th) mbp(th,s,vbands[[vb]]))
    c(pt=mbp(FIT[[s]]$par,s,vbands[[vb]]),lo=unname(quantile(v,.025)),hi=unname(quantile(v,.975)))})})
names(pred)<-names(FIT)

## ---- OUTPUTS ----------------------------------------------------------------
cat("=== (1) Wu reproduction check (pipeline) ===\n")
cat(sprintf("  NHANES FI_wu (mucus/liquid/solid) overall women = %.3f (%.3f-%.3f)  [Wu 2014 ~0.09]\n",
    EP$overall_wu[1],EP$overall_wu[2],EP$overall_wu[3]))
cat("\n=== (2) definition-matched NHANES endpoint (liquid/solid) + cycle stability ===\n")
print(EP$fi_nhs_ourband[,c("band","prev","lo","hi","n")],digits=3)
for(cy in unique(EP$fi_nhs_bycycle$cycle)){s<-EP$fi_nhs_bycycle[EP$fi_nhs_bycycle$cycle==cy,]
  cat(sprintf("  cycle %s dip(70-79<60-69)=%s\n",cy,s$prev[s$band=="70-79"]<s$prev[s$band=="60-69"]))}
cat(sprintf("FROZEN NHS fits: r = %.3f / %.3f / %.3f (A/B/C)\n",FIT$A$r,FIT$B$r,FIT$C$r))

cat("\n=== (3) EXTERNAL VALIDATION: frozen NHS model B vs NHANES ===\n")
cat(sprintf("%-6s | %-20s | %-20s | overlap | relerr\n","band","NHANES (95%CI)","model B (95%CI)"))
within<-logical(nrow(OBS))
for(i in seq_len(nrow(OBS))){vb<-OBS$band[i];pb<-unname(pred$B[,vb])
  within[i]<-OBS$lo[i]<=pb[3]&&OBS$hi[i]>=pb[2]
  cat(sprintf("%-6s | %.3f (%.3f-%.3f)  | %.3f (%.3f-%.3f)  | %-7s | %+.0f%%\n",
    vb,OBS$prev[i],OBS$lo[i],OBS$hi[i],pb[1],pb[2],pb[3],ifelse(within[i],"yes","NO"),
    100*(pb[1]-OBS$prev[i])/OBS$prev[i]))}
cat("\n=== (4) structural sensitivity A/B/C (point predictions) ===\n")
for(vb in names(vbands)) cat(sprintf("  %-6s A=%.3f B=%.3f C=%.3f | NHANES %.3f\n",
    vb,pred$A["pt",vb],pred$B["pt",vb],pred$C["pt",vb],OBS$prev[OBS$band==vb]))
o80<-(pred$B["pt","80+"]-OBS$prev[OBS$band=="80+"])/OBS$prev[OBS$band=="80+"]
cat(sprintf("\n=== (5) >=80 performance: model %.3f vs NHANES %.3f (%+.0f%%) ===\n",
    pred$B["pt","80+"],OBS$prev[OBS$band=="80+"],100*o80))
cat("\n=== (8) VERDICT under prespecified gate ===\n")
cat(sprintf("  AICc-n note: n=7 (6 prevalence + 1 incidence); raw -2logL A/B/C = %.1f/%.1f/%.1f\n",
    FIT$A$m2ll,FIT$B$m2ll,FIT$C$m2ll))
cat(sprintf("  major >=80 error: %+.0f%%  | obs within model CI: [%s]\n",100*o80,paste(within,collapse=",")))
cat("  VERDICT: FAIL as a direct national prevalence generator (NHS underpredicts national\n")
cat("  level; the 70-79 dip is NOT cycle-robust). r remains model_identified_from_NHS\n")
cat("  (NHANES validates PREVALENCE, not remission). See option-5 architecture:\n")
cat("  scripts/fi_baseline_initializer_and_cohort_diagnostic.R\n")

## ---- observed-vs-predicted plot --------------------------------------------
png(here("artifacts","fi_validation.png"),width=1100,height=720,res=140)
par(mar=c(4.5,4.8,3.6,1.2)); x<-1:3
plot(x,OBS$prev,pch=19,cex=1.6,ylim=c(0.05,0.28),xaxt="n",xlab="Age band (women)",
     ylab="Monthly liquid/solid FI prevalence",col="black",cex.main=0.92,
     main="Frozen NHS FI model vs independent NHANES (definition-matched)\nCALIBRATION: NHS only | VALIDATION: NHANES 2005-2010 survey-weighted")
axis(1,at=x,labels=OBS$band); arrows(x,OBS$lo,x,OBS$hi,angle=90,code=3,length=.08,lwd=2)
for(s in c("A","C")) lines(x,pred[[s]]["pt",],col=ifelse(s=="A","#2980b9","#27ae60"),lty=ifelse(s=="A",2,3),lwd=1.5)
lines(x,pred$B["pt",],col="#c0392b",lwd=2.5);points(x,pred$B["pt",],pch=17,col="#c0392b",cex=1.4)
legend("topleft",bty="n",cex=.85,legend=c("NHANES observed (95% CI)","model B (primary)","model A","model C"),
  col=c("black","#c0392b","#2980b9","#27ae60"),pch=c(19,17,NA,NA),lty=c(NA,1,2,3),lwd=c(NA,2.5,1.5,1.5))
dev.off(); cat(sprintf("\nplot -> artifacts/fi_validation.png\n"))
