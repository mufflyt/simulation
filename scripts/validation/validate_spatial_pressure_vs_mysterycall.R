#!/usr/bin/env Rscript
# ============================================================================
# VALIDATE THE isochrones<->mysterycall EMPIRICAL BRIDGE (integrated demand model).
# Does local E2SFCA geographic demand pressure log(D/S) predict appointment ACCESS?
# Gate before any access-clearing queue: (1) audit the <=5km rescue linkage (PPV),
# (2) decompose the access hurdle H0-H4 and test pressure at each stage,
# (3) exact-NPI + audited-rescue sensitivities.
#
# PRIVACY: consumes the restricted mystery-call file via env var MYSTERYCALL_DATA
# (never committed). Records input SHA-256s. Writes ONLY aggregate-safe outputs to
# artifacts/access_validation/ (linkage audit, coefficients, decile diagnostics,
# provenance) -- no caller/patient-level rows.
# ============================================================================
suppressPackageStartupMessages({library(dplyr); library(lme4); library(digest)})

MYST <- Sys.getenv("MYSTERYCALL_DATA",
  "/Users/tylermuffly/Movies/results_CorbisieroMysteryCal_DATA_LABELS_2021_03_15_1249_mutate_50.rds")
ISO  <- "/Users/tylermuffly/isochrones/artifacts"
PROVF<- file.path(ISO,"2sfca/ec2/e2sfca_20260712_190734/unpacked/step_4_2sfca_FPMRS_2020_providers.rds")
CMAPF<- file.path(ISO,"20260702_120134_90bf52ef/step_3_year_coord_map.rds")
COHF <- file.path(ISO,"20260802_101936_ce1223fc/step_2.5_final_cohort.rds")
OUT  <- "scripts/validation/access_validation_outputs"; dir.create(OUT, showWarnings=FALSE, recursive=TRUE)
stopifnot(file.exists(MYST), file.exists(PROVF), file.exists(CMAPF), file.exists(COHF))
prov <- list(built=as.character(Sys.time()),
  mysterycall=list(path=MYST, sha256=digest(file=MYST, algo="sha256")),
  providers   =list(path=PROVF, sha256=digest(file=PROVF,algo="sha256")),
  coord_map   =list(path=CMAPF, sha256=digest(file=CMAPF,algo="sha256")),
  cohort      =list(path=COHF, sha256=digest(file=COHF, algo="sha256")))

## ---- inputs ---------------------------------------------------------------
m  <- readRDS(MYST); pv <- readRDS(PROVF); cm <- readRDS(CMAPF); coh <- readRDS(COHF)
ll<-do.call(rbind,strsplit(pv$coord_id,"_")); pv$clat<-as.numeric(ll[,1]); pv$clon<-as.numeric(ll[,2])
pv$L<-pv$weighted_demand/pv$supply
cm<-cm[cm$analysis_year==2020 & !is.na(cm$coord_id),c("npi","coord_id")]; cm$npi<-as.character(cm$npi)
coh$npi<-as.character(coh$npi)
lastname<-function(x){x<-tolower(gsub("[^a-z ]","",tolower(x)));w<-strsplit(trimws(x)," ")
  vapply(w,function(z){z<-z[z!=""&z!="doctor"&z!="dr"];if(!length(z))NA_character_ else z[length(z)]},character(1))}
coh$last <- if("last_name"%in%names(coh)) tolower(coh$last_name) else lastname(coh$physician_name)
cohxy<-coh[!is.na(coh$lat)&!is.na(coh$lon),]; cohxy<-cohxy[!duplicated(cohxy$npi),]

## ---- mystery outcomes + hurdle stages -------------------------------------
cc1<-"Able to contact office on first call?"; cc2<-"Able to contact office on second call attempt?  Use this is unable to reach the office the first time."
aff<-function(x) grepl("^yes",trimws(as.character(x)),ignore.case=TRUE)
wait<-suppressWarnings(as.numeric(m[["Business days until appointment"]]))
excl<-as.character(m[["Reason for exclusions"]])
m$insurance<-relevel(factor(ifelse(grepl("medicaid",m$insurance_type,ignore.case=TRUE),"Medicaid","BCBS")),"BCBS")
m$npi<-ifelse(is.na(m$NPI),NA_character_,as.character(as.integer(m$NPI)))
m$mlast<-lastname(m$Name)
m$reached   <- aff(m[[cc1]])|aff(m[[cc2]])                                  # H0
m$appt_obt  <- as.integer(!is.na(wait)); m$wait_days<-wait                    # H3 success / H4
# failure-stage classification from exclusion text (only for non-obtained calls)
fail_stage<-function(e){ if(is.na(e)||e=="") return(NA_character_)
  if(grepl("voicemail|not answered|busy|hold|personal phone",e,ignore.case=TRUE)) return("H0")
  if(grepl("did not correspond|closed medical",e,ignore.case=TRUE)) return("H1")
  if(grepl("MEDICAID",e,ignore.case=TRUE)) return("H2")
  if(grepl("new patients|referral required|midlevel",e,ignore.case=TRUE)) return("H3")
  NA_character_ }
m$fstage<-vapply(excl, fail_stage, character(1))
# staged flags (conditional universe)
m$appropriate <- m$reached & !(m$fstage %in% c("H1"))                         # reached & right office
m$eligible    <- m$appropriate & !(m$fstage %in% c("H2"))                     # + insurance accepted
m$obtainable  <- ifelse(m$appt_obt==1,1L, ifelse(m$eligible & m$fstage %in% c("H3"),0L, NA_integer_))  # H3 among eligible

## ---- crosswalk (Tier1 NPI; Tier2 <=5km via mystery/cohort geocode) --------
m$clat<-m$lat; m$clon<-m$lng; miss<-is.na(m$clat)|is.na(m$clon)
gi<-match(m$npi[miss],cohxy$npi); m$clat[miss]<-cohxy$lat[gi]; m$clon[miss]<-cohxy$lon[gi]
hav<-function(la1,lo1,la2,lo2){R<-6371;p<-pi/180
  a<-sin((la2-la1)*p/2)^2+cos(la1*p)*cos(la2*p)*sin((lo2-lo1)*p/2)^2;2*R*asin(pmin(1,sqrt(a)))}
byc<-setNames(seq_len(nrow(pv)),pv$coord_id)
xw<-lapply(seq_len(nrow(m)),function(i){npi<-m$npi[i];tier<-"unmatched";idx<-NA;nd<-NA
  if(!is.na(npi)){ci<-cm$coord_id[match(npi,cm$npi)]; if(!is.na(ci)&&ci%in%names(byc)){tier<-"1_npi";idx<-byc[[ci]];nd<-0}}
  if(is.na(idx)&&!is.na(m$clat[i])&&!is.na(m$clon[i])){d<-hav(m$clat[i],m$clon[i],pv$clat,pv$clon);j<-which.min(d)
    nd<-d[j]; if(d[j]<=5){tier<-"2_nearest5km";idx<-j}}
  if(is.na(idx)) return(data.frame(tier=tier,mi=i,supply=NA,D=NA,L=NA,clat_a=NA,clon_a=NA,nd=nd))
  data.frame(tier=tier,mi=i,supply=pv$supply[idx],D=pv$weighted_demand[idx],L=pv$L[idx],
             clat_a=pv$clat[idx],clon_a=pv$clon[idx],nd=nd)})
xw<-do.call(rbind,xw); d<-cbind(m,xw[,-2]); d$matched<-d$tier!="unmatched"

## ---- (1) BLINDED LINKAGE AUDIT of rescued matches (PPV) --------------------
# concordance = a cohort FPMRS provider within 5km of the assigned coord_id whose
# LAST NAME matches the called physician AND state agrees. Outcome not used here.
audit_one<-function(i){
  la<-d$clat_a[i]; lo<-d$clon_a[i]; if(is.na(la)) return("no_geo")
  near<-cohxy[hav(la,lo,cohxy$lat,cohxy$lon)<=5,]
  if(!nrow(near)) return("false")
  nm<-!is.na(d$mlast[i]) && d$mlast[i] %in% near$last
  st<-!is.na(d$state[i]) && any(toupper(substr(near$practice_state,1,2))==toupper(d$state[i]),na.rm=TRUE)
  if(nm && st) "true" else if(nm||st) "ambiguous" else "false" }
resc<-which(d$tier=="2_nearest5km")
d$audit<-NA_character_; for(i in resc) d$audit[i]<-audit_one(i)
at<-table(factor(d$audit[resc],levels=c("true","ambiguous","false","no_geo")))
ppv<-as.numeric(at["true"])/(at["true"]+at["false"]); n_eval<-at["true"]+at["false"]
ci<-if(!is.na(ppv)) suppressWarnings(binom.test(at["true"],n_eval)$conf.int) else c(NA,NA)
audit_df<-data.frame(true=at["true"],ambiguous=at["ambiguous"],false=at["false"],no_geo=at["no_geo"],
  ppv=round(ppv,3),ppv_lo=round(ci[1],3),ppv_hi=round(ci[2],3),row.names=NULL)
write.csv(audit_df, file.path(OUT,"linkage_audit_summary.csv"), row.names=FALSE)

## ---- (2) SEQUENTIAL HURDLE H0-H4 + pressure at each stage ------------------
ms<-d[d$matched & !is.na(d$L),]; ms$logL<-as.numeric(scale(log(ms$L))); ms$loc<-paste0(round(ms$D),"_",ms$supply)
stage_or<-function(out, universe){ u<-ms[universe & !is.na(ms[[out]]),]
  if(length(unique(u[[out]]))<2 || nrow(u)<20) return(c(n=nrow(u),rate=round(mean(u[[out]]),2),OR=NA,lo=NA,hi=NA,p=NA))
  fit<-tryCatch(glmer(as.formula(paste0(out,"~logL+insurance+(1|loc)")),u,family=binomial,nAGQ=0),
                error=function(e) glm(as.formula(paste0(out,"~logL+insurance")),u,family=binomial))
  s<-summary(fit)$coefficients; r<-s["logL",]
  c(n=nrow(u),rate=round(mean(u[[out]]),2),OR=round(exp(r[1]),2),lo=round(exp(r[1]-1.96*r[2]),2),
    hi=round(exp(r[1]+1.96*r[2]),2),p=round(r[length(r)],3)) }
H<-rbind(
  H0_reached                = stage_or("reached",    rep(TRUE,nrow(ms))),
  H1_appropriate_g_reached  = stage_or("appropriate",ms$reached),
  H2_eligible_g_appropriate = stage_or("eligible",   ms$appropriate),
  H3_obtainable_g_eligible  = stage_or("obtainable", ms$eligible),
  ALLCAUSE_appt_obtained    = stage_or("appt_obt",   ms$reached))
write.csv(data.frame(stage=rownames(H),H,row.names=NULL), file.path(OUT,"hurdle_stage_models.csv"), row.names=FALSE)

## ---- (3) sensitivities: exact-NPI only; audited-rescue (true) only ---------
fit_or<-function(sub){ u<-sub[!is.na(sub$appt_obt) & sub$reached,]; u$logL<-as.numeric(scale(log(u$L)))
  if(nrow(u)<30) return(c(n=nrow(u),OR=NA,lo=NA,hi=NA,p=NA))
  f<-tryCatch(glmer(appt_obt~logL+insurance+(1|loc),u,family=binomial,nAGQ=0),error=function(e)glm(appt_obt~logL+insurance,u,family=binomial))
  s<-summary(f)$coefficients;r<-s["logL",];c(n=nrow(u),OR=round(exp(r[1]),2),lo=round(exp(r[1]-1.96*r[2]),2),hi=round(exp(r[1]+1.96*r[2]),2),p=round(r[length(r)],3)) }
sens<-rbind(
  primary_matched     = fit_or(ms),
  exact_npi_only      = fit_or(ms[ms$tier=="1_npi",]),
  audited_true_rescue = fit_or(ms[ms$tier=="1_npi" | (ms$tier=="2_nearest5km" & ms$audit=="true"),]))
write.csv(data.frame(set=rownames(sens),sens,row.names=NULL), file.path(OUT,"sensitivity_appt_obtained.csv"), row.names=FALSE)

## ---- decile diagnostics + provenance --------------------------------------
a<-ms[ms$reached,]; a$dec<-ntile(a$L,10)
dec<-a%>%group_by(dec)%>%summarise(n=n(),L_med=round(median(L)),appt_pct=round(100*mean(appt_obt)),
  wait_med=suppressWarnings(as.numeric(median(wait_days,na.rm=TRUE))),.groups="drop")
write.csv(as.data.frame(dec), file.path(OUT,"decile_diagnostics.csv"), row.names=FALSE)
jsonlite::write_json(prov, file.path(OUT,"provenance_manifest.json"), auto_unbox=TRUE, pretty=TRUE)

## ---- console report --------------------------------------------------------
cat("=== (1) LINKAGE AUDIT of <=5km rescues (blinded, name+state concordance) ===\n"); print(audit_df)
cat("\n=== (2) SEQUENTIAL HURDLE (OR = per SD log(D/S)) ===\n"); print(data.frame(stage=rownames(H),H,row.names=NULL))
cat("\n=== (3) appt-obtained sensitivities ===\n"); print(data.frame(set=rownames(sens),sens,row.names=NULL))
cat("\n=== decile (appt % & median wait by pressure) ===\n"); print(as.data.frame(dec))
cat(sprintf("\nsafe outputs -> %s/  (no caller-level rows). inputs hashed in provenance_manifest.json\n",OUT))
