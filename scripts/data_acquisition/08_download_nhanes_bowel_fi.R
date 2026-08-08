#!/usr/bin/env Rscript
# ============================================================================
# NHANES 2005-2010 Bowel Health (BHQ) -> fecal-incontinence prevalence endpoints
# for the FI natural-history EXTERNAL VALIDATION and national BASELINE INITIALIZER.
#
# Two endpoints, women, survey-weighted (SDMVPSU/SDMVSTRA; MEC weights /3 pooled):
#   FI_wu  = >=monthly mucus (BHQ020) OR liquid (BHQ030) OR solid (BHQ040)
#            -> reproduces Wu 2014 (PMC3970401) as a pipeline check.
#   FI_nhs = >=monthly liquid OR solid  (mucus excluded)
#            -> definition-matched to the Nurses' Health Study state used for the
#               NHS transition kernel; this is the validation + initializer endpoint.
#
# NHANES top-codes age at 80 (2007-10) / 85 (2005-06), so >=80 is the finest top band.
# Any non-"Never" BHQ response is the "1-3 times a month" floor or higher => >=monthly.
#
# Output: data-raw/nhanes/nhanes_fi_endpoint.rds  (pooled + by-cycle + fine bands)
#         data-raw/nhanes/nhanes_fi_manifest.txt
# Source: CDC NHANES public data via nhanesA (no DUA). Run: Rscript this_file.R
# ============================================================================
suppressPackageStartupMessages({library(nhanesA); library(survey); library(dplyr); library(here)})
OUT <- here("data-raw","nhanes"); dir.create(OUT, showWarnings=FALSE, recursive=TRUE)

.safe <- function(t) tryCatch(nhanes(t), error=function(e){message("fail ",t,": ",conditionMessage(e));NULL})
# BHQ_D/_E label frequencies "...month, or"/"once a week,"/"Never?"; BHQ_F uses
# "1-3 times a month"/"Once a week"/"Never" (no comma, different case). Classify by
# PATTERN so any non-Never frequency (>= monthly floor) counts, across both codings.
comp <- function(x){x<-as.character(x)
  pos <- grepl("month|week|day", x, ignore.case=TRUE)      # any reported frequency = >= monthly
  nev <- grepl("^never", x, ignore.case=TRUE)              # Never / Never?
  ifelse(pos,1L, ifelse(nev,0L, NA_integer_))}             # Don't know / Refused / NA -> NA
fi_any <- function(cols){cl<-do.call(cbind,cols)
  ifelse(apply(cl,1,function(z)any(z==1,na.rm=TRUE)),1L,
         ifelse(apply(cl,1,function(z)all(is.na(z))),NA_integer_,0L))}

harm <- function(sfx){
  d<-.safe(paste0("DEMO",sfx)); b<-.safe(paste0("BHQ",sfx)); if(is.null(d)||is.null(b))return(NULL)
  d<-d[,c("SEQN","RIAGENDR","RIDAGEYR","WTMEC2YR","SDMVPSU","SDMVSTRA")]
  b<-b[,intersect(c("SEQN","BHQ020","BHQ030","BHQ040"),names(b))]
  m<-merge(d,b,by="SEQN",all.x=TRUE); for(v in c("BHQ020","BHQ030","BHQ040")) if(!v%in%names(m)) m[[v]]<-NA
  mu<-comp(m$BHQ020); li<-comp(m$BHQ030); so<-comp(m$BHQ040)
  m$fi_wu  <- fi_any(list(mu,li,so)); m$fi_nhs <- fi_any(list(li,so)); m$cycle<-sfx; m}

raw <- bind_rows(lapply(c("_D","_E","_F"), harm))
w <- raw %>% mutate(adj_weight=WTMEC2YR/3) %>% filter(as.character(RIAGENDR)=="Female", RIDAGEYR>=20)
options(survey.lonely.psu="adjust")

svy_band <- function(dat, outcome, brks, labs){
  dat<-dat[!is.na(dat[[outcome]]),]; dat$bd<-cut(dat$RIDAGEYR,brks,right=FALSE,labels=labs)
  des<-svydesign(id=~SDMVPSU,strata=~SDMVSTRA,weights=~adj_weight,nest=TRUE,data=dat)
  f<-as.formula(paste0("~",outcome))
  rows<-lapply(labs,function(lv){
    idx<-!is.na(des$variables$bd) & des$variables$bd==lv
    if(sum(idx)<2L) return(NULL)
    e<-tryCatch(svymean(f, subset(des,idx), na.rm=TRUE), error=function(er)NULL); if(is.null(e))return(NULL)
    pv<-coef(e)[[1]]; se<-SE(e)[[1]]
    data.frame(band=lv,prev=pv,lo=max(0,pv-1.96*se),hi=min(1,pv+1.96*se),n=sum(idx),row.names=NULL)})
  do.call(rbind, rows)}
svy_overall <- function(dat,outcome){dat<-dat[!is.na(dat[[outcome]]),]
  des<-svydesign(id=~SDMVPSU,strata=~SDMVSTRA,weights=~adj_weight,nest=TRUE,data=dat)
  e<-svymean(as.formula(paste0("~",outcome)),des); c(prev=coef(e),lo=coef(e)-1.96*SE(e),hi=coef(e)+1.96*SE(e))}

OUR3 <- list(brks=c(20,40,60,70,80,Inf), labs=c("20-39","40-59","60-69","70-79","80+"))
WU   <- list(brks=c(20,40,60,80,Inf),    labs=c("20-39","40-59","60-79","80+"))
FINE <- list(brks=c(60,65,70,75,80,Inf), labs=c("60-64","65-69","70-74","75-79","80+"))

res <- list(
  overall_wu  = svy_overall(w,"fi_wu"),
  overall_nhs = svy_overall(w,"fi_nhs"),
  fi_wu_wuband  = svy_band(w,"fi_wu",WU$brks,WU$labs),
  fi_wu_ourband = svy_band(w,"fi_wu",OUR3$brks,OUR3$labs),
  fi_nhs_ourband= svy_band(w,"fi_nhs",OUR3$brks,OUR3$labs),
  fi_nhs_fine   = svy_band(w,"fi_nhs",FINE$brks,FINE$labs),
  fi_nhs_bycycle= do.call(rbind, Filter(Negate(is.null), lapply(c("_D","_E","_F"), function(cy){
      d<-svy_band(w[w$cycle==cy,],"fi_nhs",c(60,70,80,Inf),c("60-69","70-79","80+"))
      if(is.null(d)||!nrow(d)) return(NULL); cbind(d, cycle=cy)}))),
  age_wt = { a<-w[!is.na(w$fi_nhs) & w$RIDAGEYR>=60,]; aggregate(adj_weight~RIDAGEYR, a, sum) },
  provenance = list(source="NHANES 2005-2010 (D,E,F) via nhanesA",
                    endpoint_wu="monthly mucus/liquid/solid", endpoint_nhs="monthly liquid/solid",
                    survey_weighted=TRUE, weight="WTMEC2YR/3", n_women20plus=nrow(w),
                    age_topcode="80 (2007-10) / 85 (2005-06)", built=as.character(Sys.time())))
saveRDS(res, file.path(OUT,"nhanes_fi_endpoint.rds"))
writeLines(c("NHANES 2005-2010 FI endpoints (women, survey-weighted)",
  paste("built:",res$provenance$built), paste("n women 20+:",nrow(w)),
  sprintf("FI_wu overall  = %.3f (%.3f-%.3f)  [Wu 2014 ~0.09 check]",res$overall_wu[1],res$overall_wu[2],res$overall_wu[3]),
  sprintf("FI_nhs overall = %.3f (%.3f-%.3f)  [definition-matched]",res$overall_nhs[1],res$overall_nhs[2],res$overall_nhs[3]),
  "endpoints: FI_wu (mucus/liquid/solid), FI_nhs (liquid/solid); >=monthly; source CDC NHANES via nhanesA (no DUA)"),
  file.path(OUT,"nhanes_fi_manifest.txt"))
cat("saved nhanes_fi_endpoint.rds\n")
cat(sprintf("FI_wu overall %.3f (Wu check ~0.09) | FI_nhs overall %.3f\n", res$overall_wu[1], res$overall_nhs[1]))
print(res$fi_nhs_bycycle, digits=3)
