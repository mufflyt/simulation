#!/usr/bin/env Rscript
# Strengthen the <=5km rescue-linkage audit with ZIP/address concordance (shrinks
# the "ambiguous" set from the name+state-only pass) and emit a BLINDED manual-
# adjudication worksheet. Aggregate PPV -> tracked; worksheet -> local gitignored
# path only (contains called-provider identities; outcome withheld).
suppressPackageStartupMessages({library(dplyr); library(digest)})
MYST <- Sys.getenv("MYSTERYCALL_DATA","/Users/tylermuffly/Movies/results_CorbisieroMysteryCal_DATA_LABELS_2021_03_15_1249_mutate_50.rds")
ISO  <- "/Users/tylermuffly/isochrones/artifacts"
pv <- readRDS(file.path(ISO,"2sfca/ec2/e2sfca_20260712_190734/unpacked/step_4_2sfca_FPMRS_2020_providers.rds"))
cm <- readRDS(file.path(ISO,"20260702_120134_90bf52ef/step_3_year_coord_map.rds"))
coh<- readRDS(file.path(ISO,"20260802_101936_ce1223fc/step_2.5_final_cohort.rds"))
m  <- readRDS(MYST)
OUT_TRACK<-"scripts/validation/access_validation_outputs"; dir.create(OUT_TRACK,showWarnings=FALSE,recursive=TRUE)
OUT_LOCAL<-"artifacts/access_validation"; dir.create(OUT_LOCAL,showWarnings=FALSE,recursive=TRUE)  # gitignored

ll<-do.call(rbind,strsplit(pv$coord_id,"_")); pv$clat<-as.numeric(ll[,1]); pv$clon<-as.numeric(ll[,2])
cm<-cm[cm$analysis_year==2020&!is.na(cm$coord_id),c("npi","coord_id")]; cm$npi<-as.character(cm$npi)
coh$npi<-as.character(coh$npi)
z5<-function(x) substr(gsub("[^0-9]","",as.character(x)),1,5)
lastname<-function(x){x<-tolower(gsub("[^a-z ]","",tolower(x)));w<-strsplit(trimws(x)," ")
  vapply(w,function(z){z<-z[z!=""&z!="doctor"&z!="dr"];if(!length(z))NA_character_ else z[length(z)]},character(1))}
coh$last<-if("last_name"%in%names(coh)) tolower(coh$last_name) else lastname(coh$physician_name)
coh$pz<-z5(coh$practice_zip); coh$pst<-toupper(substr(coh$practice_state,1,2))
cohxy<-coh[!is.na(coh$lat)&!is.na(coh$lon),]

m$npi<-ifelse(is.na(m$NPI),NA_character_,as.character(as.integer(m$NPI)))
m$mlast<-lastname(m$Name); m$mz<-z5(m$zip); m$mst<-toupper(substr(m$state,1,2))
m$clat<-m$lat;m$clon<-m$lng; miss<-is.na(m$clat)|is.na(m$clon)
gi<-match(m$npi[miss],cohxy$npi[!duplicated(cohxy$npi)]); u<-cohxy[!duplicated(cohxy$npi),]
m$clat[miss]<-u$lat[gi]; m$clon[miss]<-u$lon[gi]
hav<-function(la1,lo1,la2,lo2){R<-6371;p<-pi/180;a<-sin((la2-la1)*p/2)^2+cos(la1*p)*cos(la2*p)*sin((lo2-lo1)*p/2)^2;2*R*asin(pmin(1,sqrt(a)))}
byc<-setNames(seq_len(nrow(pv)),pv$coord_id)

## rescue set = calls with no exact NPI->coord_id but a <=5km nearest origin
resc<-list()
for(i in seq_len(nrow(m))){npi<-m$npi[i]
  exact<-!is.na(npi)&&!is.na(cm$coord_id[match(npi,cm$npi)])&&cm$coord_id[match(npi,cm$npi)]%in%names(byc)
  if(exact||is.na(m$clat[i])||is.na(m$clon[i])) next
  d<-hav(m$clat[i],m$clon[i],pv$clat,pv$clon);j<-which.min(d); if(d[j]>5) next
  cl<-cohxy[hav(pv$clat[j],pv$clon[j],cohxy$lat,cohxy$lon)<=5,]        # cohort members at assigned cluster
  nm<-!is.na(m$mlast[i]) && m$mlast[i]%in%cl$last
  zp<-!is.na(m$mz[i]) && m$mz[i]%in%cl$pz
  st<-!is.na(m$mst[i]) && any(cl$pst==m$mst[i],na.rm=TRUE)
  cls<-if(nrow(cl)) "true" else "false"
  cls<-if((nm||zp)&&st) "true" else if(nm||zp||st) "ambiguous" else "false"
  resc[[length(resc)+1]]<-data.frame(row=i,coord_id=pv$coord_id[j],nearest_km=round(d[j],2),
    name_match=nm,zip_match=zp,state_match=st,cls=cls,
    mystery_name=m$Name[i],mystery_city=m$city[i],mystery_state=m$mst[i],mystery_zip=m$mz[i],
    cluster_members=paste(head(unique(cl$physician_name),4),collapse=" | "),
    cluster_zips=paste(head(unique(cl$pz),4),collapse=","))}
R<-do.call(rbind,resc)
tab<-table(factor(R$cls,levels=c("true","ambiguous","false")))
ppv<-tab["true"]/(tab["true"]+tab["false"]); nE<-tab["true"]+tab["false"]
ci<-suppressWarnings(binom.test(tab["true"],nE)$conf.int)
cat("=== ENHANCED rescue audit (name OR zip, with state) ===\n")
cat(sprintf("rescues=%d  true=%d ambiguous=%d false=%d  PPV=%.3f (%.3f-%.3f)\n",
  nrow(R),tab["true"],tab["ambiguous"],tab["false"],ppv,ci[1],ci[2]))
cat(sprintf("zip-concordant: %.0f%%  name-concordant: %.0f%%\n",100*mean(R$zip_match),100*mean(R$name_match)))
write.csv(data.frame(rescues=nrow(R),true=tab["true"],ambiguous=tab["ambiguous"],false=tab["false"],
  ppv=round(ppv,3),ppv_lo=round(ci[1],3),ppv_hi=round(ci[2],3),
  method="name_or_zip_with_state",row.names=NULL),
  file.path(OUT_TRACK,"linkage_audit_summary_v2.csv"),row.names=FALSE)

## BLINDED manual worksheet (NO appt outcome) -> local gitignored only
set.seed(7); samp<-R[sample(nrow(R),min(40,nrow(R))),
  c("row","coord_id","nearest_km","mystery_name","mystery_city","mystery_state","mystery_zip",
    "cluster_members","cluster_zips","name_match","zip_match","state_match","cls")]
samp$manual_verdict<-""    # for the human to fill: true/false/ambiguous
write.csv(samp, file.path(OUT_LOCAL,"manual_audit_worksheet.csv"), row.names=FALSE)
cat(sprintf("\nblinded worksheet (40 rescues, outcome withheld) -> %s/manual_audit_worksheet.csv (LOCAL, not committed)\n",OUT_LOCAL))
cat("aggregate PPV summary -> ",OUT_TRACK,"/linkage_audit_summary_v2.csv\n",sep="")
