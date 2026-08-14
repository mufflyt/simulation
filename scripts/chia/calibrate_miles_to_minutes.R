suppressPackageStartupMessages({library(sf); library(dplyr)})
base <- "/Users/tylermuffly/isochrones/data/abu_urology/augmented_isochrones_fpmrs_uro"
out <- list()
for (b in c(30, 60, 120, 180)) {
  x <- readRDS(file.path(base, sprintf("isochrones_%dmin_consolidated.rds", b)))
  # Massachusetts providers by centre coordinate
  ma <- x[x$center_lat > 41.2 & x$center_lat < 42.95 &
          x$center_lng > -73.55 & x$center_lng < -69.85, ]
  rm(x); invisible(gc())
  if (!nrow(ma)) next
  ma <- st_make_valid(ma)
  ma <- st_transform(ma, 5070)                      # equal-area, metres
  a  <- as.numeric(st_area(ma))
  r  <- sqrt(a / pi) / 1609.34                      # equivalent-area radius, miles
  r  <- r[is.finite(r) & r > 0]
  out[[as.character(b)]] <- data.frame(
    minutes = b, n = length(r),
    r_p25 = quantile(r,.25), r_med = median(r), r_p75 = quantile(r,.75))
  cat(sprintf("  %3d min: n=%4d  median radius %5.1f mi\n", b, length(r), median(r)))
  rm(ma); invisible(gc())
}
res <- bind_rows(out); rownames(res) <- NULL
res[,3:5] <- round(res[,3:5], 1)
cat("\n=== Massachusetts: equivalent-area radius by drive-time band ===\n")
print(as.data.frame(res))
saveRDS(res, "/tmp/ma_radius.rds")
