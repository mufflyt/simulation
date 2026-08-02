# Real spatial-access inputs

## `tract_fem65_centroids.csv` (vendored)

Census tract **female-65+ population with centroids** — the demand denominator
for the E2SFCA access layer. 83,492 CONUS tracts, total female 65+ ≈ **30.5M**.

| col | meaning |
|---|---|
| `GEOID` | 11-digit census tract id |
| `fem65` | female population aged 65+ (ACS) |
| `lon`, `lat` | tract centroid, EPSG:4326 |

Extracted from `twostep/data/urogyn_tract_fem65_centroids.rds` (itself derived
in the isochrones pipeline). Registered as canonical `tract_fem65_centroids`
(path + SHA-256); loaded via `load_tract_demand()` — no `sf` needed.

## Provider isochrone polygons (NOT vendored)

The **supply-side** catchment geometry — provider drive-time isochrone polygons
(30/60/120/180 min) — are large Valhalla-produced artifacts from the isochrones
pipeline and are **not** shipped here. Point `load_provider_isochrones()` at a
local `isochrones_{band}min_consolidated.rds` set via its `artifacts_dir`
argument or the `ISOCHRONES_ARTIFACTS_DIR` environment variable. The loader
fails closed if they are absent.

## How they combine

```r
tracts <- load_tract_demand()                     # real demand (vendored)
iso    <- load_provider_isochrones()              # real supply geometry (external)
membership <- build_access_membership(iso, tracts)# sf point-in-polygon overlay
surface <- real_access_surface(iso, supply)       # -> E2SFCA over real demand
```

The geometry is year-agnostic (built once); the microsim inner loop only swaps
`supply` each simulated year. Only the overlay needs `sf` (Suggests); the engine
and the tract loader stay dependency-light.

## Build note

`data-raw/` is `.Rbuildignore`d — the CSV travels with the git repo (source
runs + provenance) but not the built package.
