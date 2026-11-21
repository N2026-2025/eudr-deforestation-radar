# EUDR Deforestation Radar — Implementation Plan

**Context:** Supply Chain Compliance & Deforestation Radar for EU Deforestation Regulation (EUDR) compliance. Tracks agricultural lots (soy, beef, timber) from origin to EU port, verifying they come from deforestation-free land post-2020.

---

## 1. Project Objectives

| Objective | Description |
|---|---|
| **EUDR Compliance** | Automate due diligence statement generation per EU IS DDS v3 schema; avoid 4% turnover fines + cargo confiscation. |
| **Multi-Source Tracing** | Integrate 5 data sources: GFW alerts, Planetary Computer Sentinel-2, UN Comtrade, Argentine MAGyP, NASA FIRMS. |
| **Lot-Level Risk Scoring** | Per-lot deforestation flag + risk assessment (country risk + supply chain complexity + third-party certification). |
| **Audit Trail** | Immutable provenance (SHA-256, timestamps, API response codes) for all ingested data; 5-year record retention. |
| **Scalable ETL** | Batch pipeline (nightly/hourly) for compliance; streaming only for FIRMS near-real-time alerts. |

---

## 2. High-Level Architecture

```text
+----------------------+       +----------------------+       +----------------------+
|   Ingestion Layer    |       |   Processing Layer   |       |   Output Layer       |
| (API Extractors)     |       | (GeoPandas/Shapely)  |       | (DDS Generation)     |
+----------+-----------+       +----------+-----------+       +----------+-----------+
           |                               |                           |
           v                               v                           v
+----------------------+       +----------------------+       +----------------------+
|   Bronze Storage     |       |   Silver/Gold Layer  |       |   EUDR Compliance DB |
| (Parquet/COG in     |       |   (DuckDB over       |       | (PostGIS + Python    |
|   Object Lake)       |       |   GeoParquet/Parquet)|       |   API)               |
+----------------------+       +----------------------+       +----------------------+
           |                               |                           |
           v                               v                           v
+----------------------+       +----------------------+       +----------------------+
|   Orchestration      |       |   Monitoring/Alert   |       |   DDS Export (JSON)  |
|   (Airflow/Prefect)  |       |   (Prometheus/Grafana)|       +----------------------+
+----------------------+       +----------------------+
```

---

## 3. Data Source Integration

| Source | API Contract | Auth | Python Client | Key Fields |
|---|---|---|---|---|
| **X: GFW** | `POST /dataset/{DATASET}/{VERSION}/query/json` | API key (domains allowlist) | `openepi-client` `gfw` | `latitude`, `longitude`, `alert_date`, `confidence`, `intensity`, `area_ha` |
| **Y: Planetary Computer STAC** | STAC search + SAS token | Anonymous (SAS token for Azure Blob) | `planetary-computer` `pystac-client` | `bbox`, `datetime`, `collections: ["sentinel-2-l2a"]`, `asset.href` |
| **Z: UN Comtrade** | `GET /public/v1/preview/{typeCode}/{freqCode}/{clCode}` | Free key (optional) | `comtradeapicall` or `requests` | `reporterCode`, `partnerCode`, `cmdCode`, `flowCode`, `period`, `volume` |
| **C: MAGyP Argentina** | `GET /series/api/search` + `GET /series/api/series` | None (public) | `requests` wrapper | `series_id`, `year`, `province`, `department`, `area_ha`, `production_tm`, `yield_kgxha` |
| **A: NASA FIRMS** | `GET /api/area/csv/{MAP_KEY}/{SOURCE}/{coords}/{day_range}/{date}` | Free MAP_KEY (email) | `earthlens` / `firms-client` | `latitude`, `longitude`, `bright_ti4`, `frp`, `satellite`, `confidence`, `acq_date` |

---

## 4. EUDR Data Model (Core Entities)

| Entity | Required Fields (per EUDR) |
|---|---|
| **EconomicOperator** | `operatorRole` (`OPERATOR`/`REPRESENTATIVE_OPERATOR`), `name`, `address`, `country` |
| **Commodity** | `commercialDescription`, `hsHeading` (2–10 digit HS code, `EUDR2`), `speciesInfo` (scientific/common name for wood), `quantity` |
| **Plot/CattleEstablishment** | `geoLocation` (GeoJSON Point/MultiPoint/Polygon/MultiPolygon; ≥6 decimal lat/long; polygon required for plots >4 ha), `countryOfProduction`, `productionDate` |
| **DueDiligenceStatement** | `internalReferenceNumber`, `activityType` (`IMPORT`/`EXPORT`/`DOMESTIC`), `countryOfActivity`, `borderCrossCountry`, `commodities[]`, `producers[]`, `geoLocationConfidential` (bool), `riskMitigationMeasure` |
| **RiskAssessment** | `countryRiskLevel` (Art. 29: low/standard/high), `presenceOfForests`, `indigenousPeoples`, `claims`, `supplyChainComplexity`, `circumventionRisk`, `certificationInfo`, `sourceReliability` |

**Key EUDR Rules Encoded:**
- Deforestation cutoff: **31 Dec 2020** — any post-cutoff forest loss → disqualifies lot.
- Geolocation precision: ≥6 decimal digits; plots >4 ha require polygon; cattle → single point per establishment.
- Records retention: **5 years**.
- Risk mitigation measures (Art. 11) when risk identified.

---

## 5. ETL Pipeline Design

### 5.1 Stages (per spatial ETL reference: Discover → Extract → Transform → Validate → Load)

| Stage | Description | Tools |
|---|---|---|
| **Discover** | Query STAC for Sentinel-2 scenes in AOI within date window; query GFW alerts; query FIRMS daily fires; pull Comtrade/MAGyP master data. | `planetary_computer.search()`, `gfw_client.query()`, `firms_client.fetch_area_range()`, `requests` to Comtrade/MAGyP |
| **Extract** | Pull raw payloads → store as immutable Parquet (Bronze) with provenance SHA-256/URL/timestamp. | `pyarrow.parquet.write_table()`, `rasterio` COG reads |
| **Transform** | Normalize CRS (WGS84 / EPSG:4326); repair geometry (shapely); align schemas; compute H3 cells for aggregation; flag post-2020 deforestation via GFW/Planetary Computer intersection. | `pyproj.transform()`, `shapely`, `h3.latlng_to_cell()`, `geopandas.sindex` |
| **Validate** | Quality gates: geometry validity, CRS consistency, field completeness; dead-letter store for malformed records instead of crash. | `cerberus`/`pydantic` schemas; dead-letter Parquet bucket |
| **Load** | Write to DuckDB spatial over GeoParquet/Parquet (Silver); curated results → PostGIS for serving (Gold). | `duckdb` spatial extension, `geopandas.to_parquet()` |

### 5.2 Batch DAG (daily snapshot, 06:00 schedule)

```text
SCHEDULE daily 06:00
  1. Discover: GFW alerts (last 24h) + FIRMS (last 5d) + Sentinel-2 scenes (window) + Comtrade/MAGyP
  2. Extract: Store raw API responses as Bronze Parquet
  3. Transform: Reproject to WGS84, compute H3 cells, intersect GFW/Planetary Computer footprints with lot polygons, flag post-2020 deforestation
  4. Validate: Schema completeness, geometry validity, risk assessment completeness
  5. Load: Silver (DuckDB) → Gold (PostGIS risk scores + DDS payloads)
  6. Output: Generate per-lot DDS JSON payloads aligned to EU IS DDS v3 schema
```

### 5.3 Orchestration Choice

- **Recommended: Dagster** — partitioned assets (tile/scene partitions), dynamic partitions, built-in testability, sync/execute plan. Best fit for tiled/scene-partitioned raster/vector assets.
- Alternative: **Prefect** for Python-native flows with `.map()` fan-out; good when unit-of-work count unknown at design time.
- **Airflow** viable but XCom for GeoDataFrames → use object storage handoff instead.

---

## 6. Storage Strategy

| Layer | Format | Description |
|---|---|---|
| **Bronze** | Parquet + GeoJSON (immutable) | Raw API responses; provenance: `{url, fetched_at, sha256, response_code, record_count, license}` |
| **Silver** | Parquet + GeoParquet | Normalized, CRS-aligned, H3-cell indexed; dead-letter subset |
| **Gold** | PostGIS tables (or DuckDB views) | Risk scores per lot, deforestation flag, DDS generation inputs; serving layer for API |
| **Dead-Letter** | Separate Parquet bucket | Malformed/incomplete records for manual review |

**File formats:**
- Rasters: Cloud Optimized GeoTIFF (COG) — byte-range access for Sentinel-2.
- Vectors/tables: Parquet + GeoParquet (WKB + `geo` metadata + CRS + partitioning by space/time).

---

## 7. Key Implementation Risks & Mitigations

| Risk | Mitigation |
|---|---|
| **API rate-limit exhaustion** (GFW, FIRMS, Planetary Computer) | Use rate-limit-aware pools; batch queries; cache results; FIRMS MAP_KEY: 5,000 tx/10 min — batch by 5-day ranges; GFW tier-based keys. |
| **CRS mismatch errors** | Enforce single internal CRS (WGS84/EPSG:4326); transform at ingest; assert CRS agreement before spatial ops. |
| **Incomplete geolocation data** (some lots only have postal address) | Design for fallback: postal address → geocode via HERE/Mapbox with confidence scoring; mark as `geoLocationConfidential`; risk-weighted accordingly. |
| **Post-2020 deforestation false negatives** | Convergence-of-evidence: GFW + Planetary Computer + NASA FIRMS + Hansen/GFC2020 baselines; only clear if ALL sources agree pre-2020 stable. |
| **EUDR schema drift** (DDS v3 API changes) | Pin DDS schema version in code; backward-compatible wrapper; monitor EU Information System releases. |
| **Argentina MAGyP data gaps** | Cross-reference with multiple years; flag missing years; supplement with satellite evidence where tabular data absent. |

---

## 8. Validation & Testing

| Test Type | Description |
|---|---|
| **Unit tests** | API client wrappers (mock responses); geometry transform functions; H3 cell computation. |
| **Integration tests** | End-to-end pipeline run with synthetic lot data; verify DDS JSON output matches EU IS DDS v3 schema (using JSON Schema validation). |
| **Spatial correctness** | Verify: (a) polygon area >4 ha → polygon required; (b) ≥6 decimal digit precision; (c) post-2020 deforestation flag correctly set when GFW alert intersects lot after 2020-12-31. |
| **Provenance tests** | Verify Bronze Parquet has SHA-256 + timestamp; verify immutability (no overwrite). |
| **Performance tests** | Measure pipeline runtime for full dataset; verify DuckDB query < 30s for 10K+ lot records; verify COG byte-range reads work. |
| **Schema validation** | JSON Schema validation of DDS output against EU IS DDS v3; automated CI step. |

---

## 9. Open Questions / Decisions Needed

1. **Orchestration choice:** Dagster vs. Prefect vs. Airflow? Recommendation: Dagster for partitioned asset handling, but team familiarity may dictate otherwise.
2. **DDS submission method:** Direct POST to EU Information System (requires EU DS gateway credentials) vs. generating JSON payloads for manual submission? 
3. **Argentina data scope:** Which commodities/departments first? Soy is priority (high EUDR impact); beef/wood follow.
4. **Risk assessment granularity:** Country-level Art. 29 risk only, or lot-level risk scoring incorporating supply chain complexity, certification, and third-party data?
5. **Storage location:** Pure object lake (Azure/AWS GCS) vs. hybrid with PostGIS serving layer? DuckDB spatial can serve without PostGIS but PostGIS better for concurrent API serving.

---

## 10. Next Steps (Implementation Plan)

1. **Sprint 0 (1 week):** Set up repo, CI/CD, secrets management (API keys, MAP_KEY, SAS tokens); define DDS JSON Schema; provision DuckDB + PostGIS environments.
2. **Sprint 1 (2 weeks):** Implement API extractors for all 5 sources → Bronze Parquet with provenance; unit tests for each client.
3. **Sprint 2 (2 weeks):** Transform layer: CRS normalization, geometry repair, H3 indexing, post-2020 deforestation intersection logic; integrate first two sources (GFW + FIRMS).
4. **Sprint 3 (2 weeks):** Validate + Load: dead-letter schema, DuckDB Silver layer, DDS payload generation; integrate Comtrade + MAGyP data.
5. **Sprint 4 (2 weeks):** Risk assessment module (country risk + supply chain flags); PostGIS Gold layer; end-to-end pipeline run with sample lots.
6. **Sprint 5 (1 week):** Schema validation (JSON Schema DDS v3); documentation; handoff.

---

**Plan file:** `C:\Users\nesto\orca\projects\DATAENGINEER\.kilo\plans\1795252072765-eudr-deforestation-radar-plan.md`

**Plan status:** Implementation-ready. Contains all context, decisions, data models, pipeline design, storage strategy, risks, validation, and next steps for another implementation-capable agent to execute safely.