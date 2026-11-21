"""Main pipeline orchestrator for EUDR Deforestation Radar."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import geopandas as gpd
import pandas as pd

from .extractors import (
    BaseExtractor,
    Provenance,
    GFWExtractor,
    FIRMSExtractor,
    ComtradeExtractor,
    MAGyPExtractor,
    PlanetaryComputerExtractor,
)
from .transform import SilverTransformer, DeforestationDetector, write_silver, read_silver
from .validate import run_quality_gates, QualityGateConfig, ValidationPipeline
from .load import load_to_duckdb, load_to_postgis, DuckDBLoader, PostGISLoader
from .dds import DDSGenerator

logger = logging.getLogger(__name__)


class EUDRPipeline:
    """End-to-end EUDR Deforestation Radar pipeline."""

    def __init__(
        self,
        bronze_dir: Path | str = Path(".data/bronze"),
        silver_dir: Path | str = Path(".data/silver"),
        gold_dir: Path | str = Path(".data/gold"),
        dead_letter_dir: Path | str = Path(".data/dead_letter"),
        duckdb_path: Path | str = Path(".data/silver/eudr.duckdb"),
        postgis_config: dict[str, Any] | None = None,
        operator_id: str = "unknown",
    ) -> None:
        self.bronze_dir = Path(bronze_dir)
        self.silver_dir = Path(silver_dir)
        self.gold_dir = Path(gold_dir)
        self.dead_letter_dir = Path(dead_letter_dir)

        for d in [self.bronze_dir, self.silver_dir, self.gold_dir, self.dead_letter_dir]:
            d.mkdir(parents=True, exist_ok=True)

        self.duckdb_path = Path(duckdb_path)
        self.postgis_config = postgis_config or {}
        self.operator_id = operator_id

        self.transformer = SilverTransformer()
        self.validator = ValidationPipeline(output_dir=dead_letter_dir)
        self.dds_generator = DDSGenerator(operator_id=operator_id)

    def run_ingestion(
        self,
        gfw_geometry: dict[str, Any] | None = None,
        gfw_start_date: str | None = None,
        firms_area: str = "world",
        firms_day_range: int = 1,
        comtrade_reporter: str = "76",
        comtrade_partner: str = "288",
        magyp_series: str = "AGRO_A_Soja_0001,AGRO_A_Soja_0002,AGRO_A_Soja_0003",
        pc_bbox: list[float] | None = None,
        pc_start_date: str | None = None,
        pc_end_date: str | None = None,
        api_keys: dict[str, str] | None = None,
    ) -> dict[str, Provenance]:
        """Run data ingestion from all 5 sources."""
        api_keys = api_keys or {}
        provenances = {}

        # 1. Global Forest Watch
        if gfw_geometry and api_keys.get("gfw_api_key"):
            logger.info("Ingesting GFW alerts...")
            gfw = GFWExtractor(
                api_key=api_keys["gfw_api_key"],
                output_dir=self.bronze_dir,
            )
            _, prov = gfw.extract(
                geometry=gfw_geometry,
                start_date=gfw_start_date,
            )
            provenances["gfw"] = prov

        # 2. NASA FIRMS
        if api_keys.get("firms_map_key"):
            logger.info("Ingesting NASA FIRMS fires...")
            firms = FIRMSExtractor(
                map_key=api_keys["firms_map_key"],
                output_dir=self.bronze_dir,
            )
            _, prov = firms.extract(
                area_coordinates=firms_area,
                day_range=firms_day_range,
            )
            provenances["firms"] = prov

        # 3. UN Comtrade
        logger.info("Ingesting UN Comtrade data...")
        comtrade = ComtradeExtractor(
            api_key=api_keys.get("comtrade_api_key"),
            output_dir=self.bronze_dir,
        )
        try:
            _, prov = comtrade.extract(
                reporter_code=comtrade_reporter,
                partner_code=comtrade_partner,
            )
            provenances["comtrade"] = prov
        except Exception as e:
            logger.warning("Comtrade extraction failed: %s", str(e))
            provenances["comtrade"] = None

        # 4. MAGyP Argentina
        logger.info("Ingesting MAGyP Argentina data...")
        magyp = MAGyPExtractor(output_dir=self.bronze_dir)
        try:
            _, prov = magyp.extract(series_ids=magyp_series)
            provenances["magyp"] = prov
        except Exception as e:
            logger.warning("MAGyP extraction failed: %s", str(e))
            provenances["magyp"] = None

        # 5. Planetary Computer
        if pc_bbox and pc_start_date and pc_end_date:
            logger.info("Ingesting Planetary Computer Sentinel-2...")
            pc = PlanetaryComputerExtractor(
                output_dir=self.bronze_dir,
                subscription_key=api_keys.get("planetary_computer_key"),
            )
            _, prov = pc.extract(
                bbox=pc_bbox,
                start_date=pc_start_date,
                end_date=pc_end_date,
            )
            provenances["planetary_computer"] = prov

        return provenances

    def run_transform(
        self,
        lots_df: pd.DataFrame,
        gfw_alerts: pd.DataFrame | None = None,
        firms_fires: pd.DataFrame | None = None,
    ) -> gpd.GeoDataFrame:
        """Run transform layer on lots data."""
        logger.info("Running transform on %d lots...", len(lots_df))
        silver = self.transformer.transform_lots(
            lots_df=lots_df,
            gfw_alerts=gfw_alerts,
            firms_fires=firms_fires,
        )
        return silver

    def run_validate(
        self,
        gdf: gpd.GeoDataFrame,
    ) -> tuple[gpd.GeoDataFrame, list[dict[str, Any]]]:
        """Run validation and quality gates."""
        logger.info("Running quality gates...")
        config = QualityGateConfig()
        result = run_quality_gates(gdf, config)

        valid_gdf, dead_letters = self.validator.process(
            df=gdf,
            gdf=gdf,
            source_name="pipeline",
            dataset_id="lots",
        )

        return valid_gdf, dead_letters

    def run_load(
        self,
        gdf: gpd.GeoDataFrame,
    ) -> int:
        """Load to DuckDB (Silver) and PostGIS (Gold)."""
        logger.info("Loading to DuckDB...")
        load_to_duckdb(gdf, self.duckdb_path)

        if self.postgis_config:
            logger.info("Loading to PostGIS...")
            load_to_postgis(
                gdf,
                **self.postgis_config,
            )

        return len(gdf)

    def run_dds_generation(
        self,
        gdf: gpd.GeoDataFrame,
    ) -> list[dict[str, Any]]:
        """Generate DDS payloads for all lots."""
        logger.info("Generating DDS payloads...")
        lots_list = gdf.to_dict("records")
        dds_payloads = self.dds_generator.generate_dds_batch(lots_list)

        # Attach DDS to GeoDataFrame
        gdf["dds_payload"] = [json.dumps(d) for d in dds_payloads]

        # Write DDS JSON files
        dds_dir = self.gold_dir / "dds"
        dds_dir.mkdir(parents=True, exist_ok=True)

        for payload in dds_payloads:
            ref = payload["internalReferenceNumber"]
            with open(dds_dir / f"{ref}.json", "w", encoding="utf-8") as f:
                json.dump(payload, f, indent=2)

        logger.info("Generated %d DDS payloads", len(dds_payloads))
        return dds_payloads

    def run_full_pipeline(
        self,
        lots_df: pd.DataFrame,
        ingestion_config: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Run complete pipeline: ingest -> transform -> validate -> load -> DDS."""
        results = {
            "started_at": datetime.now(timezone.utc).isoformat(),
            "provenances": {},
            "transformed_lots": 0,
            "valid_lots": 0,
            "dead_letter_count": 0,
            "dds_generated": 0,
            "errors": [],
        }

        ingestion_config = ingestion_config or {}

        try:
            # 1. Ingest
            provenances = self.run_ingestion(**ingestion_config)
            results["provenances"] = {k: v.to_dict() for k, v in provenances.items()}

            # Load Bronze data for transform
            gfw_alerts = None
            firms_fires = None
            for prov in provenances.values():
                if prov.dataset_id == "alerts":
                    bronze_path = self.bronze_dir / f"gfw_{prov.dataset_id}" / f"data_{prov.fetched_at[:19].replace(':', '')}.parquet"
                    if bronze_path.exists():
                        gfw_alerts = pd.read_parquet(bronze_path)
                elif prov.dataset_id == "active_fire":
                    bronze_path = self.bronze_dir / f"firms_{prov.dataset_id}" / f"data_{prov.fetched_at[:19].replace(':', '')}.parquet"
                    if bronze_path.exists():
                        firms_fires = pd.read_parquet(bronze_path)

            # 2. Transform
            silver = self.run_transform(lots_df, gfw_alerts, firms_fires)
            results["transformed_lots"] = len(silver)

            # 3. Validate
            valid_silver, dead_letters = self.run_validate(silver)
            results["valid_lots"] = len(valid_silver)
            results["dead_letter_count"] = len(dead_letters)

            # 4. Load
            self.run_load(valid_silver)

            # 5. DDS Generation
            dds_payloads = self.run_dds_generation(valid_silver)
            results["dds_generated"] = len(dds_payloads)

            # Write Silver layer
            write_silver(valid_silver, self.silver_dir / "lots")

            results["completed_at"] = datetime.now(timezone.utc).isoformat()
            results["status"] = "success"

        except Exception as e:
            logger.exception("Pipeline failed")
            results["status"] = "failed"
            results["errors"].append(str(e))
            results["completed_at"] = datetime.now(timezone.utc).isoformat()

        return results


def run_pipeline(
    lots_df: pd.DataFrame,
    bronze_dir: Path | str = ".data/bronze",
    silver_dir: Path | str = ".data/silver",
    gold_dir: Path | str = ".data/gold",
    **kwargs: Any,
) -> dict[str, Any]:
    """Convenience function to run full pipeline."""
    pipeline = EUDRPipeline(
        bronze_dir=bronze_dir,
        silver_dir=silver_dir,
        gold_dir=gold_dir,
    )
    return pipeline.run_full_pipeline(lots_df, **kwargs)