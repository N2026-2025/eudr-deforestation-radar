"""Validation layer: quality gates and dead-letter handling for EUDR pipeline."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal

import geopandas as gpd
import pandas as pd
import pyarrow.parquet as pq
from pydantic import BaseModel, Field, ValidationError


logger = logging.getLogger(__name__)


DEAD_LETTER_SUBDIR = "dead_letter"


class QualityGateResult(BaseModel):
    """Result of running quality gates on a dataset."""

    passed: bool = True
    errors: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)
    records_checked: int = 0
    records_passed: int = 0
    records_failed: int = 0


class QualityGateConfig(BaseModel):
    """Configuration for quality gates."""

    require_geometry: bool = True
    require_valid_geometry: bool = True
    require_production_date: bool = False
    require_area_ha: bool = False
    min_area_ha: float = 0.0
    require_h3_cell: bool = False
    max_invalid_ratio: float = 0.1


def run_quality_gates(
    gdf: gpd.GeoDataFrame,
    config: QualityGateConfig | None = None,
) -> QualityGateResult:
    """Run quality gates on a GeoDataFrame of lots."""
    config = config or QualityGateConfig()
    errors: list[str] = []
    warnings: list[str] = []

    result = QualityGateResult(
        passed=True,
        errors=errors,
        warnings=warnings,
        records_checked=len(gdf),
    )

    if len(gdf) == 0:
        result.passed = True
        result.warnings.append("Empty dataset - no records to validate")
        return result

    # Geometry validity
    if config.require_geometry and "geometry" not in gdf.columns:
        errors.append("Missing geometry column")
        result.passed = False
    if config.require_valid_geometry and "geometry" in gdf.columns:
        invalid_count = (~gdf.geometry.is_valid).sum()
        invalid_ratio = invalid_count / len(gdf)
        if invalid_count > 0:
            warnings.append(f"{invalid_count} records have invalid geometries")
        if invalid_ratio > config.max_invalid_ratio:
            errors.append(
                f"Too many invalid geometries: {invalid_ratio:.2%} exceeds max {config.max_invalid_ratio:.2%}"
            )
            result.passed = False

    # Area checks
    if config.require_area_ha and "area_ha" in gdf.columns:
        below_min = gdf[gdf["area_ha"] < config.min_area_ha]
        if len(below_min) > 0:
            errors.append(f"{len(below_min)} records below minimum area threshold")

    # H3 cell check
    if config.require_h3_cell and "h3_cell" in gdf.columns:
        null_cells = gdf["h3_cell"].isna().sum()
        if null_cells > 0:
            errors.append(f"{null_cells} records missing H3 cell")

    # Production date check
    if config.require_production_date and "production_date" in gdf.columns:
        missing_dates = gdf["production_date"].isna().sum()
        if missing_dates > 0:
            warnings.append(f"{missing_dates} records missing production date")

    result.errors = errors
    result.warnings = warnings
    result.passed = len(errors) == 0
    valid_geoms = gdf.geometry.is_valid.sum() if "geometry" in gdf.columns else len(gdf)
    result.records_passed = valid_geoms
    result.records_failed = len(gdf) - result.records_passed if not result.passed else 0

    if not result.passed:
        logger.error("Quality gates failed: %s", errors)

    return result


class DeadLetterEntry(BaseModel):
    """Dead-letter record entry for failed validation."""

    original_id: str | None = None
    error: str
    error_type: str
    raw_data: dict[str, Any] | None = None
    failed_at: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


class ValidationPipeline:
    """Orchestrates quality gates and dead-letter handling."""

    def __init__(
        self,
        output_dir: Path | str = Path(".data/dead_letter"),
    ) -> None:
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def process(
        self,
        df: pd.DataFrame,
        gdf: gpd.GeoDataFrame,
        source_name: str,
        dataset_id: str,
    ) -> tuple[gpd.GeoDataFrame, list[DeadLetterEntry]]:
        """
        Run quality gates; return valid data and dead-letter entries.

        Returns:
            (valid_gdf, dead_letter_entries)
        """
        config = QualityGateConfig()
        result = run_quality_gates(gdf, config)

        if result.passed:
            return gdf.drop(columns=["geometry_valid"]) if "geometry_valid" in gdf.columns else gdf, []

        # Build dead-letter entries for failed records
        dead_letter_entries: list[DeadLetterEntry] = []

        if "geometry" in gdf.columns and "geometry_valid" in gdf.columns:
            # Filter to invalid geometries
            invalid_gdf = gdf[~gdf["geometry_valid"]].copy()
            for idx in invalid_gdf.index:
                dead_letter_entries.append(
                    DeadLetterEntry(
                        original_id=str(idx),
                        error="Invalid geometry",
                        error_type="invalid_geometry",
                        raw_data=gdf.loc[idx].to_dict(),
                    )
                )

        # Deduplicate errors
        unique_errors = list(set(result.errors))
        for err in unique_errors:
            dead_letter_entries.append(
                DeadLetterEntry(
                    original_id=None,
                    error=err,
                    error_type="quality_gate_failure",
                    raw_data=None,
                )
            )

        # Write dead-letter entries
        if dead_letter_entries:
            timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
            dl_path = self.output_dir / f"dead_letter_{source_name}_{dataset_id}_{timestamp}.jsonl"
            with open(dl_path, "w", encoding="utf-8") as f:
                for entry in dead_letter_entries:
                    f.write(json.dumps(entry.model_dump()) + "\n")
            logger.warning("Wrote %d dead-letter entries to %s", len(dead_letter_entries), dl_path)

        return (
            gdf[gdf.geometry.is_valid].copy().drop(columns=["geometry_valid"])
            if "geometry" in gdf.columns
            else gdf.drop(columns=["geometry_valid"]) if "geometry_valid" in gdf.columns else gdf
        ), dead_letter_entries
