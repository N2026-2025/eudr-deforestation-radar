"""Global Forest Watch API Extractor."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import requests

from .base import BaseExtractor, Provenance

logger = logging.getLogger(__name__)


class GFWExtractor(BaseExtractor):
    """Extractor for Global Forest Watch Data API."""

    BASE_URL = "https://data-api.globalforestwatch.org"
    ALERTS_DATASET = "umd_glad_dist_alerts"
    ALERTS_VERSION = "latest"

    def __init__(
        self,
        api_key: str,
        output_dir: Path | str,
        domain_allowlist: list[str] | None = None,
    ) -> None:
        super().__init__("gfw", output_dir)
        self.api_key = api_key
        self.domain_allowlist = domain_allowlist or ["localhost"]
        self._headers = {
            "x-api-key": api_key,
            "Content-Type": "application/json",
        }

    def get_datasets(self) -> dict[str, Any]:
        """List available GFW datasets."""
        url = f"{self.BASE_URL}/dataset"
        response = self._request_with_retry("GET", url, headers=self._headers)
        return response.json()

    def query_alerts(
        self,
        geometry: dict[str, Any],
        start_date: str | None = None,
        end_date: str | None = None,
        confidence: str | None = None,
        limit: int = 10000,
    ) -> tuple[pd.DataFrame, Provenance]:
        """
        Query GLAD/RADD alerts within a geometry.

        Args:
            geometry: GeoJSON Polygon/MultiPolygon
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            confidence: Filter by confidence (low/nominal/high)
            limit: Maximum records to return
        """
        where_clauses = []
        if start_date:
            where_clauses.append(f"umd_glad_landsat_alerts__date >= '{start_date}'")
        if end_date:
            where_clauses.append(f"umd_glad_landsat_alerts__date <= '{end_date}'")
        if confidence:
            where_clauses.append(f"umd_glad_landsat_alerts__confidence = '{confidence}'")

        where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"

        sql = f"""
            SELECT
                latitude,
                longitude,
                umd_glad_landsat_alerts__date as alert_date,
                umd_glad_landsat_alerts__confidence as confidence,
                umd_glad_dist_alerts__intensity as intensity,
                area__ha as area_ha
            FROM results
            WHERE {where_sql}
            LIMIT {limit}
        """

        payload = {
            "sql": sql,
            "geometry": geometry,
        }

        url = f"{self.BASE_URL}/dataset/{self.ALERTS_DATASET}/{self.ALERTS_VERSION}/query/json"
        response = self._request_with_retry("POST", url, headers=self._headers, json=payload)

        content = response.content
        sha256 = self.compute_sha256(content)
        data = response.json()

        records = data.get("data", [])
        df = pd.DataFrame(records)

        if not df.empty:
            df["source"] = "gfw"
            df["source_dataset"] = self.ALERTS_DATASET
            df["ingested_at"] = self._utc_now_iso()

        provenance = Provenance(
            source="gfw",
            dataset_id=self.ALERTS_DATASET,
            fetched_at=self._utc_now_iso(),
            url=url,
            sha256=sha256,
            response_code=response.status_code,
            record_count=len(df),
            license="GFW Data License - see https://www.globalforestwatch.org/data-licensing/",
        )

        return df, provenance

    def extract(
        self,
        geometry: dict[str, Any],
        start_date: str | None = None,
        end_date: str | None = None,
        confidence: str | None = None,
        limit: int = 10000,
        partition: str | None = None,
    ) -> tuple[pd.DataFrame, Provenance]:
        """Extract GFW alerts and write to Bronze."""
        df, provenance = self.query_alerts(
            geometry=geometry,
            start_date=start_date,
            end_date=end_date,
            confidence=confidence,
            limit=limit,
        )

        self.write_bronze(df, provenance, "alerts", partition=partition)
        return df, provenance