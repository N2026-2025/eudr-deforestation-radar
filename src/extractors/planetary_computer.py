"""Microsoft Planetary Computer STAC API Extractor."""

from __future__ import annotations

import hashlib
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import planetary_computer
import pystac_client
import requests

from .base import BaseExtractor, Provenance

logger = logging.getLogger(__name__)


class PlanetaryComputerExtractor(BaseExtractor):
    """Extractor for Microsoft Planetary Computer STAC API (Sentinel-2, Landsat)."""

    STAC_URL = "https://planetarycomputer.microsoft.com/api/stac/v1"
    SAS_URL = "https://planetarycomputer.microsoft.com/api/sas/v1/token"

    def __init__(
        self,
        output_dir: Path | str | None = None,
        subscription_key: str | None = None,
    ) -> None:
        super().__init__("planetary_computer", output_dir or Path(".data/bronze/planetary_computer"))
        self.subscription_key = subscription_key
        if subscription_key:
            planetary_computer.set_subscription_key(subscription_key)

        self._catalog = pystac_client.Client.open(
            self.STAC_URL,
            modifier=planetary_computer.sign_inplace,
        )

    def _utc_now_iso(self) -> str:
        return datetime.now(timezone.utc).isoformat()

    def search_sentinel2(
        self,
        bbox: list[float],
        start_date: str,
        end_date: str,
        collections: list[str] | None = None,
        max_items: int = 100,
    ) -> pd.DataFrame:
        """
        Search Sentinel-2 scenes in an AOI and date range.

        Args:
            bbox: [min_lon, min_lat, max_lon, max_lat]
            start_date: YYYY-MM-DD
            end_date: YYYY-MM-DD
            collections: STAC collections to search (default: sentinel-2-l2a)
            max_items: Maximum items to return
        """
        collections = collections or ["sentinel-2-l2a"]
        datetime_range = f"{start_date}/{end_date}"

        search = self._catalog.search(
            collections=collections,
            bbox=bbox,
            datetime=datetime_range,
        )

        items = list(search.get_items())[:max_items]

        records = []
        for item in items:
            record = {
                "source": "planetary_computer",
                "item_id": item.id,
                "collection": item.collection_id,
                "datetime": item.datetime.isoformat() if item.datetime else None,
                "bbox": item.bbox,
                "geometry": item.geometry,
                "assets": {name: asset.href for name, asset in item.assets.items()},
                "properties": dict(item.properties),
                "ingested_at": self._utc_now_iso(),
            }
            records.append(record)

        df = pd.DataFrame.from_records(records)
        return df

    def get_sas_token(self, collection: str) -> dict[str, str]:
        """Get SAS token for a collection's Azure Blob Storage."""
        url = f"{self.SAS_URL}/{collection}"
        response = self._request_with_retry("GET", url)
        return response.json()

    def extract(
        self,
        bbox: list[float],
        start_date: str,
        end_date: str,
        collections: list[str] | None = None,
        max_items: int = 100,
        partition: str | None = None,
    ) -> tuple[pd.DataFrame, Provenance]:
        """Extract Sentinel-2 scene metadata and write to Bronze."""
        df = self.search_sentinel2(
            bbox=bbox,
            start_date=start_date,
            end_date=end_date,
            collections=collections,
            max_items=max_items,
        )

        content = df.to_json().encode("utf-8")
        sha256 = self.compute_sha256(content)

        provenance = Provenance(
            source="planetary_computer",
            dataset_id="sentinel2_scenes",
            fetched_at=self._utc_now_iso(),
            url=self.STAC_URL,
            sha256=sha256,
            response_code=200,
            record_count=len(df),
            license="Microsoft Planetary Computer Data License",
        )

        self.write_bronze(df, provenance, "sentinel2_scenes", partition=partition)
        return df, provenance