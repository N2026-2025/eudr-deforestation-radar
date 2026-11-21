"""NASA FIRMS Active Fire API Extractor."""

from __future__ import annotations

import hashlib
import logging
from pathlib import Path
from typing import Any

import pandas as pd

from .base import BaseExtractor, Provenance

logger = logging.getLogger(__name__)


class FIRMSExtractor(BaseExtractor):
    """Extractor for NASA FIRMS Active Fire API."""

    BASE_URL = "https://firms.modaps.eosdis.nasa.gov"

    FIRE_SOURCES = {
        "MODIS_NRT": "MODIS_Terra_NRT",
        "MODIS_SP": "MODIS_Terra_SP",
        "VIIRS_NOAA20_NRT": "VIIRS_NOAA20_NRT",
        "VIIRS_NOAA20_SP": "VIIRS_NOAA20_SP",
        "VIIRS_NOAA21_NRT": "VIIRS_NOAA21_NRT",
        "VIIRS_NOAA21_SP": "VIIRS_NOAA21_SP",
        "VIIRS_SNPP_NRT": "VIIRS_SNPP_NRT",
        "VIIRS_SNPP_SP": "VIIRS_SNPP_SP",
        "LANDSAT_NRT": "Landsat8_NRT",
    }

    def __init__(self, map_key: str, output_dir: Path | str) -> None:
        super().__init__("firms", output_dir)
        self.map_key = map_key
        self._headers = {"User-Agent": f"eudr-deforestation-radar/{map_key}"}

    def fetch_area_csv(
        self,
        area_coordinates: str,
        day_range: int = 1,
        start_date: str | None = None,
    ) -> pd.DataFrame:
        """
        Fetch fire detections for an area.

        Args:
            area_coordinates: Comma-separated west,south,east,north (or "world")
            day_range: Number of days to query (1-5)
            start_date: Optional start date in YYYY-MM-DD format
        """
        area = area_coordinates.strip().lower()
        dates = start_date or "0"  # relative to today; actual value computed server-side

        url = (
            f"{self.BASE_URL}/api/area/csv/{self.map_key}/"
            f"{self.FIRE_SOURCES['MODIS_NRT']}/{area}/{day_range}/{dates}"
        )
        response = self._request_with_retry("GET", url, headers=self._headers)
        content = response.content
        sha256 = self.compute_sha256(content)
        content_str = content.decode("utf-8-sig")  # handle BOM

        # Parse CSV
        lines = content_str.strip().splitlines()
        if len(lines) < 2:
            return pd.DataFrame()

        header = lines[0].split(",")
        records = [dict(zip(header, line.split(","))) for line in lines[1:] if line.strip()]

        df = pd.DataFrame.from_records(records)
        if not df.empty:
            df["source"] = "firms"
            df["source_product"] = self.FIRE_SOURCES["MODIS_NRT"]
            df["ingested_at"] = self._utc_now_iso()

        provenance = Provenance(
            source="firms",
            dataset_id="active_fire",
            fetched_at=self._utc_now_iso(),
            url=url,
            sha256=sha256,
            response_code=response.status_code,
            record_count=len(df),
            license="NASA FIRMS Data License",
        )

        return df

    def extract(
        self,
        area_coordinates: str = "world",
        day_range: int = 1,
        start_date: str | None = None,
        partition: str | None = None,
    ) -> tuple[pd.DataFrame, Provenance]:
        """Extract FIRMS fire detections and write to Bronze."""
        df = self.fetch_area_csv(area_coordinates=area_coordinates, day_range=day_range, start_date=start_date)

        provenance = Provenance(
            source="firms",
            dataset_id="active_fire",
            fetched_at=self._utc_now_iso(),
            url=f"{self.BASE_URL}/api/area/csv/{self.map_key}",
            sha256=hashlib.sha256(str(len(df)).encode()).hexdigest(),
            response_code=200,
            record_count=len(df),
            license="NASA FIRMS Data License",
        )

        self.write_bronze(df, provenance, "active_fire", partition=partition)
        return df, provenance
