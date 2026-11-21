"""UN Comtrade Public API Extractor."""

from __future__ import annotations

import hashlib
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import requests

from .base import BaseExtractor, Provenance

logger = logging.getLogger(__name__)


class ComtradeExtractor(BaseExtractor):
    """Extractor for UN Comtrade Public API."""

    BASE_URL = "https://comtradeapi.un.org"

    def __init__(
        self,
        api_key: str | None = None,
        output_dir: Path | str | None = None,
    ) -> None:
        super().__init__("comtrade", output_dir or Path(".data/bronze/comtrade"))
        self.api_key = api_key
        self._headers: dict[str, str] = {}
        if api_key:
            self._headers["Ocp-Apim-Subscription-Key"] = api_key

    def _utc_now_iso(self) -> str:
        return datetime.now(timezone.utc).isoformat()

    def get_export_volumes(
        self,
        reporter_code: str,
        partner_code: str = "0",
        cmd_code: str = "TOTAL",
        period: str = "latest",
        flow_code: str = "X",
        frequency: str = "A",
    ) -> pd.DataFrame:
        """
        Get export volumes from UN Comtrade.

        Args:
            reporter_code: UN M49 code of reporting country
            partner_code: UN M49 code of partner country (0 = world)
            cmd_code: HS commodity code
            period: Year or YYYYMM
            flow_code: M=Import, X=Export
            frequency: A=Annual, M=Monthly
        """
        try:
            url = (
                f"{self.BASE_URL}/public/v1/preview/C/{frequency}/HS"
                f"?reporterCode={reporter_code}"
                f"&partnerCode={partner_code}"
                f"&cmdCode={cmd_code}"
                f"&period={period}"
                f"&flowCode={flow_code}"
            )

            response = self._request_with_retry("GET", url, headers=self._headers)
            content = response.content
            sha256 = self.compute_sha256(content)
            data = response.json()

            records = data.get("data", [])
            df = pd.DataFrame(records)

            if not df.empty:
                df["source"] = "comtrade"
                df["source_type"] = "export_volumes"
                df["ingested_at"] = self._utc_now_iso()

            return df
        except Exception as e:
            logger.warning("Comtrade API request failed: %s", str(e))
            return pd.DataFrame()

    def extract(
        self,
        reporter_code: str = "76",  # Argentina
        partner_code: str = "288",  # EU27
        cmd_code: str = "1201",     # Soybeans
        period: str = "latest",
        partition: str | None = None,
    ) -> tuple[pd.DataFrame, Provenance]:
        """Extract Comtrade export volumes and write to Bronze."""
        df = self.get_export_volumes(
            reporter_code=reporter_code,
            partner_code=partner_code,
            cmd_code=cmd_code,
            period=period,
        )

        url = (
            f"{self.BASE_URL}/public/v1/preview/C/A/HS"
            f"?reporterCode={reporter_code}&partnerCode={partner_code}"
            f"&cmdCode={cmd_code}&period={period}&flowCode=X"
        )

        provenance = Provenance(
            source="comtrade",
            dataset_id="export_volumes",
            fetched_at=self._utc_now_iso(),
            url=url,
            sha256=hashlib.sha256(url.encode()).hexdigest(),
            response_code=200 if not df.empty else 400,
            record_count=len(df),
            license="UN Comtrade Public Data",
        )

        self.write_bronze(df, provenance, "export_volumes", partition=partition)
        return df, provenance


class MAGyPExtractor(BaseExtractor):
    """Extractor for Argentine MAGyP Datos Abiertos API."""

    BASE_URL = "https://apis.datos.gob.ar/series/api"

    def __init__(self, output_dir: Path | str | None = None) -> None:
        super().__init__("magyp", output_dir or Path(".data/bronze/magyp"))

    def _utc_now_iso(self) -> str:
        return datetime.now(timezone.utc).isoformat()

    def search_series(
        self,
        query: str = "soja",
        source: str = "agroindustria",
        limit: int = 100,
    ) -> dict[str, Any]:
        """Search MAGyP time series by query."""
        url = f"{self.BASE_URL}/search"
        params = {
            "q": query,
            "source": source,
            "limit": limit,
        }
        response = self._request_with_retry("GET", url, params=params)
        content = response.content
        sha256 = self.compute_sha256(content)
        data = response.json()
        return data, sha256, response.status_code

    def get_series(
        self,
        series_ids: str,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> pd.DataFrame:
        """
        Get time series data for given series IDs.

        Args:
            series_ids: Comma-separated series IDs
            start_date: ISO 8601 start date
            end_date: ISO 8601 end date
        """
        url = f"{self.BASE_URL}/series"
        params: dict[str, Any] = {"ids": series_ids}
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date

        response = self._request_with_retry("GET", url, params=params)
        content = response.content
        sha256 = self.compute_sha256(content)
        data = response.json()

        series_data = data.get("series", [])
        df = pd.DataFrame(series_data)

        if not df.empty:
            df["source"] = "magyp"
            df["ingested_at"] = self._utc_now_iso()

        return df, sha256, response.status_code, len(df)

    def extract(
        self,
        series_ids: str = "AGRO_A_Soja_0001,AGRO_A_Soja_0002,AGRO_A_Soja_0003",
        partition: str | None = None,
    ) -> tuple[pd.DataFrame, Provenance]:
        """Extract MAGyP production series and write to Bronze."""
        df, sha256, status_code, record_count = self.get_series(series_ids=series_ids)

        provenance = Provenance(
            source="magyp",
            dataset_id="production_series",
            fetched_at=self._utc_now_iso(),
            url=f"{self.BASE_URL}/series",
            sha256=sha256,
            response_code=status_code,
            record_count=record_count,
            license="Argentina MAGyP Open Data",
        )

        self.write_bronze(df, provenance, "production_series", partition=partition)
        return df, provenance