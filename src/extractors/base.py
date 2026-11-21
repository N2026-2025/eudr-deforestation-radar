"""Common base classes for all extractors."""

from __future__ import annotations

import hashlib
import json
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from tenacity import retry, stop_after_attempt, wait_exponential

logger = logging.getLogger(__name__)


@dataclass
class Provenance:
    """Immutable provenance record for ingested data."""

    source: str
    dataset_id: str
    fetched_at: str
    url: str
    sha256: str
    response_code: int
    record_count: int
    license: str | None = None
    layer: Literal["bronze"] = "bronze"

    def to_dict(self) -> dict[str, Any]:
        return {
            "source": self.source,
            "dataset_id": self.dataset_id,
            "fetched_at": self.fetched_at,
            "url": self.url,
            "sha256": self.sha256,
            "response_code": self.response_code,
            "record_count": self.record_count,
            "license": self.license,
            "layer": self.layer,
        }


class BaseExtractor(ABC):
    """Base class for all data source extractors."""

    def __init__(self, source_name: str, output_dir: Path | str) -> None:
        self.source_name = source_name
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    @abstractmethod
    def extract(self, **kwargs: Any) -> tuple[pd.DataFrame, Provenance]:
        """Extract data from the source API. Returns (dataframe, provenance)."""

    def write_bronze(
        self,
        df: pd.DataFrame,
        provenance: Provenance,
        dataset_id: str,
        partition: str | None = None,
    ) -> Path:
        """Write dataframe and provenance to Bronze storage as Parquet."""
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        suffix = f"_{partition}" if partition else ""
        dir_path = self.output_dir / f"{self.source_name}_{dataset_id}"
        dir_path.mkdir(parents=True, exist_ok=True)

        data_path = dir_path / f"data_{timestamp}{suffix}.parquet"
        prov_path = dir_path / f"prov_{timestamp}{suffix}.json"

        df.to_parquet(data_path, index=False)
        prov_path.write_text(json.dumps(provenance.to_dict(), indent=2), encoding="utf-8")

        logger.info(
            "Wrote bronze data: %s (%d records), provenance: %s",
            data_path,
            len(df),
            prov_path,
        )
        return data_path

    def compute_sha256(self, content: bytes) -> str:
        """Compute SHA-256 hash of content for provenance."""
        return hashlib.sha256(content).hexdigest()

    def _utc_now_iso(self) -> str:
        return datetime.now(timezone.utc).isoformat()

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    def _request_with_retry(
        self,
        method: str,
        url: str,
        headers: dict[str, str] | None = None,
        json: dict[str, Any] | None = None,
        params: dict[str, Any] | None = None,
        timeout: int = 30,
    ) -> requests.Response:
        """HTTP request with retry logic."""
        response = requests.request(
            method=method,
            url=url,
            headers=headers,
            json=json,
            params=params,
            timeout=timeout,
        )
        response.raise_for_status()
        return response