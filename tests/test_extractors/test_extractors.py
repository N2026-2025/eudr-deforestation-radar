"""Tests for EUDR Deforestation Radar extractors."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from src.extractors import GFWExtractor, FIRMSExtractor, ComtradeExtractor, Provenance
from pathlib import Path


def test_provenance_creation():
    """Test Provenance dataclass creation."""
    prov = Provenance(
        source="test",
        dataset_id="test_dataset",
        fetched_at="2024-01-01T00:00:00Z",
        url="https://example.com",
        sha256="abc123",
        response_code=200,
        record_count=5,
    )
    assert prov.source == "test"
    assert prov.dataset_id == "test_dataset"
    assert prov.record_count == 5
    d = prov.to_dict()
    assert d["source"] == "test"


def test_gfw_extractor_init():
    """Test GFW extractor initialization."""
    extractor = GFWExtractor(api_key="test_key", output_dir=Path("/tmp/test"))
    assert extractor.api_key == "test_key"
    assert extractor.BASE_URL == "https://data-api.globalforestwatch.org"


def test_firms_extractor_init():
    """Test FIRMS extractor initialization."""
    extractor = FIRMSExtractor(map_key="test_key", output_dir=Path("/tmp/test"))
    assert extractor.map_key == "test_key"


def test_comtrade_extractor_init():
    """Test Comtrade extractor initialization."""
    extractor = ComtradeExtractor(api_key="test_key", output_dir=Path("/tmp/test"))
    assert extractor.api_key == "test_key"


def test_comtrade_empty_result():
    """Test Comtrade returns empty DataFrame on failure."""
    extractor = ComtradeExtractor(output_dir=Path("/tmp/test"))
    df = extractor.get_export_volumes(reporter_code="999", partner_code="999")
    assert isinstance(df, pd.DataFrame)