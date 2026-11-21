"""Tests for EUDR Deforestation Radar validation layer."""

from __future__ import annotations

from shapely.geometry import Point, Polygon

import geopandas as gpd
import pandas as pd
import pytest

from src.validate import (
    QualityGateConfig,
    QualityGateResult,
    run_quality_gates,
    ValidationPipeline,
    DeadLetterEntry,
)


def test_quality_gate_config_defaults():
    """Test default quality gate config."""
    config = QualityGateConfig()
    assert config.require_geometry is True
    assert config.require_valid_geometry is True
    assert config.max_invalid_ratio == 0.1


def test_quality_gates_pass():
    """Test quality gates passing for valid data."""
    gdf = gpd.GeoDataFrame(
        {"lot_id": ["LOT-001"]}, geometry=[Point(0, 0)], crs="EPSG:4326"
    )

    config = QualityGateConfig()
    result = run_quality_gates(gdf, config)

    assert result.passed is True
    assert result.errors == []
    assert result.records_checked == 1


def test_quality_gates_fail_no_geometry():
    """Test quality gates fail when geometry column missing."""
    gdf = gpd.GeoDataFrame({"lot_id": ["LOT-001"]})

    config = QualityGateConfig(require_geometry=True)
    result = run_quality_gates(gdf, config)

    assert result.passed is False
    assert "Missing geometry column" in result.errors


def test_quality_gates_fail_invalid_geometry():
    """Test quality gates fail for too many invalid geometries."""
    # Create a self-intersecting polygon (bow-tie)
    invalid_poly = Polygon([(0, 0), (1, 1), (0, 1), (1, 0)])
    gdf = gpd.GeoDataFrame(
        {"lot_id": ["LOT-001", "LOT-002"]},
        geometry=[invalid_poly, Point(0, 0)],
        crs="EPSG:4326",
    )

    config = QualityGateConfig(max_invalid_ratio=0.1)
    result = run_quality_gates(gdf, config)

    assert result.passed is False
    assert any("invalid geometries" in e for e in result.errors)


def test_quality_gates_pass_with_some_invalid_under_threshold():
    """Test quality gates pass when invalid ratio under threshold."""
    invalid_poly = Polygon([(0, 0), (1, 1), (0, 1), (1, 0)])
    gdf = gpd.GeoDataFrame(
        {"lot_id": ["LOT-001", "LOT-002", "LOT-003"]},
        geometry=[invalid_poly, Point(0, 0), Point(1, 1)],
        crs="EPSG:4326",
    )

    config = QualityGateConfig(max_invalid_ratio=0.5)
    result = run_quality_gates(gdf, config)

    assert result.passed is True


def test_dead_letter_entry():
    """Test DeadLetterEntry creation."""
    entry = DeadLetterEntry(
        original_id="test-123",
        error="Test error",
        error_type="test_type",
    )
    assert entry.original_id == "test-123"
    assert entry.error == "Test error"
    assert entry.error_type == "test_type"


def test_validation_pipeline_process():
    """Test validation pipeline process."""
    gdf = gpd.GeoDataFrame(
        {"lot_id": ["LOT-001", "LOT-002"]}, geometry=[Point(0, 0), Point(1, 1)], crs="EPSG:4326"
    )
    df = pd.DataFrame({"lot_id": ["LOT-001", "LOT-002"]})

    pipeline = ValidationPipeline()
    valid_gdf, dead_letters = pipeline.process(df, gdf, "test", "lots")

    assert len(dead_letters) == 0
    assert len(valid_gdf) == 2