"""Tests for EUDR Deforestation Radar transform layer."""

from __future__ import annotations

from shapely.geometry import Point, Polygon
from shapely import wkt

import geopandas as gpd
import pandas as pd
import pytest

from src.transform import (
    SilverTransformer,
    GeometryTransformer,
    H3Indexer,
    DeforestationDetector,
)


def test_geometry_transformer_ensure_wgs84():
    """Test CRS normalization to WGS84."""
    gdf = gpd.GeoDataFrame(
        {"id": [1]}, geometry=[Point(0, 0)], crs="EPSG:3857"
    )
    transformer = GeometryTransformer()
    result = transformer.ensure_wgs84(gdf)
    assert str(result.crs) == "EPSG:4326"


def test_geometry_transformer_repair_invalid():
    """Test geometry repair."""
    # Create a self-intersecting polygon (bow-tie)
    invalid_poly = Polygon([(0, 0), (1, 1), (0, 1), (1, 0)])
    gdf = gpd.GeoDataFrame({"id": [1]}, geometry=[invalid_poly], crs="EPSG:4326")
    assert not gdf.geometry.is_valid.all()

    transformer = GeometryTransformer()
    repaired = transformer.repair_geometry(gdf)
    assert repaired.geometry.is_valid.all()


def test_h3_indexer_latlng_to_cell():
    """Test H3 cell generation from lat/lng."""
    indexer = H3Indexer(resolution=8)
    cell = indexer.latlng_to_cell(0.0, 0.0)
    assert cell.startswith("8")  # resolution 8
    assert len(cell) == 15


def test_h3_indexer_polygon_to_cells():
    """Test H3 cell generation from polygon."""
    indexer = H3Indexer(resolution=8)
    poly = Polygon([(0, 0), (0.01, 0), (0.01, 0.01), (0, 0.01)])
    cells = indexer.polygon_to_cells(poly)
    assert len(cells) > 0


def test_silver_transformer_basic():
    """Test basic lot transformation."""
    lots_df = pd.DataFrame({
        "lot_id": ["LOT-001"],
        "geometry": [Point(-55.4, -21.0)],
        "commodity": ["soy"],
        "production_date": ["2019-08-15"],
        "country_of_production": ["AR"],
    })

    transformer = SilverTransformer()
    silver = transformer.transform_lots(lots_df)

    assert len(silver) == 1
    assert "area_ha" in silver.columns
    assert "h3_cell" in silver.columns
    assert "has_post_cutoff_alert" in silver.columns
    assert "has_recent_fire" in silver.columns
    assert "eudr_compliant" in silver.columns
    assert silver["eudr_compliant"].iloc[0] == True


def test_silver_transformer_polygon():
    """Test transformation with polygon geometry."""
    lots_df = pd.DataFrame({
        "lot_id": ["LOT-002"],
        "geometry": [Polygon([[-55.4, -21.0], [-55.3, -21.0], [-55.3, -20.9], [-55.4, -20.9]])],
        "commodity": ["beef"],
        "production_date": ["2018-06-20"],
        "country_of_production": ["AR"],
    })

    transformer = SilverTransformer()
    silver = transformer.transform_lots(lots_df)

    assert len(silver) == 1
    assert silver["area_ha"].iloc[0] > 0


def test_deforestation_detector_gfw_alerts():
    """Test GFW alert detection."""
    lots_df = pd.DataFrame({
        "lot_id": ["LOT-001"],
        "geometry": [Polygon([[-55.4, -21.0], [-55.3, -21.0], [-55.3, -20.9], [-55.4, -20.9]])],
    })
    lots_gdf = gpd.GeoDataFrame(lots_df, geometry="geometry", crs="EPSG:4326")

    alerts_df = pd.DataFrame({
        "latitude": [-20.95],
        "longitude": [-55.35],
        "alert_date": ["2021-06-15"],
        "confidence": ["high"],
        "intensity": [1.0],
    })

    detector = DeforestationDetector()
    result = detector.flag_gfw_alerts(lots_gdf, alerts_df)

    assert result["has_post_cutoff_alert"].iloc[0] == True
    assert result["alert_count"].iloc[0] == 1


def test_deforestation_detector_no_alerts_before_cutoff():
    """Test that alerts before cutoff don't flag."""
    lots_df = pd.DataFrame({
        "lot_id": ["LOT-001"],
        "geometry": [Polygon([[-55.4, -21.0], [-55.3, -21.0], [-55.3, -20.9], [-55.4, -20.9]])],
    })
    lots_gdf = gpd.GeoDataFrame(lots_df, geometry="geometry", crs="EPSG:4326")

    alerts_df = pd.DataFrame({
        "latitude": [-20.95],
        "longitude": [-55.35],
        "alert_date": ["2019-06-15"],  # Before 2020-12-31
        "confidence": ["high"],
        "intensity": [1.0],
    })

    detector = DeforestationDetector()
    result = detector.flag_gfw_alerts(lots_gdf, alerts_df)

    assert result["has_post_cutoff_alert"].iloc[0] == False