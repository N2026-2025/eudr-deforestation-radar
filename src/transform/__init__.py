"""Transform layer for EUDR Deforestation Radar."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import geopandas as gpd
import h3
import pandas as pd
import pyproj
from shapely import make_valid, wkt
from shapely.geometry import MultiPolygon, Polygon, shape
from shapely.geometry import mapping

logger = logging.getLogger(__name__)


EUDR_CUTOFF_DATE = "2020-12-31"
DEFAULT_CRS = "EPSG:4326"
DEFAULT_H3_RESOLUTION = 8


class GeometryTransformer:
    """Handles geometry normalization, validation, and area computation."""

    def __init__(self, target_crs: str = DEFAULT_CRS) -> None:
        self.target_crs = target_crs
        self._transformer = pyproj.Transformer.from_crs("EPSG:4326", target_crs, always_xy=True)

    def ensure_wgs84(self, gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """Ensure GeoDataFrame is in WGS84 (EPSG:4326)."""
        if gdf.crs is None:
            logger.warning("Geometry has no CRS, assuming WGS84")
            gdf = gdf.set_crs(DEFAULT_CRS)
        elif str(gdf.crs) != DEFAULT_CRS:
            logger.info("Reprojecting from %s to %s", gdf.crs, DEFAULT_CRS)
            gdf = gdf.to_crs(DEFAULT_CRS)
        return gdf

    def repair_geometry(self, gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """Repair invalid geometries using shapely.make_valid."""
        gdf = gdf.copy()
        gdf["geometry"] = gdf.geometry.apply(make_valid)
        return gdf

    def validate_geometry(self, gdf: gpd.GeoDataFrame) -> tuple[gpd.GeoDataFrame, list[int]]:
        """Validate geometries and return valid gdf + list of invalid indices."""
        gdf = gdf.copy()
        gdf["geometry_valid"] = gdf.geometry.apply(lambda geom: geom.is_valid)
        invalid_indices = gdf[~gdf["geometry_valid"]].index.tolist()
        valid_gdf = gdf[gdf["geometry_valid"]].drop(columns=["geometry_valid"])
        return valid_gdf, invalid_indices

    def compute_area_ha(self, gdf: gpd.GeoDataFrame) -> pd.Series:
        """Compute area in hectares using equal-area projection."""
        gdf_aea = gdf.to_crs("ESRI:54030")
        return gdf_aea.geometry.area / 10000


class H3Indexer:
    """Handles H3 spatial indexing for aggregation and joins."""

    def __init__(self, resolution: int = DEFAULT_H3_RESOLUTION) -> None:
        self.resolution = resolution

    def latlng_to_cell(self, lat: float, lng: float) -> str:
        """Convert lat/lng to H3 cell."""
        return h3.latlng_to_cell(lat, lng, self.resolution)

    def polygon_to_cells(self, polygon: Polygon | MultiPolygon) -> list[str]:
        """Get all H3 cells intersecting a polygon."""
        if polygon.geom_type == "Polygon":
            coords = list(polygon.exterior.coords)
            lat_lng_pairs = [(y, x) for x, y in coords[:-1]]
            return h3.polygon_to_cells(h3.LatLngPoly(lat_lng_pairs), self.resolution)
        elif polygon.geom_type == "MultiPolygon":
            cells = set()
            for poly in polygon.geoms:
                coords = list(poly.exterior.coords)
                lat_lng_pairs = [(y, x) for x, y in coords[:-1]]
                cells.update(h3.polygon_to_cells(h3.LatLngPoly(lat_lng_pairs), self.resolution))
            return list(cells)
        return []

    def index_geodataframe(self, gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """Add H3 cell column to GeoDataFrame."""
        gdf = gdf.copy()
        gdf["h3_cell"] = gdf.geometry.apply(self._get_representative_cell)
        return gdf

    def _get_representative_cell(self, geom: Any) -> str | None:
        """Get representative H3 cell for a geometry."""
        if geom is None or geom.is_empty:
            return None
        if geom.geom_type == "Point":
            return self.latlng_to_cell(geom.y, geom.x)
        elif geom.geom_type in ("Polygon", "MultiPolygon"):
            centroid = geom.centroid
            return self.latlng_to_cell(centroid.y, centroid.x)
        return None


class DeforestationDetector:
    """Detects post-2020 deforestation using multi-source evidence."""

    def __init__(self, cutoff_date: str = EUDR_CUTOFF_DATE) -> None:
        self.cutoff_date = pd.Timestamp(cutoff_date)

    def flag_gfw_alerts(
        self,
        lots_gdf: gpd.GeoDataFrame,
        alerts_df: pd.DataFrame,
    ) -> gpd.GeoDataFrame:
        """Flag lots with post-cutoff GFW alerts intersecting their geometry."""
        lots = lots_gdf.copy()
        lots["has_post_cutoff_alert"] = False
        lots["alert_count"] = 0
        lots["max_alert_intensity"] = 0.0

        if alerts_df.empty:
            return lots

        alerts_gdf = gpd.GeoDataFrame(
            alerts_df,
            geometry=gpd.points_from_xy(alerts_df["longitude"], alerts_df["latitude"]),
            crs=DEFAULT_CRS,
        )

        alerts_gdf["alert_date"] = pd.to_datetime(alerts_gdf["alert_date"], errors="coerce")
        post_cutoff_alerts = alerts_gdf[alerts_gdf["alert_date"] > self.cutoff_date]

        if post_cutoff_alerts.empty:
            return lots

        joined = gpd.sjoin(post_cutoff_alerts, lots[["lot_id", "geometry"]], how="inner", predicate="within")

        if not joined.empty:
            alert_counts = (
                joined.groupby("lot_id")
                .agg(
                    alert_count=("alert_date", "count"),
                    max_intensity=("intensity", "max"),
                )
                .reset_index()
            )

            # Drop original alert_count to avoid column collision after merge
            lots = lots.drop(columns=["alert_count"])
            lots = lots.merge(alert_counts, on="lot_id", how="left")
            # Fill NaN with 0 (lots without matching alerts)
            lots["alert_count"] = lots["alert_count"].fillna(0).astype(int)
            lots["max_alert_intensity"] = lots["max_intensity"].fillna(0.0)
            lots["has_post_cutoff_alert"] = lots["alert_count"] > 0

        return lots

    def flag_firms_fires(
        self,
        lots_gdf: gpd.GeoDataFrame,
        fires_df: pd.DataFrame,
    ) -> gpd.GeoDataFrame:
        """Flag lots with NASA FIRMS fires intersecting their geometry."""
        lots = lots_gdf.copy()
        lots["has_recent_fire"] = False
        lots["fire_count"] = 0
        lots["max_frp"] = 0.0

        if fires_df.empty:
            return lots

        fires_gdf = gpd.GeoDataFrame(
            fires_df,
            geometry=gpd.points_from_xy(fires_df["longitude"], fires_df["latitude"]),
            crs=DEFAULT_CRS,
        )

        fires_gdf["frp"] = pd.to_numeric(fires_gdf.get("frp", 0), errors="coerce").fillna(0)

        joined = gpd.sjoin(fires_gdf, lots[["lot_id", "geometry"]], how="inner", predicate="within")

        if not joined.empty:
            fire_counts = (
                joined.groupby("lot_id")
                .agg(
                    fire_count=("frp", "count"),
                    max_frp=("frp", "max"),
                )
                .reset_index()
            )

            lots = lots.merge(fire_counts, on="lot_id", how="left")
            lots["fire_count"] = lots["fire_count"].fillna(0).astype(int)
            lots["max_frp"] = lots["max_frp"].fillna(0.0)
            lots["has_recent_fire"] = lots["fire_count"] > 0

        return lots


class SilverTransformer:
    """Main transform pipeline: Bronze -> Silver."""

    def __init__(
        self,
        h3_resolution: int = DEFAULT_H3_RESOLUTION,
        target_crs: str = DEFAULT_CRS,
    ) -> None:
        self.geometry_transformer = GeometryTransformer(target_crs)
        self.h3_indexer = H3Indexer(h3_resolution)
        self.deforestation_detector = DeforestationDetector()

    def transform_lots(
        self,
        lots_df: pd.DataFrame,
        gfw_alerts: pd.DataFrame | None = None,
        firms_fires: pd.DataFrame | None = None,
    ) -> gpd.GeoDataFrame:
        """Transform raw lots data to Silver layer with deforestation flags."""
        if "geometry" not in lots_df.columns:
            raise ValueError("lots_df must contain 'geometry' column")

        lots_df = lots_df.copy()
        lots_df["geometry"] = lots_df["geometry"].apply(self._parse_geometry)
        lots = gpd.GeoDataFrame(lots_df, geometry="geometry", crs=DEFAULT_CRS)

        lots = self.geometry_transformer.ensure_wgs84(lots)
        lots = self.geometry_transformer.repair_geometry(lots)
        lots, invalid_idx = self.geometry_transformer.validate_geometry(lots)

        if len(invalid_idx) > 0:
            logger.warning("Removed %d invalid geometries", len(invalid_idx))

        lots["area_ha"] = self.geometry_transformer.compute_area_ha(lots)
        lots = self.h3_indexer.index_geodataframe(lots)

        # Set defaults for deforestation flags if no data provided
        if gfw_alerts is not None and not gfw_alerts.empty:
            lots = self.deforestation_detector.flag_gfw_alerts(lots, gfw_alerts)
        else:
            lots["has_post_cutoff_alert"] = False
            lots["alert_count"] = 0
            lots["max_alert_intensity"] = 0.0

        if firms_fires is not None and not firms_fires.empty:
            lots = self.deforestation_detector.flag_firms_fires(lots, firms_fires)
        else:
            lots["has_recent_fire"] = False
            lots["fire_count"] = 0
            lots["max_frp"] = 0.0

        lots["eudr_compliant"] = ~lots["has_post_cutoff_alert"] & ~lots["has_recent_fire"]

        if "production_date" in lots.columns:
            lots["production_date"] = pd.to_datetime(lots["production_date"], errors="coerce")

        lots["transformed_at"] = datetime.now(timezone.utc).isoformat()

        return lots

    def _parse_geometry(self, geom: Any) -> Any:
        """Parse geometry from various formats (GeoJSON dict, WKT string, etc.)."""
        if geom is None:
            return None
        if isinstance(geom, dict):
            return mapping(shape(geom))
        elif isinstance(geom, str):
            try:
                return wkt.loads(geom)
            except Exception:
                try:
                    return shape(json.loads(geom))
                except Exception:
                    return None
        return geom


def write_silver(gdf: gpd.GeoDataFrame, output_path: Path | str) -> None:
    """Write GeoDataFrame to Silver Parquet."""
    output_path = Path(output_path)
    output_path.mkdir(parents=True, exist_ok=True)
    gdf.to_parquet(output_path / "data.parquet", index=False)
    logger.info("Wrote Silver layer to %s (%d records)", output_path, len(gdf))


def read_silver(input_path: Path | str) -> gpd.GeoDataFrame:
    """Read GeoDataFrame from Silver Parquet."""
    return gpd.read_parquet(Path(input_path) / "data.parquet")
