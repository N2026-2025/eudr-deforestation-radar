"""DDS (Due Diligence Statement) Generation aligned to EU Information System v3 schema."""

from __future__ import annotations

import base64
import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Any

from src.load import DuckDBLoader
from src.transform import GeometryTransformer, SilverTransformer

logger = logging.getLogger(__name__)


class DDSGenerator:
    """Generates EUDR-compliant Due Diligence Statements aligned to EU IS DDS v3."""

    DDS_VERSION = "3"
    EU_IS_BASE_URL = "https://api.trades.ec.europa.eu"

    def __init__(
        self,
        eu_is_base_url: str = EU_IS_BASE_URL,
        operator_id: str = "unknown",
        transformer: SilverTransformer | None = None,
    ) -> None:
        self.eu_is_base_url = eu_is_base_url
        self.operator_id = operator_id
        self.transformer = transformer or SilverTransformer()

    def _utc_now_iso(self) -> str:
        return datetime.now(timezone.utc).isoformat()

    def _geojson_from_geometry(
        self,
        geometry: Any,
    ) -> dict[str, Any]:
        """Convert shapely geometry to GeoJSON dict."""
        from shapely.geometry import mapping

        if geometry is None:
            return {
                "type": "Point",
                "coordinates": [0, 0],
            }
        try:
            return mapping(geometry)
        except Exception:
            return {
                "type": "Polygon",
                "coordinates": [
                    [
                        [0, 0],
                        [1, 0],
                        [1, 1],
                        [0, 0],
                    ],
                ],
            }

    def generate_dds_for_lot(
        self,
        lot_data: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Generate a Due Diligence Statement for a single lot.

        Args:
            lot_data: Lot information including geometry, commodity, etc.

        Returns:
            DDS payload aligned to EU IS DDS v3 schema.
        """
        lot_id = lot_data.get("lot_id", str(uuid.uuid4()))

        # Determine risk level
        risk_level = self._assess_risk(lot_data)

        commodity = lot_data.get("commodity", "soy")
        hs_heading = lot_data.get("hs_heading", "1201")
        species_info = lot_data.get("species_info") or {"scientific": None, "common": commodity}

        producer = lot_data.get("producer") or {
            "position": 1,
            "country": lot_data.get("country_of_production", "AR"),
            "name": self.operator_id,
            "geometryGeojson": self._geojson_from_geometry(lot_data.get("geometry")),
        }

        commodity_obj = {
            "position": 1,
            "descriptors": {
                "commercialDescription": lot_data.get("commercial_description") or commodity,
                "quantity": lot_data.get("quantity", 0.0),
                "hsHeading": hs_heading,
            },
            "hsHeading": hs_heading,
            "speciesInfo": species_info,
            "producers": [producer],
        }

        dds_payload = {
            "ddsVersion": self.DDS_VERSION,
            "ddsType": "due-diligence-statement-v3",
            "internalReferenceNumber": f"DDS-{uuid.uuid4().hex[:12].upper()}",
            "activityType": lot_data.get("activity_type", "IMPORT"),
            "representedOperator": {
                "operatorId": self.operator_id,
            },
            "countryOfActivity": lot_data.get("country_of_activity", "EU"),
            "borderCrossCountry": lot_data.get("border_cross_country", True),
            "commodities": [commodity_obj],
            "geoLocationConfidential": lot_data.get("geo_location_confidential", False),
            "riskAssessment": {
                "riskLevel": risk_level,
                "assessmentDate": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
            },
            "metadata": {
                "generatedAt": self._utc_now_iso(),
                "source": "eudr-deforestation-radar",
                "cutoffDate": "2020-12-31",
            },
        }

        return dds_payload

    def _assess_risk(self, lot_data: dict[str, Any]) -> str:
        """Assess risk level for a lot based on compliance flags."""
        has_post_cutoff_alert = lot_data.get("has_post_cutoff_alert", False)
        has_recent_fire = lot_data.get("has_recent_fire", False)
        eudr_compliant = lot_data.get("eudr_compliant", True)

        if not eudr_compliant:
            return "high"

        if has_post_cutoff_alert and has_recent_fire:
            return "high"

        if has_post_cutoff_alert:
            return "medium"

        if has_recent_fire:
            return "medium"

        country_risk = lot_data.get("country_risk", "standard")
        if country_risk == "high":
            return "high"

        if country_risk == "low":
            return "low"

        return "low"

    def generate_dds_batch(
        self,
        lots: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Generate DDS payloads for multiple lots."""
        return [self.generate_dds_for_lot(lot) for lot in lots]

    def generate_dds_json_string(self, dds_payload: dict[str, Any]) -> str:
        """Serialize DDS payload to JSON string for API submission."""
        return json.dumps(dds_payload, indent=2, default=str)

    def prepare_dds_for_api_submission(
        self,
        dds_payload: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Prepare DDS payload for submission to EU Information System.
        The DDS v3 API expects specific field names including base64-encoded GeoJSON.
        """
        # The actual EU API expects specific schema; this prepares the payload
        # with the expected field structure per DDS v3.
        prepared = dict(dds_payload)
        prepared["ddsPayload"] = base64.b64encode(
            json.dumps(dds_payload).encode("utf-8")
        ).decode("utf-8")

        return prepared
