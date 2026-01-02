"""
Dataset Extensions - Cloud3D Cyclones

Two extensions:
1. Cloud3DMetadata - Base satellite/CloudSat metadata (same as finetune)
2. Cloud3DCycloneMetadata - IBTrACS cyclone-specific metadata
"""

import json
import re
from pathlib import Path
from typing import Literal

import pyarrow as pa
from pydantic import Field

from tacotoolbox.sample.datamodel import SampleExtension


class Cloud3DMetadata(SampleExtension):
    """
    Cloud3D dataset-specific metadata for satellite-CloudSat colocated samples.

    Fields:
        - cloud3d:satellite: Geostationary satellite source (GOES, Himawari, MSG)
        - cloud3d:geostationary_id: Original geostationary satellite file ID
        - cloud3d:cloudsat_id: CloudSat granule/profile ID
        - cloud3d:has_flxhr: Whether 2B-FLXHR radiative flux/heating rate data is available
    """

    satellite: Literal["GOES", "Himawari", "MSG"] = Field(
        description="Geostationary satellite source"
    )
    geostationary_id: str = Field(
        description="Original geostationary satellite file identifier"
    )
    cloudsat_id: str = Field(
        description="CloudSat granule/profile identifier"
    )
    has_flxhr: bool = Field(
        description="Whether 2B-FLXHR radiative flux/heating rate data is available"
    )

    def get_schema(self) -> pa.Schema:
        return pa.schema([
            pa.field("cloud3d:satellite", pa.string()),
            pa.field("cloud3d:geostationary_id", pa.string()),
            pa.field("cloud3d:cloudsat_id", pa.string()),
            pa.field("cloud3d:has_flxhr", pa.bool_()),
        ])

    def get_field_descriptions(self) -> dict[str, str]:
        return {
            "cloud3d:satellite": "Geostationary satellite source (GOES, Himawari, MSG)",
            "cloud3d:geostationary_id": "Original geostationary satellite file identifier",
            "cloud3d:cloudsat_id": "CloudSat granule/profile identifier",
            "cloud3d:has_flxhr": "Whether 2B-FLXHR radiative flux/heating rate data is available",
        }

    def _compute(self, sample) -> pa.Table:
        data = {
            "cloud3d:satellite": [self.satellite],
            "cloud3d:geostationary_id": [self.geostationary_id],
            "cloud3d:cloudsat_id": [self.cloudsat_id],
            "cloud3d:has_flxhr": [self.has_flxhr],
        }
        return pa.Table.from_pydict(data, schema=self.get_schema())

    @classmethod
    def from_directory(
        cls,
        directory: Path,
        satellite: Literal["GOES", "Himawari", "MSG"],
    ) -> "Cloud3DMetadata":
        """Create from sample directory containing *_global.json."""
        json_files = list(directory.glob("*_global.json"))
        if not json_files:
            raise FileNotFoundError(f"No *_global.json found in {directory}")
        
        with open(json_files[0], "r") as f:
            metadata = json.load(f)
        
        attrs = metadata["attributes"]
        geostationary_id = Path(attrs["satellite_filename"]).stem
        cloudsat_id = Path(attrs["cloudsat_filename"]).stem
        has_flxhr = "no_flxhr" not in directory.name
        
        return cls(
            satellite=satellite,
            geostationary_id=geostationary_id,
            cloudsat_id=cloudsat_id,
            has_flxhr=has_flxhr,
        )


class Cloud3DCycloneMetadata(SampleExtension):
    """
    IBTrACS cyclone metadata for Cloud3D tropical cyclone samples.

    Fields extracted from *_global.json:
        - cyclone:storm_id: IBTrACS SID (e.g., "2019074S08151")
        - cyclone:center_lat: Cyclone center latitude
        - cyclone:center_lon: Cyclone center longitude
        - cyclone:dist_km: Distance from patch center to cyclone center (km)
        - cyclone:delta_t_seconds: Temporal offset between satellite and CloudSat (seconds)
    """

    storm_id: str = Field(
        description="IBTrACS storm identifier (SID)"
    )
    center_lat: float = Field(
        description="Cyclone center latitude from IBTrACS"
    )
    center_lon: float = Field(
        description="Cyclone center longitude from IBTrACS"
    )
    dist_km: float = Field(
        description="Distance from patch center to cyclone center in kilometers"
    )
    delta_t_seconds: float = Field(
        description="Temporal offset between geostationary and CloudSat observations (seconds)"
    )

    def get_schema(self) -> pa.Schema:
        return pa.schema([
            pa.field("cyclone:storm_id", pa.string()),
            pa.field("cyclone:center_lat", pa.float64()),
            pa.field("cyclone:center_lon", pa.float64()),
            pa.field("cyclone:dist_km", pa.float64()),
            pa.field("cyclone:delta_t_seconds", pa.float64()),
        ])

    def get_field_descriptions(self) -> dict[str, str]:
        return {
            "cyclone:storm_id": "IBTrACS storm identifier (SID)",
            "cyclone:center_lat": "Cyclone center latitude from IBTrACS",
            "cyclone:center_lon": "Cyclone center longitude from IBTrACS",
            "cyclone:dist_km": "Distance from patch center to cyclone center in kilometers",
            "cyclone:delta_t_seconds": "Temporal offset between geostationary and CloudSat observations (seconds)",
        }

    def _compute(self, sample) -> pa.Table:
        data = {
            "cyclone:storm_id": [self.storm_id],
            "cyclone:center_lat": [self.center_lat],
            "cyclone:center_lon": [self.center_lon],
            "cyclone:dist_km": [self.dist_km],
            "cyclone:delta_t_seconds": [self.delta_t_seconds],
        }
        return pa.Table.from_pydict(data, schema=self.get_schema())

    @classmethod
    def from_directory(cls, directory: Path) -> "Cloud3DCycloneMetadata":
        """Create from sample directory containing *_global.json with IBTrACS data."""
        json_files = list(directory.glob("*_global.json"))
        if not json_files:
            raise FileNotFoundError(f"No *_global.json found in {directory}")
        
        with open(json_files[0], "r") as f:
            metadata = json.load(f)
        
        attrs = metadata["attributes"]
        
        # Normalize longitude to [-180, 180] (IBTrACS uses [0, 360] for some basins)
        lon = attrs["LON"]
        if lon > 180:
            lon = lon - 360
        
        return cls(
            storm_id=attrs["SID"],
            center_lat=attrs["LAT"],
            center_lon=lon,
            dist_km=attrs["dist_km"],
            delta_t_seconds=abs(attrs.get("abs_delta_t_s", attrs.get("delta_t", 0))),
        )