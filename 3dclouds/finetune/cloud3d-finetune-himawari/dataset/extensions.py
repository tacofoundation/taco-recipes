"""
Dataset Extensions

Extensions add metadata to your TACO dataset at different levels:

1. SampleExtension - Per-file or per-folder metadata
   Applied in levelN.py: sample.extend_with(MyExtension(...))
   
2. TortillaExtension - Computed across all samples in a tortilla
   Applied in tortilla.py: tortilla.extend_with(MyExtension())
   
3. TacoExtension - Dataset-wide metadata
   Applied in taco.py: taco.extend_with(MyExtension())

Each extension must implement 3 methods:
- get_schema() -> defines the PyArrow schema (field names + types)
- get_field_descriptions() -> human-readable description of each field
- _compute() -> returns PyArrow Table with the actual metadata values
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
    Cloud3D dataset-specific metadata for GOES-CloudSat colocated samples.

    Fields:
        - cloud3d:cyclone: Whether this sample is from a tropical cyclone
        - cloud3d:satellite: Geostationary satellite source (GOES, Himawari, MSG)
        - cloud3d:geostationary_id: Original geostationary satellite file ID
        - cloud3d:cloudsat_id: CloudSat granule/profile ID
        - cloud3d:has_flxhr: Whether 2B-FLXHR radiative flux/heating rate data is available
    """

    cyclone: bool = Field(
        description="Whether this sample is from a tropical cyclone observation"
    )
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
        description="Whether 2B-FLXHR broadband radiative flux and heating rate data is available"
    )

    def get_schema(self) -> pa.Schema:
        """Return the expected Arrow schema for this extension."""
        return pa.schema([
            pa.field("cloud3d:cyclone", pa.bool_()),
            pa.field("cloud3d:satellite", pa.string()),
            pa.field("cloud3d:geostationary_id", pa.string()),
            pa.field("cloud3d:cloudsat_id", pa.string()),
            pa.field("cloud3d:has_flxhr", pa.bool_()),
        ])

    def get_field_descriptions(self) -> dict[str, str]:
        """Return field descriptions for documentation."""
        return {
            "cloud3d:cyclone": "Whether this sample is from a tropical cyclone observation",
            "cloud3d:satellite": "Geostationary satellite source (GOES, Himawari, MSG)",
            "cloud3d:geostationary_id": "Original geostationary satellite file identifier",
            "cloud3d:cloudsat_id": "CloudSat granule/profile identifier",
            "cloud3d:has_flxhr": "Whether 2B-FLXHR radiative flux/heating rate data is available",
        }

    def _compute(self, sample) -> pa.Table:
        """Compute extension values and return PyArrow Table."""
        data = {
            "cloud3d:cyclone": [self.cyclone],
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
        satellite: Literal["GOES", "Himawari", "MSG"] = "GOES",
        is_cyclone: bool = False,
    ) -> "Cloud3DMetadata":
        """
        Create Cloud3DMetadata by reading from a sample directory.

        Args:
            directory: Path to sample directory containing *_global.json
            satellite: Geostationary satellite source
            is_cyclone: Whether this is a cyclone sample

        Returns:
            Cloud3DMetadata instance with extracted values
        """
        # Find metadata JSON
        json_files = list(directory.glob("*_global.json"))
        if not json_files:
            raise FileNotFoundError(f"No *_global.json found in {directory}")
        
        metadata_file = json_files[0]
        
        # Read JSON metadata
        with open(metadata_file, "r") as f:
            metadata = json.load(f)
        
        # Extract IDs from filenames
        geostationary_id = Path(metadata["attributes"]["satellite_filename"]).stem
        cloudsat_id = Path(metadata["attributes"]["cloudsat_filename"]).stem
        
        # Check for FLXHR availability (no_flxhr in directory name means NOT available)
        has_flxhr = re.search(r"no_flxhr", directory.name) is None
        
        return cls(
            cyclone=is_cyclone,
            satellite=satellite,
            geostationary_id=geostationary_id,
            cloudsat_id=cloudsat_id,
            has_flxhr=has_flxhr,
        )