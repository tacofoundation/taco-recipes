"""
Cloud3D Custom Extensions

Sample-level extensions for the Cloud 3D dataset.
"""

from typing import Literal
import pyarrow as pa
from tacotoolbox.sample.datamodel import SampleExtension


class Satellite(SampleExtension):
    """
    Satellite sensor identifier.
    
    Identifies which geostationary satellite acquired the imagery.
    Valid values: GOES, HIMAWARI, MSG
    """
    
    satellite: Literal["GOES", "HIMAWARI", "MSG"]
    
    def get_schema(self) -> pa.Schema:
        """Return the schema for this extension."""
        return pa.schema([
            pa.field("cloud3d:satellite", pa.string()),
        ])
    
    def get_field_descriptions(self) -> dict[str, str]:
        """Return field descriptions for documentation."""
        return {
            "cloud3d:satellite": "Geostationary satellite platform (GOES, HIMAWARI, or MSG)",
        }
    
    def _compute(self, sample) -> pa.Table:
        """Compute the metadata for this sample."""
        return pa.Table.from_pydict({
            "cloud3d:satellite": [self.satellite],
        }, schema=self.get_schema())


class Cyclone(SampleExtension):
    """
    Tropical cyclone presence flag.
    
    Indicates whether the sample contains imagery of a tropical cyclone.
    """
    
    is_cyclone: bool
    
    def get_schema(self) -> pa.Schema:
        """Return the schema for this extension."""
        return pa.schema([
            pa.field("cloud3d:cyclone", pa.bool_()),
        ])
    
    def get_field_descriptions(self) -> dict[str, str]:
        """Return field descriptions for documentation."""
        return {
            "cloud3d:cyclone": "Whether the sample contains tropical cyclone imagery",
        }
    
    def _compute(self, sample) -> pa.Table:
        """Compute the metadata for this sample."""
        return pa.Table.from_pydict({
            "cloud3d:cyclone": [self.is_cyclone],
        }, schema=self.get_schema())