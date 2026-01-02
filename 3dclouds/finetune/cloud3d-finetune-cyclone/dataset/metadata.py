"""
Metadata Provider - Cloud3D Cyclones

Scans cyclones directory containing both GOES-16 and Himawari-8/9 samples.
Detects satellite type from folder naming pattern:
  - G16_* → GOES-16
  - YYYYMMDD_* → Himawari-8/9

Each sample folder contains:
  - geo_patch.tif          (stored in TACO)
  - cloudsat_aligned.tif   (stored in TACO)
  - complete.nc            (not stored)
  - *_global.json          (read for metadata extraction)
"""

import json
import re
from pathlib import Path

import tacoreader
if tacoreader.__version__ < "2.0.0":
    raise ImportError(
        f"tacoreader >= 2.0.0 required (found {tacoreader.__version__}). "
        "Run: pip install -U tacoreader"
    )

from dataset.config import DATAFRAME_BACKEND

tacoreader.use(DATAFRAME_BACKEND)

# Data directory
DATA_DIR = Path("/data/databases/CLOUD_3D/finetune/geotiff/cyclones")

# Patterns to detect satellite type
GOES_PATTERN = re.compile(r"^G16_")
HIMAWARI_PATTERN = re.compile(r"^\d{8}_")  # YYYYMMDD_


def detect_satellite(folder_name: str) -> str:
    """Detect satellite type from folder name."""
    if GOES_PATTERN.match(folder_name):
        return "GOES-16"
    elif HIMAWARI_PATTERN.match(folder_name):
        return "Himawari"
    else:
        raise ValueError(f"Unknown satellite pattern: {folder_name}")


def load_global_json(folder: Path) -> dict:
    """Load *_global.json from sample folder."""
    global_files = list(folder.glob("*_global.json"))
    if not global_files:
        return {}
    with open(global_files[0], "r") as f:
        return json.load(f)


def load_contexts(limit: float | int | None = None) -> list[dict]:
    """
    Load cyclone dataset metadata.

    Returns context dicts with:
      - id: folder name (unique sample ID)
      - path: folder path as bytes
      - satellite_type: "GOES-16" or "Himawari"
      - goes_id / himawari_id: satellite-specific ID from global.json
      - cloudsat_id: CloudSat granule ID from global.json
      - has_flxhr: whether FLXHR data is available
    """
    contexts = []
    
    for folder in sorted(DATA_DIR.iterdir()):
        if not folder.is_dir():
            continue
        
        folder_name = folder.name
        satellite_type = detect_satellite(folder_name)
        
        # Load metadata from global.json
        global_meta = load_global_json(folder)
        
        ctx = {
            "id": folder_name,
            "path": str(folder).encode(),
            "satellite_type": satellite_type,
            "has_flxhr": "no_flxhr" not in folder_name,
        }
        
        # Extract fields from global.json if available
        attrs = global_meta.get("attributes", {})
        
        if satellite_type == "GOES-16":
            ctx["goes_id"] = attrs.get("goes_id", folder_name)
            ctx["himawari_id"] = None
        else:
            ctx["himawari_id"] = attrs.get("himawari_id", folder_name)
            ctx["goes_id"] = None
        
        ctx["cloudsat_id"] = attrs.get("cloudsat_id")
        ctx["start"] = attrs.get("start")  # Timestamp for STAC
        
        contexts.append(ctx)
    
    # Apply limit
    if limit is None:
        return contexts
    elif isinstance(limit, float):
        count = int(len(contexts) * limit) or 1
        return contexts[:count]
    else:
        return contexts[:limit]


if __name__ == "__main__":
    contexts = load_contexts()
    
    goes_count = sum(1 for c in contexts if c["satellite_type"] == "GOES-16")
    himawari_count = sum(1 for c in contexts if c["satellite_type"] == "Himawari")
    flxhr_count = sum(1 for c in contexts if c["has_flxhr"])
    
    print(f"Loaded {len(contexts)} contexts")
    print(f"  GOES-16:  {goes_count}")
    print(f"  Himawari: {himawari_count}")
    print(f"  With FLXHR: {flxhr_count}")
    print(f"\nFirst context: {contexts[0]}")
    print(f"Last context:  {contexts[-1]}")