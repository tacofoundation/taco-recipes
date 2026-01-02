"""
Metadata Provider

This module loads your dataset metadata and provides it as contexts.

A "context" is a dict containing all information needed to build one root sample.
- REQUIRED: "id" field (unique identifier)
- OPTIONAL: any other fields your levels need (paths, coordinates, dates, etc.)

How contexts flow through TACO:
1. load_contexts() returns list[dict]
2. level0.build() iterates over all contexts
3. Each context is passed to level1.build() → level2.build() → ... → leaf level
4. Levels use context fields to locate files, apply extensions, build samples

The limit parameter enables testing with a subset of your data.

Usage:
    from dataset.metadata import load_contexts

    # Load all contexts
    contexts = load_contexts()

    # Load subset for testing
    contexts = load_contexts(limit=10)
"""

import tacoreader
if tacoreader.__version__ < "2.0.0":
    raise ImportError(
        f"tacoreader >= 2.0.0 required (found {tacoreader.__version__}). "
        "Run: pip install -U tacoreader"
    )

from pathlib import Path
from dataset.config import DATAFRAME_BACKEND

tacoreader.use(DATAFRAME_BACKEND)

# Root path to GOES-CloudSat colocated data
ROOT_PATH = Path("/data/databases/CLOUD_3D/finetune/geotiff/cloudsat_goes/")


def load_contexts(limit: float | int | None = None) -> list[dict]:
    """
    Load GOES-CloudSat directory paths as contexts.

    Args:
        limit: Optional limit for contexts
               - None: all contexts
               - float (0.0-1.0): percentage
               - int: exact count

    Returns:
        list[dict]: One dict per GOES directory
            - "id": directory name (e.g., "G16_xxx_001")
            - "path": Path object to directory
    """
    # Scan for GOES directories
    goes_dirs = sorted(ROOT_PATH.glob("G16_*"))
    
    contexts = [
        {"id": d.name, "path": d}
        for d in goes_dirs
        if d.is_dir()
    ]
    
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
    print(f"Loaded {len(contexts)} GOES-CloudSat contexts")
    if contexts:
        print(f"First: {contexts[0]}")
        print(f"Last: {contexts[-1]}")