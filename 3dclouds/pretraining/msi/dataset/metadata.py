"""
Metadata Provider

This module loads MSG GeoTIFF files and provides them as contexts.

A "context" is a dict containing all information needed to build one root sample.
- REQUIRED: "id" field (unique identifier)
- OPTIONAL: any other fields your levels need (paths, coordinates, dates, etc.)

How contexts flow through TACO:
1. load_contexts() returns list[dict]
2. level0.build() iterates over all contexts
3. Each context is passed to build_sample()
4. Levels use context fields to locate files, apply extensions, build samples

The limit parameter enables testing with a subset of your data.

Usage:
    from metadata import load_contexts

    # Load all contexts
    contexts = load_contexts()

    # Load subset for testing
    contexts = load_contexts(limit=10)
"""

from pathlib import Path

import tacoreader
if tacoreader.__version__ < "2.0.0":
    raise ImportError(
        f"tacoreader >= 2.0.0 required (found {tacoreader.__version__}). "
        "Run: pip install -U tacoreader"
    )

from dataset.config import DATAFRAME_BACKEND

# Configure DataFrame backend globally
tacoreader.use(DATAFRAME_BACKEND)


def load_contexts(limit: float | int | None = None) -> list[dict]:
    """
    Load MSG GeoTIFF files and return list of context dicts.

    Scans directory:
    - /data/databases/CLOUD_3D/pretraining/geotiff/msg/

    Args:
        limit: Optional limit for contexts
               - If None: returns all contexts
               - If float (0.0-1.0): percentage of total (e.g., 0.1 = 10%)
               - If int: exact count (e.g., 10 = first 10 contexts)

    Returns:
        list[dict]: One dict per GeoTIFF file

        Each dict contains:
        - "id": str - filename without extension
        - "path": Path - full path to .tif file
    """
    
    root = Path("/data/databases/CLOUD_3D/pretraining/geotiff/")
    msg_path = root / "msg"
    
    tif_files = []
    if msg_path.exists():
        tif_files.extend(list(msg_path.rglob("*.tif")))
    
    tif_files = sorted(tif_files)
    
    contexts = []
    for tif_file in tif_files:
        contexts.append({
            "id": tif_file.stem,
            "path": tif_file,
        })
    
    if limit is None:
        return contexts
    elif isinstance(limit, float):
        count = int(len(contexts) * limit) or 1
        return contexts[:count]
    else:
        return contexts[:limit]


if __name__ == "__main__":
    contexts = load_contexts()
    print(f"Loaded {len(contexts)} contexts")
    print(f"First context: {contexts[0]}")