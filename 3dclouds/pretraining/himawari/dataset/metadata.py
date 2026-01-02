"""
Metadata Provider

This module loads HIMAWARI GeoTIFF files and provides them as contexts.

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

    # Load 10% for testing
    contexts = load_contexts(limit=0.1)

    # Load first 10 for testing
    contexts = load_contexts(limit=10)
"""

from pathlib import Path


def load_contexts(limit: float | int | None = None) -> list[dict]:
    """
    Load HIMAWARI GeoTIFF files and return list of context dicts.

    Scans two directories:
    - /data/databases/CLOUD_3D/pretraining/geotiff/himawari/
    - /data/databases/CLOUD_3D/pretraining/geotiff/cyclones/himawari/

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
        - "is_cyclone": bool - True if from /cyclones/ directory
    """
    
    root = Path("/data/databases/CLOUD_3D/pretraining/geotiff/")
    
    # Scan both directories
    himawari_path = root / "himawari"
    cyclones_path = root / "cyclones" / "himawari"
    
    tif_files = []
    
    # Add regular HIMAWARI files
    if himawari_path.exists():
        tif_files.extend(list(himawari_path.rglob("*.tif")))
    
    # Add cyclone HIMAWARI files
    if cyclones_path.exists():
        tif_files.extend(list(cyclones_path.rglob("*.tif")))
    
    # Sort for consistent ordering
    tif_files = sorted(tif_files)
    
    # Build contexts
    contexts = []
    for tif_file in tif_files:
        contexts.append({
            "id": tif_file.stem,
            "path": tif_file,
            "is_cyclone": "/cyclones/" in str(tif_file),
        })
    
    # Apply limit
    if limit is None:
        return contexts
    elif isinstance(limit, float):
        # Percentage (0.0-1.0)
        count = int(len(contexts) * limit) or 1
        return contexts[:count]
    else:
        # Exact count
        return contexts[:limit]


if __name__ == "__main__":
    contexts = load_contexts()
    print(f"Loaded {len(contexts)} contexts")
    print(f"First context: {contexts[0]}")