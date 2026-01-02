"""
Metadata Provider

Loads MSG/SEVIRI-CloudSat colocated pairs for finetuning dataset.

Each context represents one MSG directory containing:
    - geo_patch.tif: SEVIRI imagery (11 spectral bands + satellite/solar angles + mask)
    - cloudsat_aligned.tif: CloudSat ground truth profile
    - *_global.json: Metadata (read for attributes, not stored)
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

# Root path to MSG/SEVIRI-CloudSat colocated data
ROOT_PATH = Path("/data/databases/CLOUD_3D/finetune/geotiff/cloudsat_msg/")


def load_contexts(limit: float | int | None = None) -> list[dict]:
    """
    Load MSG/SEVIRI-CloudSat directory paths as contexts.
    
    Args:
        limit: Optional limit for debugging
            - None: all samples
            - float (0-1): fraction of samples
            - int: exact number of samples
    
    Returns:
        List of context dicts with keys:
            - id: unique sample identifier (folder name)
            - path: Path to sample directory
    """
    # Scan for MSG sample directories (pattern: MSG*_*)
    sample_dirs = sorted([
        d for d in ROOT_PATH.iterdir() 
        if d.is_dir() and d.name.startswith("MSG")
    ])
    
    # Apply limit if specified
    if limit is not None:
        if isinstance(limit, float) and 0 < limit < 1:
            n_samples = int(len(sample_dirs) * limit)
            sample_dirs = sample_dirs[:n_samples]
        elif isinstance(limit, int) and limit > 0:
            sample_dirs = sample_dirs[:limit]
    
    # Build contexts
    contexts = [
        {
            "id": d.name,
            "path": d,
        }
        for d in sample_dirs
    ]
    
    return contexts


if __name__ == "__main__":
    # Quick test
    contexts = load_contexts(limit=5)
    print(f"Found {len(contexts)} samples")
    for ctx in contexts[:3]:
        print(f"  - {ctx['id']}")