"""
TACO Dataset Definition

This is the final step to create your TACO dataset.

1. Calls create_tortilla() to get the root Tortilla
2. Wraps it in Taco with COLLECTION metadata (from config.py)
3. Applies TACO-level extensions (optional)

Run directly to preview COLLECTION.json before building:
    python -m dataset.taco
"""

import json

from tacotoolbox.taco.datamodel import Taco
# from tacotoolbox.taco.extensions.publications import Publications, Publication
# from dataset.extensions import DatasetStats

from dataset.config import COLLECTION, LEVEL0_SAMPLE_LIMIT
from dataset.tortilla import create_tortilla
from dataset.metadata import load_contexts


def create_taco(contexts: list[dict] | None = None) -> Taco:
    """
    Create complete TACO from Tortilla + COLLECTION metadata.
    
    Args:
        contexts: List of context dicts, if None uses load_contexts(LEVEL0_SAMPLE_LIMIT)
    
    Returns:
        Taco: Complete TACO dataset
    """
    if contexts is None:
        contexts = load_contexts(limit=LEVEL0_SAMPLE_LIMIT)
    
    print(f"Getting root Tortilla with {len(contexts)} contexts...")
    root_tortilla = create_tortilla(contexts)

    print("Creating TACO with COLLECTION metadata...")
    taco = Taco(tortilla=root_tortilla, **COLLECTION)

    # TACO-level extensions - dataset-wide metadata
    # Uncomment extensions as needed:
    
    # taco.extend_with(DatasetStats())
    # taco.extend_with(Publications(publications=[
    #     Publication(
    #         doi="10.1038/s41586-021-03819-2",
    #         citation="Smith et al. (2023). Dataset Name. Nature.",
    #         summary="Introduces dataset methodology"
    #     )
    # ]))

    return taco


def preview(taco: Taco) -> dict:
    """
    Generate COLLECTION.json preview from Taco object.
    
    Args:
        taco: Complete Taco object
        
    Returns:
        dict: COLLECTION.json content (without tortilla data)
    """
    return taco.model_dump(exclude={'tortilla'}, mode='json')


if __name__ == "__main__":
    contexts = load_contexts(limit=LEVEL0_SAMPLE_LIMIT or 2)
    taco = create_taco(contexts)
    
    print(f"\nID: {taco.id}")
    print(f"Version: {taco.dataset_version}")
    print(f"Root samples: {len(taco.tortilla.samples)}")
    
    print("\nCOLLECTION.json:")
    print(json.dumps(preview(taco), indent=2, default=str))