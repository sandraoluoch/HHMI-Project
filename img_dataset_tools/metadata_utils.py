"""
img_dataset_tools.metadata_utils

This script contains two metadata extraction functions to be used in extract_metadata.py.

The overall goal of this project is to download 3D microscopy image datasets from different
sources accessible and create an entry table of the various image metadata. 
"""

import logging
import zarr
import os

logger = logging.getLogger(__name__)

# FUNCTION 6: FLATTENING NESTED DICTIONARY FOR .DM3 METADATA EXTRACTION

def flatten_dm3_dict(nested_dict, parent_key=""):
    """
    This function flattens a .dm3 nested dictionary to extract key-value metadata pairs. 
    """
    dm3_flat_dict = {}
    for k, value in nested_dict.items():
        key = f"{parent_key}.{k}" if parent_key else k
        if isinstance(value, dict):
            dm3_flat_dict.update(flatten_dm3_dict(value, key))
        else:
            short_key = key.split(".")[-1]
            dm3_flat_dict[short_key] = value
    
    return dm3_flat_dict

# FUNCTION 7: FLATTENING NESTED DICTIONARY FOR .ZARR METADATA EXTRACTION

def extract_zarr_metadata(zarr_root_folder):
    """
    This function finds all .zarray-containing arrays,
    and extracts metadata (including resolution + custom attrs) into a list of dicts.
    """
    metadata = []

    for root, dirs, files in os.walk(zarr_root_folder):
        if ".zarray" in files:
            try:
                z_obj = zarr.open(root, mode='r')

                zarr_rows_dict = {
                    "dataset_id": os.path.relpath(root, start=zarr_root_folder),
                    "format": "ZARR",
                    "shape": z_obj.shape,
                    "ndim": z_obj.ndim,
                    "dtype": str(z_obj.dtype),
                    "chunks": z_obj.chunks,
                    "compressor": str(z_obj.compressor),
                    "size": z_obj.size,
                    "fill_value": z_obj.fill_value,
                    "file_path": os.path.join(getattr(z_obj.store, "path", "unknown_store"), z_obj.path)
                }

                resolution_keys = ["pixelResolution", "scale", "spacing"]
                unit_keys = ["units", "unit", "resolution_unit"]

                for key in resolution_keys:
                    if key in z_obj.attrs:
                        zarr_rows_dict["resolution"] = z_obj.attrs[key]
                        break

                for key in unit_keys:
                    if key in z_obj.attrs:
                        zarr_rows_dict["resolution_unit"] = z_obj.attrs[key]
                        break

                for key, value in z_obj.attrs.items():
                    if key not in resolution_keys + unit_keys:
                        zarr_rows_dict[f"Attr_{key}"] = value

                metadata.append(zarr_rows_dict)

            except Exception as e:
                print(f"Failed to read {root}: {e}")

    return metadata

__all__ = ["flatten_dm3_dict", "extract_zarr_metadata"]
