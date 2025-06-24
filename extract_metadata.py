"""
This script extracts metadata information from different 3D microscopy image datasets.
The datasets are assumed to have been previously downloaded to a local directory using 
the download_image_datasets.py script.

The goal is to generate a unified metadata table containing image format, shape, resolution,
data type, and other useful information for downstream AI/ML pipelines.
"""

import os
import tifffile
import dm3_lib as dm3
import glob
import pandas as pd
from img_dataset_tools.metadata_utils import flatten_dm3_dict, extract_zarr_metadata

load_directory = os.path.join(os.getcwd(), "saved_datasets")


dataset_folders = [] 

for subfolder in os.listdir(load_directory):
    folder_path = os.path.join(load_directory, subfolder)
    if os.path.isdir(folder_path) and not subfolder.startswith("."):
         dataset_folders.append(folder_path)

metadata_list = []


for folder in dataset_folders:
    # extracting .tif/.tiff metadata
    for tif_file in glob.glob(f"{folder}/*.tif") + glob.glob(f"{folder}/*.tiff"):
        with tifffile.TiffFile(tif_file) as tif:
            page = tif.pages[0]  
            series = tif.series[0]
            tags = page.tags

            # for resolution
            x_tag = tags.get("XResolution")
            y_tag = tags.get("YResolution")
            unit_tag = tags.get("ResolutionUnit")

            # Safe resolution extraction
            x_res = (x_tag.value[0] / x_tag.value[1]) if x_tag and isinstance(x_tag.value, tuple) else (x_tag.value if x_tag else None)
            y_res = (y_tag.value[0] / y_tag.value[1]) if y_tag and isinstance(y_tag.value, tuple) else (y_tag.value if y_tag else None)

            tif_rows_dict = {
                "dataset_id": os.path.basename(tif_file).split(".")[-2], 
                "format": "TIFF",
                "shape": series.shape,
                "dtype": series.dtype,
                "ndims": series.ndim,
                "resolution": (x_res, y_res),
                "file_size_MB": f"{os.path.getsize(tif_file)/(1e6):.2f}",
                "samples_per_pixel": page.samplesperpixel,
                "file_path": tif_file
            }

            metadata_list.append(tif_rows_dict)

    # extracting .dm3 metadata
    for dm3_file in glob.glob(f"{folder}/*.dm3"):
        dm3_data = dm3.DM3(dm3_file)
        dm3_array = dm3_data.imagedata
        dm3_flattened = flatten_dm3_dict(dm3_data.tags)
           
        dm3_rows_dict = {
            "dataset_id": os.path.basename(dm3_file).split(".")[-2],
            "format": "DM3",
            "shape": dm3_data.imagedata.shape,
            "dtype": str(dm3_array.dtype),
            "resolution": dm3_flattened.get("ImageData.Calibrations.Dimension"),
            "file_size_MB": f"{os.path.getsize(dm3_file)/(1e6)}",
            "pixel_size": (dm3_flattened.get("Pixel size")),
            "size": dm3_flattened.get("Size"),
            "channel": dm3_array.shape[-1] if dm3_array.ndim >= 3 else 1,
            "zoom_ratio": dm3_flattened.get("Zoom ratio"),
            "ndims": dm3_data.imagedata.ndim,
            "chunks": dm3_flattened.get("chunking"),
            "file_path": dm3_file
        }  

        metadata_list.append(dm3_rows_dict)


# extract zarr metadata
zarr_path = os.path.join(os.getcwd(), "saved_datasets", "jrc_mus-nacc-2.zarr")
metadata_list.extend(extract_zarr_metadata(zarr_path))

# convert to dataframe
metadata_table = pd.DataFrame(metadata_list)
print(metadata_table)

# move 'file_path' to the last column 
if "file_path" in metadata_table.columns:
    cols = [col for col in metadata_table.columns if col != "file_path"] + ["file_path"]
    metadata_table = metadata_table[cols] 

# save as csv
metadata_table.to_csv("metadata_table.csv", index=False)
