"""
This script extracts metadata information from different image datasets. The image datasets were
previously downloaded to a local directory using the download_image_datasets.py script.

The overall goal of this project is to download 3D microscopy image datasets from different
sources accessible and create an entry table of the various image metadata. 
"""

import os
from PIL import Image
import tifffile
from ncempy.io import dm
import dm3_lib as dm3
import zarr
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
            
            tif_rows_dict = {
                "name": os.path.basename(tif_file).split(".")[-2], 
                "format": "TIFF",
                "shape": series.shape,
                "dtype": series.dtype,
                "ndims": series.ndim,
                "file_size_MB": f"{os.path.getsize(tif_file)/(1e6):.2f}",
                "samples_per_pixel": page.samplesperpixel
            }

            # print(f"Non-empty metadata keys in {os.path.basename(tif_file)}:")
            # for key, value in tif_rows_dict.items():
            #     if value not in (None, '', [], {}):
            #         print(f" - {key}: {value}")
            # break

            metadata_list.append(tif_rows_dict)

    # extracting .dm3 metadata
    for dm3_file in glob.glob(f"{folder}/*.dm3"):
        dm3_data = dm3.DM3(dm3_file)
        dm3_flattened = flatten_dm3_dict(dm3_data.tags)
           
        dm3_rows_dict = {
            "name": os.path.basename(dm3_file).split(".")[-2],
            "format": "DM3",
            "shape": dm3_flattened.get("ImageList.ImageData"),
            "dtype": dm3_flattened.get("ImageData.DataType"),
            "file_size_MB": f"{os.path.getsize(dm3_file)/(1e6)}",
            "pixel_size": (dm3_flattened.get("Pixel size")),
            "size": dm3_flattened.get("Size"),
            "channel": dm3_flattened.get("Channel"),
            "zoom_ratio": dm3_flattened.get("Zoom ratio"),
            "chunking": dm3_flattened.get("chunking")
        }
       
        # print(f"Non-empty metadata keys in {os.path.basename(dm3_file)}:")
        # for key, value in dm3_flattened.items():
        #     if value not in (None, '', [], {}):
        #         print(f" - {key}: {value}")
        # break
        
        metadata_list.append(dm3_rows_dict)


    # extracting .zarr metadata 
    for subfolder in glob.glob(f"{folder}/*.zarr"):
        try:
            z = zarr.open(subfolder, mode='r')  # handles both group and array cases
            zarr_metadata = extract_zarr_metadata(z, base_path="") # list of dictionaries

            # if zarr_metadata:
            #     first_array = zarr_metadata[0]
            #     print(f"Non-empty metadata keys for first Zarr array in {os.path.basename(subfolder)}:")
            #     for key, value in first_array.items():
            #         if value not in (None, '', [], {}, 'None'):
            #             print(f" - {key}: {value}")
            # break  

            
            metadata_list.extend([row for row in zarr_metadata])
            
        except Exception as e:
            print(f"Failed to read {subfolder}: {e}")

      
# convert to table
metadata_table = pd.DataFrame(metadata_list)

# save as csv
metadata_csv = metadata_table.to_csv("metadata_table.csv", index=False)