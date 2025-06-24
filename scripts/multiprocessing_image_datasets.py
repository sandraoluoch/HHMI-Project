
"""
This script downloads image datasets stored in different formats in a parallelized, multi-threaded
manner.

The overall goal of this project is to download 3D microscopy image datasets from different
sources accessible and create an entry table of the various image metadata. 
"""
import os
import time
import logging
from tqdm.contrib.concurrent import process_map
from functools import partial
from img_dataset_tools.webscrapers import (url_image_scrape_dynamic, url_image_scrape_static,
                                  url_image_scrape_selenium, url_image_scrape_zarr, 
                                  url_image_scrape_neuroglancer)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


# create wrapper function for the functions that download the different datasets
def download_dataset_wrapper(url, save_directory):
    try:
        start_time = time.time()
        logging.info(f"Starting download for: {url}")

        if "idr.openmicroscopy.org" in url:
            url_image_scrape_dynamic(url, save_directory)
        elif "ftp.ebi.ac.uk" in url:
            url_image_scrape_static(url, save_directory)
        elif "cvlab.epfl.ch" in url:
            url_image_scrape_selenium(url, save_directory)
        elif "neuroglancer" in url:
            url_image_scrape_neuroglancer(url, save_directory)
        elif "janelia-cosem-datasets" in url:
            url_image_scrape_zarr(url, save_directory)
        else:
            raise ValueError(f"This function does not support this url: {url}")
        
        end_time = time.time()
        elapsed_time = (end_time - start_time)/60
        logging.info(f"Finished {url} in {elapsed_time:.2f} minutes.")

    except Exception as e:
        logging.error(f"Failed {url}: {e}", exc_info=True)
        return f"Failed {url}: {e}"
        

if __name__ == "__main__":
    start_time = time.time()

    # count number of cpus
    cpu_count = os.cpu_count()
    logging.info(f"Number of logical CPUs: {cpu_count}")
    processes = cpu_count - 1

    # list of urls containing the five different datasets
    list_of_urls = ["https://idr.openmicroscopy.org/webclient/img_detail/9846137/?dataset=10740",
                "https://ftp.ebi.ac.uk/empiar/world_availability/11759/data/",
                "https://cvlab.epfl.ch/data/data-em/",
                "s3://janelia-cosem-datasets/jrc_mus-nacc-2/jrc_mus-nacc-2.zarr",
                "gs://neuroglancer-janelia-flyem-hemibrain/emdata/raw/jpeg"
                ]
    
    # create a "saved_datasets" folder in user's current working directory
    save_directory = os.path.join(os.getcwd(), "saved_datasets")
    os.makedirs(save_directory, exist_ok=True)

    process_map(partial(download_dataset_wrapper, save_directory=save_directory), 
                           list_of_urls, max_workers=processes)
            
    end_time = time.time()
    elapsed_time = (end_time - start_time)/3600
    logging.info(f"All downloads complete. Time elapsed: {elapsed_time:.2f} hours")