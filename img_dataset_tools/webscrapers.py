"""
img_dataset_tools.webscrapers

This script contains five functions to download image datasets stored online in different ways
to be used in multiprocessing_image_datasets.py.

The overall goal of this project is to download 3D microscopy image datasets from different
sources accessible and create an entry table of the various image metadata. 
"""
import logging
import os 
import time
import requests
from bs4 import BeautifulSoup
from idr.connections import connection
from tifffile import imwrite
import numpy as np
import zarr
from zarr.convenience import copy_store
import fsspec
from selenium import webdriver
import shutil
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor
from cloudvolume import CloudVolume

logger = logging.getLogger(__name__)

# FUNCTION 1: FOR DYNAMIC IMAGES
def url_image_scrape_dynamic(url, save_directory):
    """
    This function downloads image datasets from the Omero dynamic website 
    using the idr-py package. 
    """
    start_time = time.time()
    try:
        # open IDR connection
        conn = connection(host='idr.openmicroscopy.org') 
        conn.SERVICE_OPTS.setOmeroGroup(-1)

        # create dataset object and dataset id
        dataset_id = url.split("=")[-1]
        dataset = conn.getObject("Dataset", int(dataset_id))

        # create dataset folder to save output in
        dataset_folder = os.path.join(save_directory, "omero_" + dataset_id)
        os.makedirs(dataset_folder, exist_ok=True)

    # Iterate through images in the dataset
        for img in tqdm(dataset.listChildren(), 
                        desc=f"Downloading dataset omero_{dataset_id}", unit="file"):
            name = img.getName()
            z_size = img.getSizeZ()
            t_size = img.getSizeT()
            c_size = img.getSizeC()
            x_size = img.getSizeX()
            y_size = img.getSizeY()

            logger.info(f"Image: {name} ({z_size}Z x {c_size}C x {t_size}T)")

            for t in range(t_size):
                for c in range(c_size):
                    stack = np.zeros((z_size, y_size, x_size), dtype=np.uint8)
                    for z in range(z_size):
                        plane = img.getPrimaryPixels().getPlane(z,c,t)
                        stack[z,:,:] = plane

                    out_path = f"{name.replace('/', '_')}_t{t:03}_c{c:02}.tiff"
                    filepath = os.path.join(dataset_folder, out_path)
                    imwrite(filepath, stack)
                    logger.info(f"Saved: {filepath}")

        # disconnect from IDR
        conn.close() 

        end_time = time.time()
        elapsed_time = (end_time - start_time)/60

        logger.info(f"Download of dataset omero_{dataset_id} complete and saved to {filepath} in {elapsed_time:.2f} min")
    except Exception as e:
        logger.error(f"Failed dynamic scrape from {url}: {e}", exc_info=True)

    
# FUNCTION 2: FOR STATIC IMAGES 
def url_image_scrape_static(url, save_directory): 
    """
    This function downloads a .tif image dataset from the static Empiar webpage 
    using beautiful soup and requests. 
    """

    start_time = time.time()
    try:
        # create dataset folder to save output in
        dataset_id = url.split("/")[-3]
        dataset_folder = os.path.join(save_directory, "empiar_" + dataset_id )
        os.makedirs(dataset_folder, exist_ok=True)

        resp = requests.get(url)
        soup = BeautifulSoup(resp.text, 'html.parser')

        hrefs = []
        for link in soup.find_all("a"):
            href = link.get("href")
            if href and href.lower().endswith(".dm3"):
            
                hrefs.append(href)

        for href in tqdm(hrefs, desc= f"Downloading dataset empiar_{dataset_id}", unit="File"):
            
            file_url = os.path.join(url, href)
            filepath = os.path.join(dataset_folder, href)
            try:
                with requests.get(file_url, stream=True) as r:
                    r.raise_for_status()
                    with open(filepath, 'wb') as f:
                        for chunk in r.iter_content(chunk_size=8192):
                            f.write(chunk)
                logger.info(f"Downloaded: {filepath}")
            except Exception as e:
                logger.error(f"Failed to download {file_url}: {e}", exc_info=True)

        end_time = time.time()
        elapsed_time = (end_time - start_time)/60 
        logger.info(f"Download of Empiar dataset {dataset_id} complete and saved to {dataset_folder} in {elapsed_time:.2f} min")

    except Exception as e:
        logger.error(f"Failed static scrape from {url}: {e}", exc_info=True)
    
    
# FUNCTION 3: FOR STATIC IMAGES 

def url_image_scrape_selenium(url, save_directory, num_workers=4): 
    """
    This function retrieves the .tif image dataset on the webpage using selenium because the 
    webpage has dynamic links. Incorporates multithreading to speed up downloads.
    """

    start_time = time.time()
    try:
        # count number of cpus
        cpu_count = os.cpu_count()
        processes = cpu_count - 1

        # create dataset folder to save output in
        dataset_id = "mitochondria-" + url.split("/")[-2]
        dataset_folder = os.path.join(save_directory, dataset_id)
        os.makedirs(dataset_folder, exist_ok=True)

        # launch headless browser
        driver = webdriver.Chrome()
        driver.get(url)
        html = driver.page_source
        soup = BeautifulSoup(html, "html.parser") # make it a searchable object
        driver.quit()

        # get all tif links without any duplicate urls or empty spaces
        tif_links_all = []
        
        for link in soup.find_all("a"):
                href = link.get("href")
                if href and href.lower().endswith(".tif"):
                    tif_links_all.append(href)

        # cleaning up the links list (getting rid of duplicates and empty spaces)
        tif_links_final = list(set(link.strip().replace(' ', '%20') for link in tif_links_all))

        # create task list for multithreading
        tasks = [(link, os.path.join(dataset_folder, os.path.basename(link))) for link in tif_links_final]

        
        def tif_downloader(link_and_path):
            link, filepath = link_and_path
            try:
                with requests.get(link, stream=True) as r:
                    with open(filepath, 'wb') as f:
                        shutil.copyfileobj(r.raw, f)
                return f"{os.path.basename(filepath)}"
            except Exception as e:
                return f"{os.path.basename(filepath)}: {e}"

        # save to local directory using multithreading
        with ThreadPoolExecutor(max_workers=processes) as executor:
            results = list(tqdm(
                executor.map(tif_downloader, tasks),
                total= len(tasks),
                desc= f"Downloading {dataset_id}"
            ))

        for result in results:
            logger.info(result)
    
        end_time = time.time()
        elapsed_time = (end_time - start_time)/60 
        logger.info(f"Download of dataset {dataset_id} complete and saved to {dataset_folder} in {elapsed_time:.2f} min")
    
    except Exception as e:
        logger.error(f"Failed selenium scrape from {url}: {e}", exc_info=True)
    
   
# FUNCTION 4: FOR ZARR FILES

def url_image_scrape_zarr(url, save_directory):

    """
    This function downloads an image dataset from Janelia in zarr format saved in an s3 bucket
    using fsspec and zarr.
    """

    start_time = time.time()
    try:
        # create dataset folder to save output in
        dataset_id = url.split("/")[-1]
        dataset_folder = os.path.join(save_directory, dataset_id)
        os.makedirs(dataset_folder, exist_ok=True)
        
        fs = fsspec.filesystem("s3", anon=True)
        base = url.replace("s3://", "").rstrip("/")
        all_files = fs.find(url)

        # Get all parent folders of .zarray files
        zarr_keys = []
        for path in all_files:
            if path.endswith(".zarray"):
                rel_path = os.path.dirname(path[len(base) + 1:])
                zarr_keys.append(rel_path)

        if not zarr_keys:
            raise ValueError("No .zarray datasets found")

        logger.info(f"Found {len(zarr_keys)} arrays under {url}:")

        # for key in zarr_keys:
        #     print(f" - {key}")

        # Download each array
        for key in tqdm(zarr_keys, desc=f"Downloading {dataset_id}", unit="array"):
            try:
                online_path = os.path.join(url, key)
                local_path = os.path.join(dataset_folder, key)

                # remove filepath if it exists already
                if os.path.exists(local_path):
                    shutil.rmtree(local_path)

                online_mapper = fsspec.get_mapper(online_path, anon=True)
                local_mapper = zarr.DirectoryStore(local_path)

                logger.info(f"Copying: {key}")
                zarr.copy_store(online_mapper, local_mapper)
            except Exception as e:
                logger.error(f"Failed to copy Zarr key {key}: {e}", exc_info=True)

        end_time = time.time()
        elapsed = (end_time - start_time) / 60
        logger.info(f"Downloaded all arrays from {dataset_id} in {elapsed:.2f} minutes")
    
    except Exception as e:
        logger.error(f"Failed Zarr scrape from {url}: {e}", exc_info=True)



# FUNCTION 5: FOR PIXEL CROP REGION OF NEUROGLANCER DATASET

def url_image_scrape_neuroglancer(url, save_directory, crop_region=(1000,1000,1000)):
    """
    This function downloads a random crop region of size 1000x1000x1000 from a 
    Neuroglancer dataset with the CloudVolume package. 
    """
    start_time = time.time()

    try:
        # waive authentication credentials
        os.environ["CLOUD_VOLUME_AUTH"] = "false"

        # create dataset folder to put output in
        dataset_id = url.split("/")[-4]
        dataset_folder = os.path.join(save_directory, dataset_id)
        os.makedirs(dataset_folder, exist_ok=True)

        # get the highest resolution of the image
        volume = CloudVolume(url, mip=0, use_https=True, fill_missing=True)
        volume_shape = volume.shape[:3]

        # image and crop region dimensions
        z_dim, y_dim, x_dim = volume_shape
        z_crop, y_crop, x_crop = crop_region

        if z_dim >= z_crop and y_dim >= y_crop and x_dim >= x_crop:
                random_z = np.random.randint(0, z_dim - z_crop + 1)
                random_y = np.random.randint(0, y_dim - y_crop + 1)
                random_x = np.random.randint(0, x_dim - x_crop + 1)
        
        # random pixel crop regin
        chunk = volume[random_z:random_z + z_crop, random_y:random_y + y_crop, 
                    random_x:random_x + x_crop]
        
        # save to local directory
        filename = f"{dataset_id}_crop.tif"
        output_path = os.path.join(dataset_folder, filename)
        imwrite(output_path, chunk)

        end_time = time.time()
        elapsed_time = (end_time - start_time)/60

        logger.info(f"Downloaded {filename} in {elapsed_time} minutes")
    except Exception as e:
        logger.error(f"Failed neuroglancer scrape from {url}: {e}", exc_info=True)

__all__ = [
    "url_image_scrape_dynamic",
    "url_image_scrape_static",
    "url_image_scrape_selenium",
    "url_image_scrape_zarr",
    "url_image_scrape_neuroglancer",
]

