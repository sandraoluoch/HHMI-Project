<h2><b>Description</b></h2>

The goal of this project for the Howard Hughes Medical Institute is to make high-resolution microscopy image data machine-readable, block-accessible and ready for downstream ML/AI pipelines. This repository provides a modular Python package (img_dataset_tools) for downloading and processing image datasets stored online in different formats. This metadata is then consolidated and stored in a data entry table saved in a .csv file.

The code in this repository supports dynamic web scraping, parallelized downloading from cloud-hosted repositories, and unified metadata extraction for .tif, .dm3, and .zarr file formats.

<h2><b>Summary of Workflow</b></h2>

In order to generate the metadata table, I had to download the 3D image datasets that were stored online in different ways. Since the datasets were stored in different ways online, my approach was to create five different functions to webscrape and download the image datasets and save them to a local directory.  Looking through the provided URLs, the images were stored on static webpages, dynamic webpages, an s3 bucket link and a google storage link. The datasets were in .tif, .dm3, zarr and a "precomputed" volume neuroglancer format. As per the task instructions, I downloaded the datasets in a parallel manner using Python's Multiprocessing library. This enabled the datasets to be downloaded all at once instead of sequentially.

<b>STEP 1: WEBSCRAPING FUNCTIONS</b>

1) Static Webpage Scraper 
   
Libraries used: requests, BeautifulSoup

For the first function which webscrapes and downloads an image dataset from a static webpage, I decided to use BeautifulSoup and requests libraries because they are lightweight, easy to use, and effective for extracting links directly from HTML. These tools make it straightforward to retrieve .dm3 file URLs by parsing the href tags on the page. This was pretty straightforward and the dataset downloaded quickly. 

2) Dynamic Webpage Scraper

Library used: idr-py

For the second function that webscrapes and downloads an image dataset from OMERO, a dynamic webpage. IDR hosts datasets behind a JavaScript-powered interface and the idr-py library simplifies the connection process and allows direct access to datasets, metadata, and pixel. This eliminated the need to emulate browser behavior or extract dynamic links manually.

3)  Dynamic JavaScript Links
   
Libraries used: Selenium, BeautifulSoup, requests, ThreadPoolExecutor

At first glance the third URL for the electron microscopy dataset, looked like a static webpage but it has dynamic links and uses javascript. I decided to use Selenium library in creating this third function because it retrieves all the .tif hyperlinks available. However, downloading this dataset caused a bottleneck in the parallel downloading process. One of the .tif files was about ~3.3GB and the others were idle while this was downloading. To combat this, I incorporated multithreading within the function to download all .tif files at once. I used an AI assistant to help incorporate the threading portion ofthis function.

4) Zarr Folder Scraper

Libraries used: zarr, fsspec, shutil

For the fourth function that webscrapes and downloads a zarr folder hosted on Amazon S3, I used fsspec to interact with the S3 bucekt and zarr to download nested arrays. These libraries made it easy to navigate nested .zarray and .zgroup structures.

5) Random Crop Region of Neuroglancer Dataset
   
Library used: CloudVolume

For the fifth function that webscrapes and downloads a random crop region of 1000x1000x1000, I used the CloudVolume library. The website, Neuroglancer, stores its images as volumes on Google Storage and CloudVolume is ideal for handling precomputed volumes. I used an AI assistant here to help me figure out how to crop the image on the actual full resolution image. I also used it to help bypass the authorization errors I kept getting when trying to download the imageI saved the cropped image as a .tif file to my local directory.

<b>STEP 2: PARALLELIZED DOWNLOADING OF DATASETS</b>

In order to download all five datasets in a parallelized fashion, I used Python's Multiprocessing library. I created a wrapper function that took all the five functions and used that as my input into the process_map method.

<b>STEP 3: CREATING METADATA TABLE</b>

Once I had all the image datasets downloaded, I created a script that extracted and consolidated the metadata. I decided to extract metadata by file type because different file types store metadata differently. Then I combined them in a Pandas Dataframe and exported it as a .csv file. For each file type (.tif, .dm3, zarr), I first listed the available metadata and then chose the ones that would be helpful for an AI/ML pipeline. This was straightforward for the .tif files, but for the .dm3 and zarr datasets, I created two functions (flatten_dm3_dict and extract_zarr_metadata) to help access the metadata. I used an AI assistant here to help with constructing these functions. 

<h2><b>Project Structure</b></h2>

<pre>HHMI-Project/
├── img_dataset_tools/                # Core module for dataset tools
│   ├── __init__.py                   # Package initializer
│   ├── webscrapers.py                # Functions to download datasets from various sources
│   ├── metadata_utils.py             # Utilities for extracting and flattening metadata
│   └── dm3_lib/                      # Local copy of dm3_lib for DM3 file parsing
│       ├── __init__.py
│       └── _dm3_lib.py
│
├── scripts/                          # Scripts for processing datasets
│   ├── multiprocessing_image_datasets.py  # Parallelized dataset downloader
│   └── extract_metadata.py                # Metadata extraction script
│
├── saved_datasets/                   # Directory for downloaded datasets (created at runtime)
│
├── metadata_table.csv                # Output CSV containing aggregated metadata (generated at runtime)
│
├── README.md                         # Project documentation
└── .gitignore                        # Specifies files and directories to be ignored by Git </pre>

<h2><b>Installation and Running the Code</b></h2>
This code works best using conda and Python=3.10+

In your terminal, clone the repo and navigate to the HHMI-Project folder:
<pre>
   git clone https://github.com/sandraoluoch/HHMI-Project.git
   cd HHMI-Project
</pre>

Create a new conda environment and activate it:
<pre>
   conda create -n hhmi-env python=3.10 -y
   conda activate hhmi-env
</pre>

Install the following packages:
<pre>
   pip install pandas numpy tqdm tifffile zarr fsspec s3fs requests beautifulsoup4 selenium ncempy cloud-volume
</pre>

Install the zeroc-ice package separately using conda-forge:
<pre>
   conda install conda-forge::zeroc-ice
</pre>

Install the idr-py package separately using conda-forge:
<pre>
   conda install bioconda::idr-py   
</pre>

Install the img_dataset_tools package:
<pre>
   pip install -e .
</pre>

For downloading the image datasets:
<pre>
   python scripts/multiprocessing_image_datasets.py 
</pre>

For extracting the metadata and creating a table:
<pre>
   python scripts/extract_metadata.py 
</pre>




