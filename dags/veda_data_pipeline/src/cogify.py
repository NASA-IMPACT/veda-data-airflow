from affine import Affine
import os
import requests

import boto3
from netCDF4 import Dataset
import numpy as np
from rasterio.crs import CRS
from rasterio.io import MemoryFile
from rasterio.warp import calculate_default_transform
from rio_cogeo.cogeo import cog_translate
from rio_cogeo.profiles import cog_profiles


config = {
    "DEFAULT": {"output_bucket": "climatedashboard-data", "output_dir": "OMDOAO3e_003"},
    "GPM_3IMERGM": {"group": "Grid", "variable_name": "precipitation"},
    "ERA5": {"variable_name": "cbh"},
    "NISAR": {
        "variable_name": "science/LSAR/GCOV/grids/frequencyA/HHHH",
        "src_crs": "+proj=utm +zone=32S +datum=WGS84",
        "x_variable": "science/LSAR/GCOV/metadata/radarGrid/xCoordinates",
        "y_variable": "science/LSAR/GCOV/metadata/radarGrid/yCoordinates",
    },  # WIP
    "OMNO2d": {
        "variable_name": "HDFEOS/GRIDS/ColumnAmountNO2/Data Fields/ColumnAmountNO2TropCloudScreened",
        "affine_transformation": "(xmin, xres, 0, ymax, 0, -yres)",
    },
    "OMDOAO3e": {
        "variable_name": "HDFEOS/GRIDS/ColumnAmountO3/Data Fields/ColumnAmountO3",
        "affine_transformation": "(xmin, xres, 0, ymax, 0, -yres)%",
    },
}

s3 = boto3.client(
    "s3",
)

# Set COG inputs
output_profile = cog_profiles.get(
    "deflate"
)  # if the files aren't uint8, this will need to be changed
output_profile["blockxsize"] = 256
output_profile["blockysize"] = 256
output_bucket = config["DEFAULT"]["output_bucket"]
output_dir = config["DEFAULT"]["output_dir"]


def upload_file(outfilename, collection):
    filename = outfilename.split("/tmp/")[1]
    try:
        s3.upload_file(
            outfilename,
            output_bucket,
            f"{collection}/{filename}",
        )
        print("File uploaded to s3")
        return f"s3://{output_bucket}/{collection}/{filename}"
    except:
        print("Failed to copy to S3 bucket")
        raise


def download_file(file_uri: str):
    filename = os.path.splitext(os.path.basename(file_uri))[0]
    filename = f"/tmp/{filename}"
    if "http" in file_uri:
        # This isn't working for GPMIMERG, need to use .netrc
        username = os.environ.get("EARTHDATA_USERNAME")
        password = os.environ.get("EARTHDATA_PASSWORD")
        with requests.Session() as session:
            session.auth = (username, password)
            request = session.request("get", file_uri)
            response = session.get(request.url, auth=(username, password))
            print("RESPONSE IS")
            print(response.status_code)
            with open(filename, "wb") as f:
                f.write(response.content)
    elif "s3://" in file_uri:
        path_parts = file_uri.split("://")[1].split("/")
        bucket = path_parts[0]
        path = "/".join(path_parts[1:])
        s3.download_file(bucket, path, filename)
    else:
        print(f"{filename} file already downloaded")
    return filename


def to_cog(upload, **config):
    """HDF5 to COG."""
    # Open existing dataset
    filename = str(config["filename"])
    variable_name = config["variable_name"]
    x_variable, y_variable = config.get("x_variable"), config.get("y_variable")
    group = config.get("group")
    src = Dataset(filename, "r")

    if group is None:
        variable = src[variable_name][:]
        nodata_value = variable.fill_value
    else:
        variable = src.groups[group][variable_name]
        nodata_value = variable._FillValue
    # This may be just what we need for IMERG
    if config["collection"] == "GPM_3IMERGM":
        variable = np.transpose(variable[0])
    if config["collection"] == "OMDOAO3e":
        variable = np.flipud(variable)

    # This implies a global spatial extent, which is not always the case
    src_height, src_width = variable.shape[0], variable.shape[1]
    if x_variable and y_variable:
        xmin = src[x_variable][:].min()
        xmax = src[x_variable][:].max()
        ymin = src[y_variable][:].min()
        ymax = src[y_variable][:].max()
    else:
        xmin, ymin, xmax, ymax = [-180, -90, 180, 90]

    src_crs = config.get("src_crs")
    if src_crs:
        src_crs = CRS.from_proj4(src_crs)
    else:
        src_crs = CRS.from_epsg(4326)

    dst_crs = CRS.from_epsg(3857)

    # calculate dst transform
    dst_transform, dst_width, dst_height = calculate_default_transform(
        src_crs,
        dst_crs,
        src_width,
        src_height,
        left=xmin,
        bottom=ymin,
        right=xmax,
        top=ymax,
    )

    # https://github.com/NASA-IMPACT/cloud-optimized-data-pipelines/blob/rwegener2-envi-to-cog/docker/omno2-to-cog/OMNO2d.003/handler.py
    affine_transformation = config.get("affine_transformation")
    if affine_transformation:
        xres = (xmax - xmin) / float(src_width)
        yres = (ymax - ymin) / float(src_height)
        geotransform = eval(affine_transformation)
        dst_transform = Affine.from_gdal(*geotransform)

    # Save output as COG
    output_profile = dict(
        driver="GTiff",
        dtype=variable.dtype,
        count=1,
        transform=dst_transform,
        crs=src_crs,
        height=src_height,
        width=src_width,
        nodata=nodata_value,
        tiled=True,
        compress="deflate",
        blockxsize=256,
        blockysize=256,
    )
    print("profile h/w: ", output_profile["height"], output_profile["width"])
    outfilename = f"{filename}.tif"
    with MemoryFile() as memfile:
        with memfile.open(**output_profile) as mem:
            data = variable.astype(np.float32)
            mem.write(data, indexes=1)
        cog_translate(
            memfile,
            outfilename,
            output_profile,
            config=dict(GDAL_NUM_THREADS="ALL_CPUS", GDAL_TIFF_OVR_BLOCKSIZE="128"),
        )
    return_obj = {
        "filename": outfilename,
    }
    if upload:
        s3location = upload_file(outfilename, config["collection"])
        return_obj["s3_filename"] = s3location

    return return_obj


def cogify_handler(event, context):
    filename = event["href"]
    collection = event["collection"]
    to_cog_config = config._sections[collection]
    downloaded_filename = download_file(file_uri=filename)
    to_cog_config["filename"] = downloaded_filename
    to_cog_config["collection"] = collection

    return_obj = {"granule_id": event["granule_id"], "collection": event["collection"]}

    output_locations = to_cog(upload=event.get("upload", False), **to_cog_config)

    return_obj = {**return_obj, **output_locations}

    print(f"Returning data: {return_obj}")
    return return_obj


if __name__ == "__main__":
    sample_event = {
        "collection": "OMDOAO3e",
        "href": "https://acdisc.gesdisc.eosdis.nasa.gov/data//Aura_OMI_Level3/OMDOAO3e.003/2022/OMI-Aura_L3-OMDOAO3e_2022m0120_v003-2022m0122t021759.he5",
        "upload": False,
        "granule_id": "G2205784904-GES_DISC",
    }
    cogify_handler(sample_event, {})
