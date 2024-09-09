import json
import tempfile
from datetime import datetime

import boto3
import numpy as np
import pandas as pd
import rasterio
import s3fs
import xarray as xr
import re


# datefmt = "month"
# nodata = -9999
# # lat_name, lon_name = 'lat', 'lon'
# lat_name, lon_name, time_name = "latitude", "longitude", "months"
# variable_list = ["fossil", "microbial", "pyrogenic", "total"]

session = boto3.session.Session()
s3_client = session.client("s3")


files_processed = pd.DataFrame(columns=["file_name", "COGs_created"])

def transform_tif(name_list, datefmt, nodata, raw_data_bucket, dest_data_bucket, cog_data_prefix, collection_name,lon_name, lat_name, time_name):

    for name in name_list:
            url = f"s3://{raw_data_bucket}/{name}"
            fs = s3fs.S3FileSystem()
            print('the url is', url)
            with fs.open(url, mode='rb') as file_obj:
                xds = xr.dataarray(file_obj)
                xds = xds.rename({lon_name: "longitude", lat_name: "latitude", time_name: "time"})
                if datefmt == "year":
                    time_val = [str(x.astype("M8[Y]")) for x in xds.time.values]
                elif datefmt == "month":
                    time_val = [str(x.astype("M8[M]")) for x in xds.time.values]
                elif datefmt == "day":
                    time_val = [str(x.astype("M8[D]")) for x in xds.time.values]
                else:
                    raise ValueError("Please provide a valid date time format")
                
                for time_increment in time_val:
                    filename = name.split("/")[-1]
                    filename_elements = re.split("[_ .]", filename)
                    data = xds
                    if data.dtype in ["float32", "float64"]:

                        data = data.where(data == nodata, -9999)

                        data = data.reindex(latitude=list(reversed(data.latitude)))
                        data.rio.set_spatial_dims("longitude", "latitude", inplace=True)
                        data.rio.write_crs("epsg:4326", inplace=True)
                        data.rio.write_nodata(nodata, inplace=True, encoded=True)

                        # generate COG
                        COG_PROFILE = {"driver": "COG", "compress": "DEFLATE"}

                        filename_elements.pop()
                        filename_elements.append(f"{datefmt}")
                        filename_elements.append(f'{time_increment.replace("-", "")}')
                        # # insert date of generated COG into filename
                        cog_filename = "_".join(filename_elements)
                        # # add extension
                        cog_filename = f"{cog_filename}.tif"

                        with tempfile.NamedTemporaryFile() as temp_file:
                            data.rio.to_raster(temp_file.name, **COG_PROFILE)
                            s3_client.upload_file(
                                Filename=temp_file.name,
                                Bucket=dest_data_bucket,
                                Key=f"{cog_data_prefix}/{collection_name}/{cog_filename}",
                            )
                    print(cog_filename)

    return

def get_all_s3_keys(bucket, model_name, ext):
    """Get a list of all keys in an S3 bucket."""
    keys = []

    kwargs = {"Bucket": bucket, "Prefix": f"{model_name}"}
    while True:
        resp = s3_client.list_objects_v2(**kwargs)
        print("response is ", resp)
        for obj in resp["Contents"]:
            if obj["Key"].endswith(ext) and "historical" not in obj["Key"]:
                keys.append(obj["Key"])

        try:
            kwargs["ContinuationToken"] = resp["NextContinuationToken"]
        except KeyError:
            break

    print(f"Discovered {len(keys)}")
    return keys

# name = '/Users/vgaur/ghgc-docs/data/casa-gfed/GEOSCarb_CASAGFED3v3_Flux.Monthly.x720_y360.2004.nc'
# name = '/Users/vgaur/ghgc-docs/data/oco2geos-co2-daygrid-v10r/oco2_GEOS_L3CO2_day_20150101_B10206Ar.nc4'
# name = '/Users/vgaur/ghgc-docs/data/tm54dvar-ch4flux-mask-monthgrid-v5/methane_emis_1999.nc'
# name = '/Users/vgaur/ghgc-docs/data/gpw/gpw_v4_population_density_rev11_2000_30_sec.tif'
# name = '/Users/vgaur/ghgc-docs/data/odiac_data/2000/odiac2022_1km_excl_intl_0001.tif'
def transform_cog(name_list, datefmt, variables_list, nodata, raw_data_bucket, dest_data_bucket, cog_data_prefix, collection_name, lon_name, lat_name, time_name, ext):
    if ext in ['.tif', '.tiff']:
        transform_tif()
    else:
        for name in name_list:
            url = f"s3://{raw_data_bucket}/{name}"
            fs = s3fs.S3FileSystem()
            print('the url is', url)
            with fs.open(url, mode='rb') as file_obj:
                try:
                    xds = xr.open_dataset(file_obj)
                except ValueError:
                    xds = xr.open_dataset(file_obj, decode_times=False)

                xds = xds.rename({lon_name: "longitude", lat_name: "latitude", time_name: "time"})
                xds = xds.assign_coords(longitude=(((xds.longitude + 180) % 360) - 180)).sortby(
                    "longitude"
                )
                variables = list(set([var for var in xds.data_vars]) & set(variables_list))
                if datefmt == "year":
                    time_val = [str(x.astype("M8[Y]")) for x in xds.time.values]
                elif datefmt == "month":
                    time_val = [str(x.astype("M8[M]")) for x in xds.time.values]
                elif datefmt == "day":
                    time_val = [str(x.astype("M8[D]")) for x in xds.time.values]
                else:
                    raise ValueError("Please provide a valid date time format")

                for time_increment in time_val:
                    for var in variables:
                        filename = name.split("/")[-1]
                        filename_elements = re.split("[_ .]", filename)
                        data = xds[var]
                        if data.dtype in ["float32", "float64"]:

                            data = data.where(data == nodata, -9999)

                            data = data.reindex(latitude=list(reversed(data.latitude)))
                            data.rio.set_spatial_dims("longitude", "latitude", inplace=True)
                            data.rio.write_crs("epsg:4326", inplace=True)
                            data.rio.write_nodata(nodata, inplace=True, encoded=True)

                            # generate COG
                            COG_PROFILE = {"driver": "COG", "compress": "DEFLATE"}

                            filename_elements.pop()
                            filename_elements.append(f"{var}")
                            filename_elements.append(f"{datefmt}")
                            filename_elements.append(f'{time_increment.replace("-", "")}')
                            # # insert date of generated COG into filename
                            cog_filename = "_".join(filename_elements)
                            # # add extension
                            cog_filename = f"{cog_filename}.tif"

                            with tempfile.NamedTemporaryFile() as temp_file:
                                data.rio.to_raster(temp_file.name, **COG_PROFILE)
                                s3_client.upload_file(
                                    Filename=temp_file.name,
                                    Bucket=dest_data_bucket,
                                    Key=f"{cog_data_prefix}/{collection_name}/{cog_filename}",
                                )
                        print(cog_filename)
