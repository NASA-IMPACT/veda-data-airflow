import importlib
import tempfile

import boto3
import pandas as pd
import s3fs
import rasterio
import numpy as np

files_processed = pd.DataFrame(columns=["file_name", "COGs_created"])


def transform_cog(
    name_list, nodata, raw_data_bucket, dest_data_bucket, data_prefix, collection_name
):
    """This function calls the plugins (dataset specific transformation functions) and
    generalizes the transformation of dataset to COGs.

    Args:
        name_list (str): List of the files to be transformed
        nodata (str): Nodata value as mentioned by the data provider
        raw_data_bucket (str): Name of the bucket where the raw data resides
        dest_data_bucket (str): Name of the bucket where we want to store the tranformed cogs
        raw_data_prefix (str): Folder where the netCDF files are stored in the bucket
        collection_name (str): Name of the collection that would be used for the dataset

    Returns:
        dict: Status and name of the file that is transformed
    """

    session = boto3.session.Session()
    s3_client = session.client("s3")
    module = importlib.import_module(
        "automated_transformation.transformation_functions"
    )
    function_name = f'{collection_name.replace("-", "_")}_transformation'
    for name in name_list:
        url = f"s3://{raw_data_bucket}/{name}"
        fs = s3fs.S3FileSystem()
        print("the url is", url)
        with fs.open(url, mode="rb") as file_obj:
            try:
                transform_func = getattr(module, function_name)
                var_data_netcdf = transform_func(file_obj, name, nodata)

                for cog_filename, data in var_data_netcdf.items():
                    # generate COG
                    min_value_netcdf = data.min().item()
                    max_value_netcdf = data.max().item()
                    std_value_netcdf = data.std().item()
                    mean_value_netcdf = data.mean().item()
                    COG_PROFILE = {"driver": "COG", "compress": "DEFLATE"}
                    with tempfile.NamedTemporaryFile() as temp_file:
                        data.rio.to_raster(temp_file.name, **COG_PROFILE)
                        s3_client.upload_file(
                            Filename=temp_file.name,
                            Bucket=dest_data_bucket,
                            Key=f"{data_prefix}/{collection_name}/{cog_filename}",
                        )
                        raster_data = rasterio.open(temp_file.name).read()
                        raster_data[raster_data == -9999] = np.nan
                        min_value_cog = np.nanmin(raster_data)
                        max_value_cog = np.nanmax(raster_data)
                        mean_value_cog = np.nanmean(raster_data)
                        std_value_cog = np.nanstd(raster_data)
                        status = {
                            "transformed_filename": cog_filename,
                            "s3uri": f"s3://{dest_data_bucket}/{data_prefix}/{collection_name}/{cog_filename}",
                            "status": "success",
                        }
            except Exception as ex:
                status = {
                    "transformed_filename": name,
                    "status": "failed",
                    "reason": f"Error: {ex}",
                }
        return status
