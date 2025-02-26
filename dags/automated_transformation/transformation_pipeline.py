import importlib
import json
import os
import tempfile

import boto3
import numpy as np
import rasterio
import requests
import s3fs
import shutil


def get_all_s3_keys(bucket, s3_prefix, ext) -> list:
    """Function fetches all the s3 keys from the given bucket and model name.

    Args:
        bucket (str): Name of the bucket from where we want to fetch the data
        s3_prefix (str): Dataset name/folder name where the data is stored
        ext (str): extension of the file that is to be fetched.

    Returns:
        list : List of all the keys that match the given criteria
    """
    session = boto3.session.Session()
    s3_client = session.client("s3")
    keys = []
    kwargs = {"Bucket": bucket, "Prefix": s3_prefix}
    there_more_files = True
    while there_more_files:
        resp = s3_client.list_objects_v2(**kwargs)
        for obj in resp["Contents"]:
            if obj["Key"].endswith(ext) and "historical" not in obj["Key"]:
                keys.append(obj["Key"])
        kwargs["ContinuationToken"] = resp.get("NextContinuationToken")
        there_more_files = resp.get("NextContinuationToken") is not None
    print(f"Discovered {len(keys)}")
    return keys


def download_python_file_from_s3(bucket_name, s3_key, temp_file_path):
    """
    Downloads a Python file from an S3 bucket and returns a temporary file path.

    Parameters:
    - bucket_name (str): The name of the S3 bucket.
    - s3_key (str): The key (path) to the file in the S3 bucket.

    Returns:
    - str: Path to the temporary file.
    """

    s3 = boto3.client("s3")

    # Download the S3 file to the temporary file location
    s3.download_file(bucket_name, s3_key, temp_file_path)
    print(
        f"Downloaded {s3_key} from bucket {bucket_name} to temporary file {temp_file_path}"
    )

    return temp_file_path


def download_python_file(uri: str):
    # Extract the file name from the URL
    file_name = os.path.basename(uri)

    # Create a temporary directory and file with the same name
    temp_dir = tempfile.mkdtemp()
    temp_file_path = os.path.join(temp_dir, file_name)
    # Write the content to the temporary file

    if uri.startswith("s3://"):
        # Remove the 's3://' prefix
        s3_path = uri[5:]
        # Split into bucket and key
        parts = s3_path.split("/", 1)
        bucket_name, key = parts
        return download_python_file_from_s3(bucket_name=bucket_name, s3_key=key, temp_file_path=temp_file_path)
    return download_python_file_from_github(url=uri, temp_file_path=temp_file_path)


def check_file_exists(url):
    """
    Function to check if link return a success status
    Args:
        url: url to the file

    Returns:
        request response if exist and raise exception if not
    """
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise an error for HTTP errors
    except requests.exceptions.RequestException as e:
        raise Exception(f"Error requesting the file: {e}")
    return response.content


def download_python_file_from_github(url, temp_file_path):
    try:
        # Send a GET request to the URL
        content = check_file_exists(url)
        with open(temp_file_path, "wb") as temp_file:
            temp_file.write(content)

        print(f"File downloaded to: {temp_file_path}")
        return temp_file_path

    except requests.exceptions.RequestException as e:
        raise Exception(f"Error downloading the file: {e}")


def load_function_from_file(file_path, function_name):
    """
    Dynamically loads a function from a Python file.

    Parameters:
    - file_path (str): Path to the Python file.
    - function_name (str): Name of the function to load.

    Returns:
    - function: The loaded function object.
    """
    # Load the module from the file path
    spec = importlib.util.spec_from_file_location("dynamic_module", file_path)
    dynamic_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(dynamic_module)

    # Return the function
    return getattr(dynamic_module, function_name)


def transform_cog(
        name_list,
        nodata,
        raw_data_bucket,
        dest_data_bucket,
        data_prefix,
        collection_name,
        plugin_url,
):
    """This function calls the plugins (dataset specific transformation functions) and
    generalizes the transformation of dataset to COGs.

    Args:
        plugin_url:
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
    json_dict = {}
    function_name = f'{collection_name.replace("-", "_")}_transformation'
    temp_file_path = download_python_file(plugin_url)
    transform_func = load_function_from_file(temp_file_path, function_name)
    for name in name_list:
        url = f"s3://{raw_data_bucket}/{name}"
        fs = s3fs.S3FileSystem()
        print("the url is", url)
        with fs.open(url, mode="rb") as file_obj:
            try:
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
                        json_dict.update(
                            {
                                "original_file_url": name,
                                "transformed_filename": cog_filename,
                                "transformed_cog_s3uri": f"s3://{dest_data_bucket}/{data_prefix}/{cog_filename}",
                                "minimum_value_cog": f"{min_value_cog:.4f}",
                                "maximum_value_cog": f"{max_value_cog:.4f}",
                                "std_value_cog": f"{std_value_cog:.4f}",
                                "mean_value_cog": f"{mean_value_cog:.4f}",
                                "minimum_value_netcdf": f"{min_value_netcdf:.4f}",
                                "maximum_value_netcdf": f"{max_value_netcdf:.4f}",
                                "std_value_netcdf": f"{std_value_netcdf:.4f}",
                                "mean_value_netcdf": f"{mean_value_netcdf:.4f}",
                            }
                        )
                    with tempfile.NamedTemporaryFile() as json_temp:
                        with open(json_temp.name, "w") as fp:
                            json.dump(json_dict, fp, indent=4)
                        print("JSON dictionary is ", json_dict)

                        # Upload the file to the specified S3 bucket and folder
                        s3_client.upload_file(
                            Filename=json_temp.name,
                            Bucket=dest_data_bucket,
                            Key=f"{data_prefix}/{collection_name}/{cog_filename[:-4]}.json",
                            ExtraArgs={"ContentType": "application/json"},
                        )
                        status = {
                            "transformed_filename": cog_filename,
                            "statistics_file": f"{cog_filename.split('.')[0]}.json",
                            "s3uri": f"s3://{dest_data_bucket}/{data_prefix}/{collection_name}/{cog_filename}",
                            "status": "success",
                        }

            except Exception as ex:
                # We are not raising an Exception because we want
                # to continue processing if one file error out
                status = {
                    "transformed_filename": name,
                    "status": "failed",
                    "reason": f"Error: {ex}",
                }
            finally:
                print(f"Deleting {temp_file_path}")
                shutil.rmtree(os.path.dirname(temp_file_path))
        return status
