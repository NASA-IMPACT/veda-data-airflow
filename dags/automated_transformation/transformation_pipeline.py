import importlib
import tempfile
import os

import boto3
import pandas as pd
import s3fs

files_processed = pd.DataFrame(columns=["file_name", "COGs_created"])

def get_all_s3_keys(bucket, model_name, ext):
    """Function fetches all the s3 keys from the given bucket and model name.

    Args:
        bucket (str): Name of the bucket from where we want to fetch the data
        model_name (str): Dataset name/folder name where the data is stored
        ext (str): extension of the file that is to be fetched.

    Returns:
        list : List of all the keys that match the given criteria
    """
    session = boto3.session.Session()
    s3_client = session.client("s3")
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

def download_python_file_from_s3(bucket_name, s3_key):
    """
    Downloads a Python file from an S3 bucket and returns a temporary file path.

    Parameters:
    - bucket_name (str): The name of the S3 bucket.
    - s3_key (str): The key (path) to the file in the S3 bucket.

    Returns:
    - str: Path to the temporary file.
    """
    s3 = boto3.client('s3')

    # Create a temporary file
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".py")
    temp_file.close()  # Close the file so it can be written to by boto3

    # Download the S3 file to the temporary file location
    s3.download_file(bucket_name, s3_key, temp_file.name)
    print(f"Downloaded {s3_key} from bucket {bucket_name} to temporary file {temp_file.name}")

    return temp_file.name

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
    temp_file_path = download_python_file_from_s3(raw_data_bucket, f'data_transformation_plugins/{function_name}')
    for name in name_list:
        url = f"s3://{raw_data_bucket}/{name}"
        fs = s3fs.S3FileSystem()
        print("the url is", url)
        with fs.open(url, mode="rb") as file_obj:
            try:
                transform_func = load_function_from_file(temp_file_path, function_name)
                var_data_netcdf = transform_func(file_obj, name, nodata)

                for cog_filename, data in var_data_netcdf.items():
                    # generate COG
                    COG_PROFILE = {"driver": "COG", "compress": "DEFLATE"}
                    with tempfile.NamedTemporaryFile() as temp_file:
                        data.rio.to_raster(temp_file.name, **COG_PROFILE)
                        s3_client.upload_file(
                            Filename=temp_file.name,
                            Bucket=dest_data_bucket,
                            Key=f"{data_prefix}/{collection_name}/{cog_filename}",
                        )
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
            finally:
                os.remove(temp_file_path)
        return status
