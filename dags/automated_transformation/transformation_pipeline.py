import tempfile
import boto3
import pandas as pd
import rasterio
import s3fs
import importlib

files_processed = pd.DataFrame(columns=["file_name", "COGs_created"])

def transform_cog(name_list, nodata, raw_data_bucket, dest_data_bucket, cog_data_prefix, collection_name):
    """This function calls the plugins (dataset specific transformation functions) and
    generalizes the transformation of dataset to COGs.

    Args:
        name_list (str): List of the files to be transformed
        nodata (str): _description_
        raw_data_bucket (str): _description_
        dest_data_bucket (str): _description_
        cog_data_prefix (str): _description_
        collection_name (str): _description_

    Returns:
        dict: _description_
    """
            
    session = boto3.session.Session()
    s3_client = session.client("s3")
    file_status = {}
    module = importlib.import_module('automated_transformation.transformation_functions')
    function_name = f'{collection_name.replace("-", "_")}_transformation'
    for name in name_list:
        url = f"s3://{raw_data_bucket}/{name}"
        fs = s3fs.S3FileSystem()
        print('the url is', url)
        with fs.open(url, mode='rb') as file_obj:
            transform_func = getattr(module, function_name)
            var_data_netcdf = transform_func(file_obj, name, nodata)
                
            for cog_filename, data in var_data_netcdf.items():
            # generate COG
                COG_PROFILE = {"driver": "COG", "compress": "DEFLATE"}
                try:
                    with tempfile.NamedTemporaryFile() as temp_file:
                        data.rio.to_raster(temp_file.name, **COG_PROFILE)
                        s3_client.upload_file(
                            Filename=temp_file.name,
                            Bucket=dest_data_bucket,
                            Key=f"{cog_data_prefix}/{collection_name}/{cog_filename}",
                        )
                    file_status[cog_filename] = f'file transformed and uploaded to "{cog_data_prefix}/{collection_name}/{cog_filename}" successfully'
                except:
                    file_status[cog_filename] = 'failed to upload/transform'
        return file_status
