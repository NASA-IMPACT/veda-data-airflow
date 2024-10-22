import re
from datetime import datetime

import boto3
import xarray


def get_all_s3_keys(bucket, model_name, ext) -> list:
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
    try:
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
    except Exception as ex:
        raise Exception(f"Error returned is {ex}")

    print(f"Discovered {len(keys)}")
    return keys


"""
The naming convention for the transformation function is as follows:
collectionname_transformation
where
1. collection name refers to the STAC collection name that we want to provide
   for the given dataset. Make sure that the collection name in the function
   is same to the one passed as argument when running the DAG.
2. Collection name will be followed by the word transformation which will
   differentiate the transformation functions from any other functions in
   file.
"""


def tm54dvar_ch4flux_mask_monthgrid_v5_transformation(file_obj, name, nodata):
    """Tranformation function for the tm5 ch4 influx dataset

    Args:
        file_obj (s3fs object): s3fs sile object for one file of the dataset
        name (str): name of the file to be transformed
        nodata (int): Nodata value as specified by the data provider

    Returns:
        dict: Dictionary with the COG name and its corresponding data array.
    """

    var_data_netcdf = {}
    xds = xarray.open_dataset(file_obj)
    xds = xds.rename({"latitude": "lat", "longitude": "lon"})
    xds = xds.assign_coords(lon=(((xds.lon + 180) % 360) - 180)).sortby("lon")
    variable = [var for var in xds.data_vars if "global" not in var]

    for time_increment in range(0, len(xds.months)):
        filename = name.split("/")[-1]
        filename_elements = re.split("[_ .]", filename)
        start_time = datetime(int(filename_elements[-2]), time_increment + 1, 1)
        for var in variable:
            data = getattr(xds.isel(months=time_increment), var)
            data = data.isel(lat=slice(None, None, -1))
            data = data.where(data == nodata, -9999)
            data.rio.set_spatial_dims("lon", "lat", inplace=True)
            data.rio.write_crs("epsg:4326", inplace=True)
            data.rio.write_nodata(-9999, inplace=True)

            # # insert date of generated COG into filename
            filename_elements.pop()
            filename_elements[-1] = start_time.strftime("%Y%m")
            filename_elements.insert(2, var)
            cog_filename = "_".join(filename_elements)
            # # add extension
            cog_filename = f"{cog_filename}.tif"
            var_data_netcdf[cog_filename] = data

    return var_data_netcdf


def gpw_transformation(file_obj, name, nodata):
    """Tranformation function for the gridded population dataset

    Args:
        file_obj (s3fs object): s3fs sile object for one file of the dataset
        name (str): name of the file to be transformed
        nodata (int): Nodata value as specified by the data provider

    Returns:
        dict: Dictionary with the COG name and its corresponding data array.
    """

    var_data_netcdf = {}
    xds = xarray.open_dataarray(file_obj, engine="rasterio")

    filename = name.split("/")[-1]
    filename_elements = re.split("[_ .]", filename)
    # # insert date of generated COG into filename
    filename_elements.pop()
    filename_elements.append(filename_elements[-3])
    xds = xds.where(xds == nodata, -9999)
    xds.rio.set_spatial_dims("x", "y", inplace=True)
    xds.rio.write_crs("epsg:4326", inplace=True)
    xds.rio.write_nodata(-9999, inplace=True)

    cog_filename = "_".join(filename_elements)
    # # add extension
    cog_filename = f"{cog_filename}.tif"
    var_data_netcdf[cog_filename] = xds
    return var_data_netcdf


def geos_oco2_transformation(file_obj, name, nodata):
    """Tranformation function for the oco2 geos dataset

    Args:
        file_obj (s3fs object): s3fs sile object for one file of the dataset
        name (str): name of the file to be transformed
        nodata (int): Nodata value as specified by the data provider

    Returns:
        dict: Dictionary with the COG name and its corresponding data array.
    """
    var_data_netcdf = {}
    xds = xarray.open_dataset(file_obj)
    xds = xds.assign_coords(lon=(((xds.lon + 180) % 360) - 180)).sortby("lon")
    variable = [var for var in xds.data_vars]
    for time_increment in range(0, len(xds.time)):
        for var in variable:
            filename = name.split("/ ")[-1]
            filename_elements = re.split("[_ .]", filename)
            data = getattr(xds.isel(time=time_increment), var)
            data = data.isel(lat=slice(None, None, -1))
            data = data.where(data == nodata, -9999)
            data.rio.set_spatial_dims("lon", "lat", inplace=True)
            data.rio.write_crs("epsg:4326", inplace=True)
            data.rio.write_nodata(-9999, inplace=True)
            # # insert date of generated COG into filename
            filename_elements[-1] = filename_elements[-3]
            filename_elements.insert(2, var)
            filename_elements.pop(-3)
            cog_filename = "_".join(filename_elements)
            # # add extension
            cog_filename = f"{cog_filename}.tif"
            var_data_netcdf[cog_filename] = data

    return var_data_netcdf


def ecco_darwin_transformation(file_obj, name, nodata):
    """Tranformation function for the ecco darwin dataset

    Args:
        file_obj (s3fs object): s3fs sile object for one file of the dataset
        name (str): name of the file to be transformed
        nodata (int): Nodata value as specified by the data provider

    Returns:
        dict: Dictionary with the COG name and its corresponding data array.
    """
    var_data_netcdf = {}
    xds = xarray.open_dataset(file_obj)
    xds = xds.rename({"y": "latitude", "x": "longitude"})
    xds = xds.assign_coords(longitude=((xds.longitude / 1440) * 360) - 180).sortby(
        "longitude"
    )
    xds = xds.assign_coords(latitude=((xds.latitude / 721) * 180) - 90).sortby(
        "latitude"
    )

    variable = [var for var in xds.data_vars]

    for _ in xds.time.values:
        for var in variable[2:]:
            filename = name.split("/")[-1]
            filename_elements = re.split("[_ .]", filename)
            data = xds[var]

            data = data.reindex(latitude=list(reversed(data.latitude)))
            data = data.where(data == nodata, -9999)
            data.rio.set_spatial_dims("longitude", "latitude", inplace=True)
            data.rio.write_crs("epsg:4326", inplace=True)
            data.rio.write_nodata(-9999, inplace=True)

            filename_elements.pop()
            filename_elements[-1] = filename_elements[-2] + filename_elements[-1]
            filename_elements.pop(-2)
            # # insert date of generated COG into filename
            cog_filename = "_".join(filename_elements)
            # # add extension
            cog_filename = f"{cog_filename}.tif"
            var_data_netcdf[cog_filename] = data
    return var_data_netcdf
