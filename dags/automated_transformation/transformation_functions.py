import boto3
import xarray
import re
from datetime import datetime

def get_all_s3_keys(bucket, model_name, ext):
    """Get a list of all keys in an S3 bucket."""
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

# Naming convention for COG transformation is DATASETNAME_TRANSFORMATION
def tm54dvar_ch4flux_mask_monthgrid_v5_transformation(file_obj, name, nodata):

    xds = xarray.open_dataset(file_obj)
    xds = xds.rename({"latitude": "lat", "longitude": "lon"})
    xds = xds.assign_coords(lon=(((xds.lon + 180) % 360) - 180)).sortby("lon")
    variable = [var for var in xds.data_vars if "global" not in var]

    for time_increment in range(0, len(xds.months)):
        filename = name.split("/ ")[-1]
        filename_elements = re.split("[_ .]", filename)
        start_time = datetime(int(filename_elements[-2]), time_increment + 1, 1)
        for var in variable:
            data = getattr(xds.isel(months=time_increment), var)
            data = data.isel(lat=slice(None, None, -1))
            xds = xds.where(xds==nodata, -9999)
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

    return data, cog_filename