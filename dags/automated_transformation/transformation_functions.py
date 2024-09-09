import boto3
import xarray as xr
import re


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
def collection_name_transformation(file_obj, name, nodata):

    xds = xr.open_dataset(file_obj)
    # xds = xds.rename({lon_name: "longitude", lat_name: "latitude", time_name: "time"})
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

            filename_elements.pop()
            filename_elements.append(f"{datefmt}")
            filename_elements.append(f'{time_increment.replace("-", "")}')
            # # insert date of generated COG into filename
            cog_filename = "_".join(filename_elements)
            # # add extension
            cog_filename = f"{cog_filename}.tif"

    return data, cog_filename