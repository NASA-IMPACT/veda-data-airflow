import tempfile
import boto3
import pandas as pd
import rasterio
import s3fs
import re
import transformation_functions as plugins
import importlib


# datefmt = "month"
# nodata = -9999
# # lat_name, lon_name = 'lat', 'lon'
# lat_name, lon_name, time_name = "latitude", "longitude", "months"
# variable_list = ["fossil", "microbial", "pyrogenic", "total"]

files_processed = pd.DataFrame(columns=["file_name", "COGs_created"])

# name = '/Users/vgaur/ghgc-docs/data/casa-gfed/GEOSCarb_CASAGFED3v3_Flux.Monthly.x720_y360.2004.nc'
# name = '/Users/vgaur/ghgc-docs/data/oco2geos-co2-daygrid-v10r/oco2_GEOS_L3CO2_day_20150101_B10206Ar.nc4'
# name = '/Users/vgaur/ghgc-docs/data/tm54dvar-ch4flux-mask-monthgrid-v5/methane_emis_1999.nc'
# name = '/Users/vgaur/ghgc-docs/data/gpw/gpw_v4_population_density_rev11_2000_30_sec.tif'
# name = '/Users/vgaur/ghgc-docs/data/odiac_data/2000/odiac2022_1km_excl_intl_0001.tif'
def transform_cog(name_list, datefmt, variables_list, nodata, raw_data_bucket, dest_data_bucket, cog_data_prefix, collection_name, lon_name, lat_name, time_name, ext):
            
    session = boto3.session.Session()
    s3_client = session.client("s3")
    module = importlib.import_module('transformation_functions')
    function_name = f'{collection_name.replace('-', '_')}_transformation'

    if hasattr(module, function_name):
        for name in name_list:
            url = f"s3://{raw_data_bucket}/{name}"
            fs = s3fs.S3FileSystem()
            print('the url is', url)
            with fs.open(url, mode='rb') as file_obj:
                transform_func = getattr(module, function_name)
                data, cog_filename = transform_func(file_obj, name, nodata)

                # generate COG
                COG_PROFILE = {"driver": "COG", "compress": "DEFLATE"}

                with tempfile.NamedTemporaryFile() as temp_file:
                    data.rio.to_raster(temp_file.name, **COG_PROFILE)
                    s3_client.upload_file(
                        Filename=temp_file.name,
                        Bucket=dest_data_bucket,
                        Key=f"{cog_data_prefix}/{collection_name}/{cog_filename}",
                    )
            print(cog_filename)
