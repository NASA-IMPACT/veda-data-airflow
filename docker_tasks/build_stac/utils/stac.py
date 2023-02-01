import os
from pathlib import Path
from functools import singledispatch
import pystac
import rasterio
from cmr import GranuleQuery
from pystac.utils import str_to_datetime
from rio_stac import stac
from rasterio.session import AWSSession
from . import regex, events, role


def create_item(
    properties,
    datetime,
    cog_url,
    collection,
    assets=None,
    asset_name=None,
    asset_roles=None,
    asset_media_type=None,
) -> pystac.Item:
    """
    Function to create a stac item from a COG using rio_stac
    """

    def create_stac_item():
        create_stac_item_respose = stac.create_stac_item(
            id=Path(cog_url).stem,
            source=cog_url,
            collection=collection,
            input_datetime=datetime,
            properties=properties,
            with_proj=True,
            with_raster=True,
            assets=assets,
            asset_name=asset_name or "cog_default",
            asset_roles=asset_roles or ["data", "layer"],
            asset_media_type=(
                asset_media_type
                or "image/tiff; application=geotiff; profile=cloud-optimized"
            ),
        )
        return create_stac_item_respose

    rasterio_kwargs = {}
    creds = {}
    if os.environ.get("AccessKeyId"):
        creds = os.environ
    if role_arn := os.environ.get("EXTERNAL_ROLE_ARN"):
        creds = role.assume_role(role_arn, "veda-data-pipelines_build-stac")
    rasterio_kwargs["session"] = AWSSession(
        aws_access_key_id=creds["AccessKeyId"],
        aws_secret_access_key=creds["SecretAccessKey"],
        aws_session_token=creds["SessionToken"],
    )
    with rasterio.Env(
        session=rasterio_kwargs["session"],
        options={**rasterio_kwargs},
    ):
        create_stac_item_resp = create_stac_item()
        return create_stac_item_resp


@singledispatch
def generate_stac(item) -> pystac.Item:
    raise Exception(f"Unsupported event type: {type(item)=}, {item=}")


@generate_stac.register
def generate_stac_regexevent(item: events.RegexEvent) -> pystac.Item:
    """
    Generate STAC item from user provided datetime range or regex & filename
    """
    if item.start_datetime and item.end_datetime:
        start_datetime = item.start_datetime
        end_datetime = item.end_datetime
        single_datetime = None
    elif single_datetime := item.single_datetime:
        start_datetime = end_datetime = None
        single_datetime = single_datetime
    else:
        start_datetime, end_datetime, single_datetime = regex.extract_dates(
            item.s3_filename, item.datetime_range
        )
    properties = item.properties or {}
    if start_datetime and end_datetime:
        properties["start_datetime"] = start_datetime.isoformat()
        properties["end_datetime"] = end_datetime.isoformat()
        single_datetime = None
    create_item_response = create_item(
        properties=properties,
        datetime=single_datetime,
        cog_url=item.s3_filename,
        collection=item.collection,
        asset_name=item.asset_name,
        asset_roles=item.asset_roles,
        asset_media_type=item.asset_media_type,
    )
    return create_item_response


@generate_stac.register
def generate_stac_cmrevent(item: events.CmrEvent) -> pystac.Item:
    """
    Generate STAC Item from CMR granule
    """
    cmr_json = GranuleQuery().concept_id(item.granule_id).get(1)[0]

    return create_item(
        properties=cmr_json,
        datetime=str_to_datetime(cmr_json["time_start"]),
        cog_url=item.s3_filename,
        collection=item.collection,
        asset_name=item.asset_name,
        asset_roles=item.asset_roles,
        asset_media_type=item.asset_media_type,
    )
