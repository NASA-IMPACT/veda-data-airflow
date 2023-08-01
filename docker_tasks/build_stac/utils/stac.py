import os

import pystac
import rasterio
from rasterio.session import AWSSession
from rio_stac import stac

from . import events, regex, role


def create_item(
    item_id,
    properties,
    datetime,
    collection,
    assets,
) -> pystac.Item:
    """
    Function to create a stac item from a COG using rio_stac
    """
    if "cog_default" in assets:
        source = assets["cog_default"].href
    else:
        source = [asset.href for asset in assets][0]

    def create_stac_item():
        create_stac_item_respose = stac.create_stac_item(
            id=item_id,
            source=source,
            collection=collection,
            input_datetime=datetime,
            properties=properties,
            with_proj=True,
            with_raster=True,
            assets=assets,
            geom_densify_pts=10,
            geom_precision=5,
        )
        return create_stac_item_respose

    rasterio_kwargs = {}
    if role_arn := os.environ.get("EXTERNAL_ROLE_ARN"):
        print("We're inside")
        creds = role.assume_role(role_arn, "veda-data-pipelines_build-stac")
        rasterio_kwargs["session"] = AWSSession(
            aws_access_key_id=creds["AccessKeyId"],
            aws_secret_access_key=creds["SecretAccessKey"],
            aws_session_token=creds["SessionToken"],
        )
    print(rasterio_kwargs.get("session"))
    with rasterio.Env(
        session=rasterio_kwargs.get("session"),
        options={**rasterio_kwargs},
    ):
        create_stac_item_resp = create_stac_item()
        return create_stac_item_resp


def generate_stac(event: events.RegexEvent) -> pystac.Item:
    """
    Generate STAC item from user provided datetime range or regex & filename
    """
    if event.start_datetime and event.end_datetime:
        start_datetime = event.start_datetime
        end_datetime = event.end_datetime
        single_datetime = None
    elif single_datetime := event.single_datetime:
        start_datetime = end_datetime = None
        single_datetime = single_datetime
    else:
        start_datetime, end_datetime, single_datetime = regex.extract_dates(
            event.s3_filename, event.datetime_range
        )
    properties = event.properties or {}
    if start_datetime and end_datetime:
        properties["start_datetime"] = start_datetime.isoformat()
        properties["end_datetime"] = end_datetime.isoformat()
        single_datetime = None
    assets = {}
    for asset_name, asset_definition in event.assets:
        with rasterio.open(source=asset_definition["href"]) as src:
            media_type = stac.get_media_type(src)
        assets[asset_name] = pystac.Asset(
            title=asset_definition["title"],
            description=asset_definition["description"],
            href=asset_definition["href"],
            media_type=media_type,
            roles=[],
        )
    create_item_response = create_item(
        item_id=event.item_id,
        properties=properties,
        datetime=single_datetime,
        collection=event.collection,
        assets=assets,
    )
    return create_item_response
