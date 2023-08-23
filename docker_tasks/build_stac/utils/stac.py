import os

import pystac
import rasterio
from rasterio.session import AWSSession
from rio_stac import stac

from . import events, regex, role


def get_sts_session():
    if role_arn := os.environ.get("EXTERNAL_ROLE_ARN"):
        creds = role.assume_role(role_arn, "veda-data-pipelines_build-stac")
        return AWSSession(
            aws_access_key_id=creds["AccessKeyId"],
            aws_secret_access_key=creds["SecretAccessKey"],
            aws_session_token=creds["SessionToken"],
        )
    return


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
        source = [asset.href for asset in assets.values()][0]

    stac_item = stac.create_stac_item(
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
    return stac_item


def generate_stac(event: events.RegexEvent) -> pystac.Item:
    """
    Generate STAC item from user provided datetime range or regex & filename
    """
    start_datetime = end_datetime = single_datetime = None
    if event.start_datetime and event.end_datetime:
        start_datetime = event.start_datetime
        end_datetime = event.end_datetime
        single_datetime = None
    elif single_datetime := event.single_datetime:
        start_datetime = end_datetime = None
        single_datetime = single_datetime
    else:
        # Having multiple assets, we try against all filenames.
        for asset_name, asset in event.assets.items():
            try:
                filename = asset["href"].split("/")[-1]
                start_datetime, end_datetime, single_datetime = regex.extract_dates(
                    filename, event.datetime_range
                )
                break
            except Exception:
                continue
    # Raise if dates can't be found
    if not (start_datetime or end_datetime or single_datetime):
        raise ValueError("No dates found in event config or by regex")

    properties = event.properties or {}
    if start_datetime and end_datetime:
        properties["start_datetime"] = start_datetime.isoformat()
        properties["end_datetime"] = end_datetime.isoformat()
        single_datetime = None
    assets = {}

    rasterio_kwargs = {}
    rasterio_kwargs["session"] = get_sts_session()
    with rasterio.Env(
        session=rasterio_kwargs.get("session"),
        options={**rasterio_kwargs},
    ):
        for asset_name, asset_definition in event.assets.items():
            with rasterio.open(asset_definition["href"]) as src:
                media_type = stac.get_media_type(src)
            assets[asset_name] = pystac.Asset(
                title=asset_definition["title"],
                description=asset_definition["description"],
                href=asset_definition["href"],
                media_type=media_type,
                roles=["data", "layer"],
            )
        create_item_response = create_item(
            item_id=event.item_id,
            properties=properties,
            datetime=single_datetime,
            collection=event.collection,
            assets=assets,
        )
        return create_item_response
