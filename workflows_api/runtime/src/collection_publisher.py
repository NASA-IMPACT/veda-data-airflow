from typing import Union

import requests
import fsspec
import xarray as xr
import xstac
from src.schemas import (
    COGDataset,
    DashboardCollection,
    DataType,
    SpatioTemporalExtent,
    ZarrDataset,
)
from src.validators import get_s3_credentials

import json

class CollectionPublisher:
    def ingest(self, collection: DashboardCollection, token: str, ingest_api: str):
        """
        Takes a collection model,
        does necessary preprocessing,
        and loads into the PgSTAC collection table
        """
        url = f"{ingest_api}/collections"
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }
        response = requests.post(url, data=collection.json(by_alias=True), headers=headers)
        if response.status_code == 200:
            print("Success:", response.json())
        else:
            print("Error:", response.status_code, response.text)


# TODO refactor
class Publisher:
    common_fields = [
        "title",
        "description",
        "license",
        "links",
        "time_density",
        "is_periodic",
        "renders",
        "stac_extensions"
    ]
    common = {
        "links": [],
        "extent": {
            "spatial": {"bbox": [[-180, -90, 180, 90]]},
            "temporal": {"interval": [[None, None]]},
        },
        "type": "Collection",
        "stac_version": "1.0.0",
    }

    def __init__(self) -> None:
        self.func_map = {
            DataType.zarr: self.create_zarr_collection,
            DataType.cog: self.create_cog_collection,
        }

    def get_template(self, dataset: Union[ZarrDataset, COGDataset]) -> dict:
        dataset_dict = dataset.dict()
        collection_dict = {
            "id": dataset_dict["collection"],
            **Publisher.common,
            **{
                key: dataset_dict[key]
                for key in Publisher.common_fields
                if key in dataset_dict.keys()
            },
        }
        return collection_dict

    def _create_zarr_template(self, dataset: ZarrDataset, store_path: str) -> dict:
        template = self.get_template(dataset)
        template["assets"] = {
            "zarr": {
                "href": store_path,
                "title": "Zarr Array Store",
                "description": "Zarr array store with one or several arrays (variables)",
                "roles": ["data", "zarr"],
                "type": "application/vnd+zarr",
                "xarray:open_kwargs": {
                    "engine": "zarr",
                    "chunks": {},
                    **dataset.xarray_kwargs,
                },
            }
        }
        return template

    def create_zarr_collection(self, dataset: ZarrDataset) -> dict:
        """
        Creates a zarr stac collection based off of the user input
        """
        s3_creds = get_s3_credentials()
        discovery = dataset.discovery_items[0]
        store_path = f"s3://{discovery.bucket}/{discovery.prefix}{discovery.zarr_store}"
        template = self._create_zarr_template(dataset, store_path)
        store = fsspec.get_mapper(store_path, client_kwargs=s3_creds)
        ds = xr.open_zarr(
            store, consolidated=bool(dataset.xarray_kwargs.get("consolidated"))
        )

        collection = xstac.xarray_to_stac(
            ds,
            template,
            temporal_dimension=dataset.temporal_dimension or "time",
            x_dimension=dataset.x_dimension or "lon",
            y_dimension=dataset.y_dimension or "lat",
            reference_system=dataset.reference_system or 4326,
        )
        return collection.to_dict()

    def create_cog_collection(self, dataset: COGDataset) -> dict:
        collection_stac = self.get_template(dataset)
        collection_stac["extent"] = SpatioTemporalExtent.parse_obj(
            {
                "spatial": {
                    "bbox": [
                        list(dataset.spatial_extent.dict(exclude_unset=True).values())
                    ]
                },
                "temporal": {
                    "interval": [
                        # most of our data uses the Z suffix for UTC - isoformat() doesn't
                        [
                            x.isoformat().replace("+00:00", "Z")
                            for x in list(
                                dataset.temporal_extent.dict(
                                    exclude_unset=True
                                ).values()
                            )
                        ]
                    ]
                },
            }
        )
        collection_stac["item_assets"] = {}

        dataset_dict = dataset.dict(exclude_unset=True)

        discovery_items_assets = []
        if (dataset_dict.get("discovery_items")):
            discovery_items_assets = [discovery_item.get("assets") for discovery_item in dataset_dict.get("discovery_items") if discovery_item.get("assets") is not None]

        if dataset_dict.get("item_assets"):
            collection_stac["item_assets"] = dataset_dict.get("item_assets")
        elif discovery_items_assets:
            for discovery_asset in discovery_items_assets:
                for key, asset in discovery_asset.items():
                    collection_stac["item_assets"][key] = {
                        k: v for k, v in asset.items() if k != "regex"
                    }
        else:
            collection_stac["item_assets"] = {
                "cog_default": {
                    "type": "image/tiff; application=geotiff; profile=cloud-optimized",
                    "roles": ["data", "layer"],
                    "title": "Default COG Layer",
                    "description": "Cloud optimized default layer to display on map",
                    "regex": "*"
                }
            }

        return collection_stac

    def generate_stac(
        self, dataset: Union[COGDataset, ZarrDataset], data_type: str
    ) -> dict:
        create_function = self.func_map.get(data_type, self.create_cog_collection)
        return create_function(dataset)

    def ingest(self, collection: DashboardCollection, token: str, ingest_api: str):
        """
        Takes a collection model,
        does necessary preprocessing,
        and loads into the PgSTAC collection table
        """
        
        url = f"{ingest_api}/collections"
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }
        response = requests.post(url, data=collection.json(by_alias=True), headers=headers)
        if response.status_code == 200:
            print("Success:", response.json())
        else:
            print("Error:", response.status_code, response.text)
