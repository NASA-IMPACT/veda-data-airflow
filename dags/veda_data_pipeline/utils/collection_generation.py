from typing import Any, Dict

import fsspec
import xarray as xr
import xstac
from veda_data_pipeline.utils.schemas import SpatioTemporalExtent
from datetime import datetime, timezone


class GenerateCollection:
    common = {
        "links": [],
        "extent": {
            "spatial": {"bbox": [[-180, -90, 180, 90]]},
            "temporal": {"interval": [[None, None]]},
        },
        "type": "Collection",
        "stac_version": "1.0.0",
    }
    keys_to_ignore = [
        "collection",
        "data_type",
        "sample_files",
        "discovery_items",
        "spatial_extent",
        "temporal_extent",
        "is_periodic",
        "time_density",
        "type",
    ]

    def get_template(self, dataset: Dict[str, Any]) -> dict:
        extra_fields = {
            key: dataset[key]
            for key in dataset.keys()
            if key not in GenerateCollection.keys_to_ignore
        }

        collection_dict = {
            "id": dataset["collection"],
            **GenerateCollection.common,
            **extra_fields,
        }

        # Default REQUIRED fields
        if not collection_dict.get("description"):
            collection_dict["description"] = dataset["collection"]
        if not collection_dict.get("license"):
            collection_dict["license"] = "proprietary"

        return collection_dict

    def _create_zarr_template(self, dataset: Dict[str, Any], store_path: str) -> dict:
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

    def create_zarr_collection(self, dataset: Dict[str, Any], role_arn: str) -> dict:
        """
        Creates a zarr stac collection based off of the user input
        """
        discovery = dataset.discovery_items[0]
        store_path = f"s3://{discovery.bucket}/{discovery.prefix}{discovery.zarr_store}"
        template = self._create_zarr_template(dataset, store_path)

        fs = fsspec.filesystem("s3", anon=False, role_arn=role_arn)
        store = fs.get_mapper(store_path)
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

    def create_cog_collection(self, dataset: Dict[str, Any]) -> dict:
        collection_stac = self.get_template(dataset)

        # Override the collection template extents if they exists
        if spatial_extent := dataset.get("spatial_extent"):
            collection_stac["extent"]["spatial"] = {"bbox": [list(spatial_extent.values())]}
        
        if temporal_extent := dataset.get("temporal_extent"):
            collection_stac["extent"]["temporal"] = {
                "interval": [
                    [
                        x
                        if x else None
                        for x in list(temporal_extent.values())
                    ]
                ]
            }

        # Handle conflicting STAC<>Dataset keys
        discovery_items_assets = []
        if (dataset.get("discovery_items", None)):
            discovery_items_assets = [
                discovery_item.get("assets") for discovery_item in dataset.get("discovery_items") if discovery_item.get("assets", None) is not None
            ]

        # Use item asset descriptions from discovery if provided
        if dataset.get("item_assets", None):
            collection_stac["item_assets"] = dataset.get("item_assets", None)

        # Also update item asset descriptions with any additional assets defined in discovery config
        for discovery_asset in discovery_items_assets:
            for key, asset in discovery_asset.items():
                collection_stac["item_assets"][key] = {
                    k: v for k, v in asset.items() if k != "regex"
                }
        # If no item asset descriptions provided in dataset or discovery config, add cog_default
        if dataset.get("item_assets", None) is None:
            collection_stac["item_assets"] = {
                "cog_default": {
                    "type": "image/tiff; application=geotiff; profile=cloud-optimized",
                    "roles": ["data", "layer"],
                    "title": "Default COG Layer",
                    "description": "Cloud optimized default layer to display on map"
                }
            }

        return collection_stac

    def generate_stac(
        self, dataset_config: Dict[str, Any], role_arn: str = None
    ) -> dict:
        """
        Generates a STAC collection based on the dataset and data type

        Args:
            dataset_config (Dict[str, Any]): dataset configuration
            role_arn (str): role arn for Zarr collection generation
        """
        data_type = dataset_config.get("data_type", "cog")
        if data_type == "zarr":
            return self.create_zarr_collection(dataset_config, role_arn)
        else:
            return self.create_cog_collection(dataset_config)
