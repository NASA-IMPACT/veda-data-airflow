from typing import Any, Dict

import fsspec
import xarray as xr
import xstac
from veda_data_pipeline.utils.schemas import SpatioTemporalExtent


class GenerateCollection:
    common_fields = [
        "title",
        "description",
        "license",
        "links",
        "time_density",
        "is_periodic",
        "renders",
        "stac_extensions",
        "assets",
        "item_assets",
        "summaries",
        "providers"
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

            
    def get_template(self, dataset: Dict[str, Any]) -> dict:
        extra_fields = {
                key: dataset[key]
                for key in dataset.keys()
                if key not in GenerateCollection.common_fields and key not in ["collection", "data_type", "sample_files", "discovery_items"]
            }
        if extra_fields:
            # elevated potential for ingestion issues with extra fields, so we log them out here
            print(f"Extra fields: {extra_fields}")

        collection_dict = {
            "id": dataset["collection"],
            **GenerateCollection.common,
            **{
                key: dataset[key]
                for key in GenerateCollection.common_fields
                if key in dataset.keys()
            },
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

        # Override the extents if they exists
        if (spatial_extent := dataset.get("spatial_extent")) or (
            temporal_extent := dataset.get("temporal_extent")
        ):
            collection_stac["extent"] = SpatioTemporalExtent.parse_obj(
                {
                    "spatial": {"bbox": [list(spatial_extent)]},
                    "temporal": {
                        "interval": [
                            # most of our data uses the Z suffix for UTC - isoformat() doesn't
                            [
                                x.isoformat().replace("+00:00", "Z")
                                for x in list(temporal_extent)
                            ]
                        ]
                    },
                }
            )

        collection_stac["item_assets"] = {
            "cog_default": {
                "type": "image/tiff; application=geotiff; profile=cloud-optimized",
                "roles": ["data", "layer"],
                "title": "Default COG Layer",
                "description": "Cloud optimized default layer to display on map",
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
