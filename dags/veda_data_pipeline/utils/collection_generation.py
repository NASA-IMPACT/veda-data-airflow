from typing import Dict, Any

import fsspec
import xstac
import xarray as xr

from veda_data_pipeline.utils.schemas import SpatioTemporalExtent

class GenerateCollection:
    common_fields = [
        "title",
        "description",
        "license",
        "links",
        "time_density",
        "is_periodic",
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
        collection_dict = {
            "id": dataset["collection"],
            **GenerateCollection.common,
            **{
                key: dataset[key]
                for key in GenerateCollection.common_fields
                if key in dataset.keys()
            },
        }
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
        self, dataset: Dict[str, Any], data_type: str, role_arn: str = None
    ) -> dict:
        if data_type == "zarr":
            return self.create_zarr_collection(dataset, role_arn)
        elif data_type == "cog":
            return self.create_cog_collection(dataset)
        else:
            raise ValueError(f"Data type {data_type} not supported")
