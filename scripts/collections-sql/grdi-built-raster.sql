INSERT INTO pgstac.collections (content) VALUES('{
    "id": "grdi-v1-built",
    "type": "Collection",
    "title": "GRDI BUILT Constituent Raster",
    "description": "Global Gridded Relative Deprivation Index (GRDI) built-up area (BUILT) Constituent raster, indexed 0 to 100",
    "stac_version": "1.0.0",
    "license": "MIT",
    "links": [],
    "extent": {
        "spatial": {
            "bbox": [
                [
                    -180.0, 
                    -56.0, 
                    180, 
                    82.18
                ]
            ]
        },
        "temporal": {
            "interval": [
                [
                    "2015-01-01T00:00:00Z",
                    "2021-12-31T23:59:59Z"
                ]
            ]
        }
    },
    "dashboard:is_periodic": false,
    "dashboard:time_density": null,
    "item_assets": {
        "cog_default": {
            "type": "image/tiff; application=geotiff; profile=cloud-optimized",
            "roles": [
                "data",
                "layer"
            ],
            "title": "Default COG Layer",
            "description": "Cloud optimized default layer to display on map"
        }
    }
}')
ON CONFLICT (id) DO UPDATE
  SET content = excluded.content;
