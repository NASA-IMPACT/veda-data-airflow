INSERT INTO pgstac.collections (content) VALUES('{
   "id": "facebook_population_density",
   "type": "Collection",
   "links":[
       {
            "href": "https://arxiv.org/pdf/1712.05839.pdf",
            "rel": "external",
            "type": "application/pdf",
            "title": "Mapping the world population one building at a time"
       }
   ],
   "title":"Population Density Maps using satellite imagery built by Meta",
   "extent":{
      "spatial":{
         "bbox":[
            [
               -180.00041666666667,
                -55.985972222324634,
                179.82041666695605,
                71.33069444444445
            ]
         ]
      },
      "temporal":{
         "interval":[
            [
               "2015-01-01T00:00:00Z",
               null
            ]
         ]
      }
   },
   "license":"MIT",
   "description":"Facebook high-resolution population density: Darker areas indicate higher population density areas and lighter areas indicate lower population density areas, with a 30m² resolution.",
   "stac_version":"1.0.0",
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
