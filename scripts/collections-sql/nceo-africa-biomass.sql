INSERT INTO pgstac.collections (content) VALUES('{
   "id": "nceo_africa_2017",
   "type": "Collection",
   "links": [
       {
            "href": "https://ceos.org/gst/africa-biomass.html",
            "rel": "external",
            "type": "text/html",
            "title": "NCEO Africa Aboveground Woody Biomass 2017 (CEOS Website)"
       }
   ],
   "title":"NCEO Africa Aboveground Woody Biomass 2017",
   "extent":{
      "spatial":{
         "bbox":[
            [
               -18.2735295,
               -35.0540590,
               51.8642329,
               37.7310386
            ]
         ]
      },
      "temporal": {
         "interval":[
            [
               "2017-01-01T00:00:00Z",
               "2018-01-01T00:00:00Z"
            ]
         ]
      }
   },
   "license": "MIT",
   "description": "The NCEO Africa Aboveground Woody Biomass (AGB) map for the year 2017 at 100 m spatial resolution was developed using a combination of LiDAR, Synthetic Aperture Radar (SAR) and optical based data. This product was developed by the UK’s National Centre for Earth Observation (NCEO) through the Carbon Cycle and Official Development Assistance (ODA) programmes.",
   "stac_version": "1.0.0",
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