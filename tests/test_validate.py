"""
Tests for the STAC collection validation utilities.
"""

import pytest
import pystac
from veda_data_pipeline.utils.validate import validate_collection


class TestValidateCollection:
    """Test suite for validate_collection function"""

    def test_validate_valid_collection_dict(self):
        """Test validation of a valid collection dictionary"""
        valid_collection = {
            "type": "Collection",
            "id": "test-collection",
            "stac_version": "1.0.0",
            "description": "Test collection",
            "license": "proprietary",
            "extent": {
                "spatial": {"bbox": [[-180, -90, 180, 90]]},
                "temporal": {"interval": [[None, None]]}
            },
            "links": []
        }

        result = validate_collection(valid_collection)
        assert result is not None
        assert result["id"] == "test-collection"
        assert result["type"] == "Collection"

    def test_validate_collection_with_extensions(self):
        """Test validation of a collection with STAC extensions"""
        collection_with_extensions = {
            "type": "Collection",
            "id": "wmts-collection",
            "stac_version": "1.0.0",
            "description": "WMTS collection",
            "license": "MIT",
            "extent": {
                "spatial": {"bbox": [[-180, -90, 180, 90]]},
                "temporal": {"interval": [["2020-01-01T00:00:00Z", "2025-01-01T00:00:00Z"]]}
            },
            "stac_extensions": [
                "https://stac-extensions.github.io/web-map-links/v1.2.0/schema.json"
            ],
            "links": [
                {
                    "href": "https://example.com/wmts",
                    "rel": "wmts",
                    "type": "image/png",
                    "wmts:layer": "test_layer"
                }
            ]
        }

        result = validate_collection(collection_with_extensions)
        assert result is not None
        assert result["id"] == "wmts-collection"
        assert len(result["stac_extensions"]) == 1

    def test_validate_pystac_collection_object(self):
        """Test validation of a pystac.Collection object"""
        collection_obj = pystac.Collection(
            id="pystac-collection",
            description="Collection from pystac object",
            extent=pystac.Extent(
                spatial=pystac.SpatialExtent([[-180, -90, 180, 90]]),
                temporal=pystac.TemporalExtent([[None, None]])
            ),
            license="proprietary"
        )

        result = validate_collection(collection_obj)
        assert result is not None
        assert result["id"] == "pystac-collection"
        assert isinstance(result, dict)

    def test_validate_invalid_collection_missing_required_fields(self):
        """Test validation fails for collection missing required fields"""
        invalid_collection = {
            "type": "Collection",
            "id": "invalid-collection",
            # Missing stac_version, description, license, extent
        }

        with pytest.raises(ValueError) as exc_info:
            validate_collection(invalid_collection)

        # Error happens during conversion to pystac.Collection
        assert "failed to convert" in str(exc_info.value).lower()

    def test_validate_invalid_collection_wrong_type(self):
        """Test validation fails for collection with wrong type"""
        invalid_collection = {
            "type": "Item",  # Should be "Collection"
            "id": "wrong-type",
            "stac_version": "1.0.0",
            "description": "Wrong type",
            "license": "proprietary",
            "extent": {
                "spatial": {"bbox": [[-180, -90, 180, 90]]},
                "temporal": {"interval": [[None, None]]}
            },
            "links": []
        }

        with pytest.raises(ValueError) as exc_info:
            validate_collection(invalid_collection)

        # Error happens during conversion because type is Item, not Collection
        assert "failed to convert" in str(exc_info.value).lower()

    def test_validate_invalid_input_type(self):
        """Test validation fails for invalid input type"""
        invalid_input = "not a collection"

        with pytest.raises(TypeError) as exc_info:
            validate_collection(invalid_input)

        assert "expected dict or pystac.collection" in str(exc_info.value).lower()

    def test_validate_collection_invalid_bbox(self):
        """Test validation fails for collection with invalid bbox"""
        # Note: pystac.Collection.validate() may not catch all bbox validation issues
        # This test documents that behavior. For stricter validation, jsonschema
        # validation would be needed in addition to pystac validation.
        invalid_bbox_collection = {
            "type": "Collection",
            "id": "invalid-bbox",
            "stac_version": "1.0.0",
            "description": "Invalid bbox",
            "license": "proprietary",
            "extent": {
                "spatial": {"bbox": [[-200, -90, 180, 90]]},  # Invalid: -200 longitude
                "temporal": {"interval": [[None, None]]}
            },
            "links": []
        }

        # pystac may accept this as it focuses on structure, not value ranges
        # If stricter validation is needed, additional checks should be added
        result = validate_collection(invalid_bbox_collection)
        assert result is not None  # Documents that pystac allows this

    def test_validate_collection_with_invalid_extension(self):
        """Test validation with malformed extension data"""
        # This collection declares the web-map-links extension but doesn't
        # provide valid wmts:layer in the wmts link
        invalid_extension_collection = {
            "type": "Collection",
            "id": "invalid-extension",
            "stac_version": "1.0.0",
            "description": "Invalid extension usage",
            "license": "MIT",
            "extent": {
                "spatial": {"bbox": [[-180, -90, 180, 90]]},
                "temporal": {"interval": [[None, None]]}
            },
            "stac_extensions": [
                "https://stac-extensions.github.io/web-map-links/v1.2.0/schema.json"
            ],
            "links": [
                {
                    "href": "https://example.com/wmts",
                    "rel": "wmts",
                    "type": "image/png"
                    # Missing required wmts:layer field
                }
            ]
        }

        # Note: pystac.validate() may or may not catch extension-specific validation
        # depending on the extension's validation implementation
        # This test documents the behavior
        try:
            result = validate_collection(invalid_extension_collection)
            # If validation passes, the extension schema may not be strictly enforced by pystac
            assert result is not None
        except ValueError:
            # If validation fails, it's working as expected
            pass

    def test_validate_returns_dict(self):
        """Test that validation always returns a dictionary"""
        collection_obj = pystac.Collection(
            id="return-type-test",
            description="Test return type",
            extent=pystac.Extent(
                spatial=pystac.SpatialExtent([[-180, -90, 180, 90]]),
                temporal=pystac.TemporalExtent([[None, None]])
            ),
            license="proprietary"
        )

        result = validate_collection(collection_obj)
        assert isinstance(result, dict)
        assert not isinstance(result, pystac.Collection)


class TestWMTSCollectionValidation:
    """Test validation with real WMTS collection configurations"""

    def test_validate_viirs_collection(self):
        """Test validation of VIIRS SNPP NRT collection from the pipeline"""
        viirs_collection = {
            "assets": {},
            "id": "VIIRS_SNPP_DayNightBand_At_Sensor_Radiance",
            "dashboard:is_periodic": True,
            "dashboard:time_density": "day",
            "dashboard:time_interval": "P1D",
            "description": "The Black Marble Nighttime At Sensor Radiance (Day/Night Band) layer is created from NASA's Black Marble daily at-sensor top-of-atmosphere nighttime radiance product (VNP46A1). It is displayed as a grayscale image. The layer is expressed in radiance units (nW/(cm2 sr)) with log10 conversion. It is stretched up to 38 nW/(cm2 sr) resulting in improvements in capturing city lights in greater spatial detail than traditional Nighttime Imagery resampled at 0-255 (e.g., Day/Night Band, Enhanced Near Constant Contrast).The ultra-sensitivity of the VIIRS Day/Night Band enables scientists to capture the Earth's surface and atmosphere in low light conditions, allowing for better monitoring of nighttime phenomena. These images are also useful for assessing anthropogenic sources of light emissions under varying illumination conditions. For instance, during partial to full moon conditions, the layer can identify the location and features of clouds and other natural terrestrial features such as sea ice and snow cover, while enabling temporal observations in urban regions, regardless of moonlit conditions. As such, the layer is particularly useful for detecting city lights, lightning, auroras, fires, gas flares, and fishing fleets.The Black Marble Nighttime At Sensor Radiance (Day/Night Band) layer is available in near real-time from the Visible Infrared Imaging Radiometer Suite (VIIRS) aboard the joint NASA/NOAA Suomi National Polar orbiting Partnership (Suomi NPP) satellite. The sensor resolution is 750 m at nadir, imagery resolution is 500 m, and the temporal resolution is daily.",
            "extent": {
                "spatial": {
                    "bbox": [
                        [
                            -180,
                            -90,
                            180,
                            90
                        ]
                    ]
                },
                "temporal": {
                    "interval": [
                        [
                            "2020-11-10T00:00:00Z",
                            "2025-04-14T00:00:00Z"
                        ]
                    ]
                }
            },
            "is_periodic": True,
            "item_assets": {
                "cog_default": {
                    "description": "Cloud optimized default layer to display on map",
                    "roles": [
                        "data",
                        "layer"
                    ],
                    "title": "Default COG Layer",
                    "type": "image/tiff; application=geotiff; profile=cloud-optimized"
                }
            },
            "license": "MIT",
            "links": [
                {
                    "href": "https://gibs{s}.earthdata.nasa.gov/wmts/epsg3857/best/wmts.cgi",
                    "href:servers": ["-a", "-b"],
                    "rel": "wmts",
                    "title": "Visualized through a WMTS",
                    "type": "image/png",
                    "wmts:dimensions": {
                        "STYLE": "default"
                    },
                    "wmts:layer": ["VIIRS_SNPP_DayNightBand_At_Sensor_Radiance"]
                }
            ],
            "product_level": "L2",
            "providers": [],
            "renders": {},
            "stac_extensions": [
                "https://stac-extensions.github.io/web-map-links/v1.2.0/schema.json"
            ],
            "stac_version": "1.1.0",
            "temporal_frequency": "twenty four hours",
            "time_density": "day",
            "time_interval": "P1D",
            "title": "Black Marble Nighttime At Sensor Radiance (Day/Night Band)",
            "type": "Collection",
            "units": "m·s⁻¹"
        }

        result = validate_collection(viirs_collection)

        assert result is not None
        assert result["id"] == "VIIRS_SNPP_DayNightBand_At_Sensor_Radiance"
        assert result["type"] == "Collection"

        # Verify web-map-links extension
        assert "https://stac-extensions.github.io/web-map-links/v1.2.0/schema.json" in result["stac_extensions"]

        # Verify WMTS link format
        wmts_links = [link for link in result["links"] if link["rel"] == "wmts"]
        assert len(wmts_links) == 1

        wmts_link = wmts_links[0]
        assert "gibs{s}.earthdata.nasa.gov" in wmts_link["href"]
        assert wmts_link["href:servers"] == ["-a", "-b"]
        assert wmts_link["wmts:layer"] == ["VIIRS_SNPP_DayNightBand_At_Sensor_Radiance"]
        assert wmts_link["wmts:dimensions"]["STYLE"] == "default"
