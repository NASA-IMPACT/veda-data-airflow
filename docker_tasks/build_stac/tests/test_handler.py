import pytest
import handler
from pydantic import ValidationError

def test_handler():
  event = {
            "collection": "TEST_COLLECTION",
            "item_id": "test_2024-04-24.tif",
            "assets": {
                "TEST_FILE": {
                    "title": "Test_FILE",
                    "description": "TEST_FILE, described",
                    "href": "./docker_tasks/build_stac/tests/test_2024-04-24.tif",
                }
            }
        }
  response =  handler.handler(event)

  assert "error" not in response["stac_item"]
  assert response["stac_item"]["id"] == "test_2024-04-24.tif"

def test_handler_malformed_tif():
  event = {
          "collection": "TEST_COLLECTION",
          "item_id": "broken__2024-04-24.tif",
          "assets": {
              "TEST_FILE": {
                  "title": "Test_FILE",
                  "description": "TEST_FILE, described",
                  "href": "./docker_tasks/build_stac/tests/broken.tif",
              }
          }
      }
  response =  handler.handler(event)

  assert "error" in response["stac_item"]

def test_handler_missing_asset():
  event = {
            "collection": "TEST_COLLECTION",
            "item_id": "test.tif",
            "assets": {
                "TEST_FILE": {
                    "title": "Test_FILE",
                    "description": "TEST_FILE, described",
                    "href": "./docker_tasks/build_stac/tests/test.tif",
                }
            }
        }
  response =  handler.handler(event)

  assert "error" in response["stac_item"]
  
@pytest.mark.parametrize(
    "bad_event",
    [{"collection": "test-collection"}],
)
def test_routing_unexpected_event(bad_event):
    """
    Ensure that a malformatted event raises a validation error
    """
    with pytest.raises(ValidationError):
        handler.handler(bad_event)