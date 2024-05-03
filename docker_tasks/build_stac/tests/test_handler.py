import contextlib
from typing import TYPE_CHECKING, Any, Dict, Type
from unittest.mock import MagicMock, Mock

import handler
import pytest
from pydantic import ValidationError
from pystac import Item
from utils import events, stac

if TYPE_CHECKING:
    from functools import _SingleDispatchCallable


def build_mock_stac_item(item: Dict[str, Any]) -> MagicMock:
    """
    Build a mocked STAC Item from a dict
    """
    expected_stac_item = MagicMock(spec=Item)
    expected_stac_item.to_dict.return_value = item
    return expected_stac_item


def test_routing_regex_event():
    """
    Ensure that the system properly identifies, classifies, and routes regex-style events.
    """
    regex_event =  {
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
    response = handler.handler(regex_event)
    print(response)

    assert response


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