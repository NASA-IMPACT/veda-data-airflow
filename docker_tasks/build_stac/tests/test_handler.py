import contextlib
from typing import TYPE_CHECKING, Any, Dict, Type
from unittest.mock import MagicMock, Mock
from pydantic import ValidationError

import pytest
from pystac import Item

import handler
from utils import stac, events

if TYPE_CHECKING:
    from functools import _SingleDispatchCallable


def build_mock_stac_item(item: Dict[str, Any]) -> MagicMock:
    """
    Build a mocked STAC Item from a dict
    """
    expected_stac_item = MagicMock(spec=Item)
    expected_stac_item.to_dict.return_value = item
    return expected_stac_item


@contextlib.contextmanager
def override_registry(
    dispatch_callable: "_SingleDispatchCallable[Any]", cls: Type, mock: Mock
):
    """
    Helper to override a singledispatch function with a mock for testing.
    """
    original = dispatch_callable.registry[cls]
    dispatch_callable.register(cls, mock)
    try:
        yield mock
    finally:
        dispatch_callable.register(cls, original)


def test_routing_regex_event():
    """
    Ensure that the system properly identifies, classifies, and routes regex-style events.
    """
    regex_event = {
        "collection": "test-collection",
        "s3_filename": "s3://test-bucket/delivery/BMHD_Maria_Stages/70001_BeforeMaria_Stage0_2017-07-21.tif",
        "granule_id": None,
        "datetime_range": None,
        "start_datetime": None,
        "end_datetime": None,
    }

    with override_registry(
        stac.generate_stac,
        events.RegexEvent,
        MagicMock(return_value=build_mock_stac_item({"mock": "STAC Item 1"})),
    ) as called_mock, override_registry(
        stac.generate_stac,
        events.CmrEvent,
        MagicMock(),
    ) as not_called_mock:
        handler.handler(regex_event, None)

    called_mock.assert_called_once_with(events.RegexEvent.parse_obj(regex_event))
    assert not not_called_mock.call_count


def test_routing_cmr_event():
    """
    Ensure that the system properly identifies, classifies, and routes CMR-style events.
    """
    cmr_event = {
        "collection": "test-collection",
        "s3_filename": "s3://test-bucket/delivery/BMHD_Maria_Stages/70001_BeforeMaria_Stage0_2017-07-21.tif",
        "granule_id": "test-granule",
    }

    with override_registry(
        stac.generate_stac,
        events.CmrEvent,
        MagicMock(return_value=build_mock_stac_item({"mock": "STAC Item 1"})),
    ) as called_mock, override_registry(
        stac.generate_stac,
        events.RegexEvent,
        MagicMock(),
    ) as not_called_mock:
        handler.handler(cmr_event, None)

    called_mock.assert_called_once_with(events.CmrEvent.parse_obj(cmr_event))
    assert not not_called_mock.call_count


@pytest.mark.parametrize(
    "bad_event",
    [{"collection": "test-collection"}],
)
def test_routing_unexpected_event(bad_event):
    """
    Ensure that a malformatted event raises a validation error
    """
    with pytest.raises(ValidationError):
        handler.handler(bad_event, None)
