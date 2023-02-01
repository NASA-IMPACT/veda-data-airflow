import pytest

from datetime import datetime

from utils import regex


@pytest.mark.parametrize(
    "test_input,expected",
    [
        (
            # Single datetime - %Y-%m-%d
            ("s3://foo/bar/foo_2010-10-31_bar.tif", None),
            (None, None, datetime(2010, 10, 31)),
        ),
        (
            # Single datetime - %Y%m%d
            ("s3://foo/bar/foo_20051212_bar.tif", None),
            (None, None, datetime(2005, 12, 12)),
        ),
        (
            # Single datetime - %Y%m
            ("s3://foo/bar/foo_200507_bar.tif", None),
            (None, None, datetime(2005, 7, 1)),
        ),
        (
            # Single datetime - %Y
            ("s3://foo/bar/foo_2012_bar.tif", None),
            (None, None, datetime(2012, 1, 1)),
        ),
        (
            # Daterange - %Y-%m-%d
            ("s3://foo/bar/foo_2005-07-02_to_2006-09-29_bar.tif", None),
            (datetime(2005, 7, 2), datetime(2006, 9, 29), None),
        ),
        (
            # Daterange - %Y%m%d
            ("s3://foo/bar/foo_20050702_to_20060929_bar.tif", None),
            (datetime(2005, 7, 2), datetime(2006, 9, 29), None),
        ),
        (
            # Daterange - %Y
            ("s3://foo/bar/foo_2005_2006_2007_bar.tif", None),
            (datetime(2005, 1, 1), datetime(2007, 1, 1), None),
        ),
        (
            # Single date converted to month range - %Y-%m-%d
            ("s3://foo/bar/foo_2005-01-02.tif", "month"),
            (datetime(2005, 1, 1), datetime(2005, 1, 31), None),
        ),
        (
            # Single date converted to month range - %Y%m%d
            ("s3://foo/bar/foo_2005-02-02.tif", "month"),
            (datetime(2005, 2, 1), datetime(2005, 2, 28), None),
        ),
        (
            # Single date converted to month range - %Y%m
            ("s3://foo/bar/foo_20050302_bar.tif", "month"),
            (datetime(2005, 3, 1), datetime(2005, 3, 31), None),
        ),
        (
            # Single date converted to month range - %Y
            ("s3://foo/bar/foo_20050402_bar.tif", "month"),
            (datetime(2005, 4, 1), datetime(2005, 4, 30), None),
        ),
        (
            # Single date converted to year range - %Y-%m-%d
            ("s3://foo/bar/foo_2005-01-02.tif", "year"),
            (datetime(2005, 1, 1), datetime(2005, 12, 31), None),
        ),
        (
            # Single date converted to year range - %Y%m%d
            ("s3://foo/bar/foo_2005-02-02.tif", "year"),
            (datetime(2005, 1, 1), datetime(2005, 12, 31), None),
        ),
        (
            # Single date converted to year range - %Y%m
            ("s3://foo/bar/foo_20050302_bar.tif", "year"),
            (datetime(2005, 1, 1), datetime(2005, 12, 31), None),
        ),
        (
            # Single date converted to year range - %Y
            ("s3://foo/bar/foo_20050402_bar.tif", "year"),
            (datetime(2005, 1, 1), datetime(2005, 12, 31), None),
        ),
    ],
)
def test_date_extraction(test_input, expected):
    """
    Ensure dateranges are properly extracted from filenames.
    """
    assert regex.extract_dates(*test_input) == expected
