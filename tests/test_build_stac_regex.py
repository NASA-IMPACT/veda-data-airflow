from datetime import datetime

import pytest
from veda_data_pipeline.utils.build_stac.utils.regex import extract_dates

# Test cases mirror the doc examples at:
# https://docs.openveda.cloud/user-guide/content-curation/dataset-ingestion/file-preparation.html#name-your-files-correctly


# ---------------------------------------------------------------------------
# Single datetime
# ---------------------------------------------------------------------------


class TestSingleDatetimeYear:
    def test_compact(self):
        start, end, single = extract_dates("nightlights_2012.tif", None)
        assert (start, end, single) == (None, None, datetime(2012, 1, 1))

    def test_compact_with_label_suffix(self):
        start, end, single = extract_dates("nightlights_2012-yearly.tif", None)
        assert (start, end, single) == (None, None, datetime(2012, 1, 1))

    def test_range_expansion(self):
        start, end, single = extract_dates("nightlights_2012.tif", "year")
        assert (start, end, single) == (
            datetime(2012, 1, 1),
            datetime(2012, 12, 31),
            None,
        )


class TestSingleDatetimeMonth:
    def test_compact(self):
        start, end, single = extract_dates("nightlights_201201.tif", None)
        assert (start, end, single) == (None, None, datetime(2012, 1, 1))

    def test_hyphen_separated(self):
        start, end, single = extract_dates("nightlights_2012-01_monthly.tif", None)
        assert (start, end, single) == (None, None, datetime(2012, 1, 1))

    def test_range_expansion(self):
        start, end, single = extract_dates("nightlights_201201.tif", "month")
        assert (start, end, single) == (
            datetime(2012, 1, 1),
            datetime(2012, 1, 31),
            None,
        )

    def test_range_expansion_hyphen_separated(self):
        # YYYY-MM format should parse the month correctly, not fall back to year-only.
        start, end, single = extract_dates(
            "202409_nightlights_final_MonthlyComposite_2024-08_monthly.tif",
            "month",
        )
        assert (start, end, single) == (
            datetime(2024, 8, 1),
            datetime(2024, 8, 31),
            None,
        )


class TestSingleDatetimeDay:
    def test_compact(self):
        start, end, single = extract_dates("nightlights_20120101day.tif", None)
        assert (start, end, single) == (None, None, datetime(2012, 1, 1))

    def test_hyphen_separated(self):
        start, end, single = extract_dates("nightlights_2012-01-01_day.tif", None)
        assert (start, end, single) == (None, None, datetime(2012, 1, 1))

    def test_range_expansion(self):
        start, end, single = extract_dates("nightlights_2012-01-01_day.tif", "day")
        assert (start, end, single) == (
            datetime(2012, 1, 1),
            datetime(2012, 1, 1, 23, 59, 59),
            None,
        )


# ---------------------------------------------------------------------------
# Datetime range
# ---------------------------------------------------------------------------


class TestDatetimeRangeYear:
    def test_compact(self):
        start, end, single = extract_dates("nightlights_2012_2014.tif", None)
        assert (start, end, single) == (
            datetime(2012, 1, 1),
            datetime(2014, 1, 1),
            None,
        )

    def test_compact_with_label_between(self):
        start, end, single = extract_dates("nightlights_2012_year_2015.tif", None)
        assert (start, end, single) == (
            datetime(2012, 1, 1),
            datetime(2015, 1, 1),
            None,
        )

    def test_returns_sorted_range(self):
        # Dates listed in reverse order in the filename should still be sorted.
        start, end, single = extract_dates("nightlights_2015_2012.tif", None)
        assert (start, end, single) == (
            datetime(2012, 1, 1),
            datetime(2015, 1, 1),
            None,
        )


class TestDatetimeRangeMonth:
    def test_compact(self):
        start, end, single = extract_dates("nightlights_201201_201205.tif", None)
        assert (start, end, single) == (
            datetime(2012, 1, 1),
            datetime(2012, 5, 1),
            None,
        )

    def test_hyphen_separated(self):
        start, end, single = extract_dates(
            "nightlights_2012-01_month_2012-06_data.tif", None
        )
        assert (start, end, single) == (
            datetime(2012, 1, 1),
            datetime(2012, 6, 1),
            None,
        )


class TestDatetimeRangeDay:
    def test_compact(self):
        start, end, single = extract_dates("nightlights_20120101day_20121221.tif", None)
        assert (start, end, single) == (
            datetime(2012, 1, 1),
            datetime(2012, 12, 21),
            None,
        )

    def test_hyphen_separated(self):
        start, end, single = extract_dates(
            "nightlights_2012-01-01_to_2012-12-31_day.tif", None
        )
        assert (start, end, single) == (
            datetime(2012, 1, 1),
            datetime(2012, 12, 31),
            None,
        )


# ---------------------------------------------------------------------------
# Sub-daily datetime
# ---------------------------------------------------------------------------


class TestSubDailyDatetime:
    def test_iso_datetime_dashes(self):
        # YYYY-MM-DDThh:mm:ss — the trailing Z is ignored by the regex.
        start, end, single = extract_dates(
            "TEMPO_NO2_L3_V03_2024-04-11T19:09:53Z.tif", None
        )
        assert (start, end, single) == (None, None, datetime(2024, 4, 11, 19, 9, 53))

    @pytest.mark.xfail(
        reason="Compact sub-daily format YYYYMMDDThh:mm:ss is not supported; "
        "colons in the time component prevent the YYYYMMDDTHHMMSS regex from matching."
    )
    def test_compact_datetime_with_colons(self):
        # The regex expects YYYYMMDDThhmmss (no colons), so 20240411T19:09:53Z raises.
        start, end, single = extract_dates(
            "TEMPO_NO2_L3_V03_20240411T19:09:53Z.tif", None
        )
        assert (start, end, single) == (None, None, datetime(2024, 4, 11, 19, 9, 53))


# ---------------------------------------------------------------------------
# Error case
# ---------------------------------------------------------------------------


def test_no_date_raises():
    with pytest.raises(Exception, match="No dates provided"):
        extract_dates("nightlights_daily_avg.tif", None)
