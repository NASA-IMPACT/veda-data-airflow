import re
from typing import Callable, Dict, Tuple, Union
from datetime import datetime
from dateutil.relativedelta import relativedelta

from . import events


DATERANGE = Tuple[datetime, datetime]


def _calculate_year_range(datetime_obj: datetime) -> DATERANGE:
    start_datetime = datetime_obj.replace(month=1, day=1)
    end_datetime = datetime_obj.replace(month=12, day=31)
    return start_datetime, end_datetime


def _calculate_month_range(datetime_obj: datetime) -> DATERANGE:
    start_datetime = datetime_obj.replace(day=1)
    end_datetime = datetime_obj + relativedelta(day=31)
    return start_datetime, end_datetime


def _calculate_day_range(datetime_obj: datetime) -> DATERANGE:
    start_datetime = datetime_obj
    end_datetime = datetime_obj + relativedelta(hour=23, minute=59, second=59)
    return start_datetime, end_datetime


DATETIME_RANGE_METHODS: Dict[events.INTERVAL, Callable[[datetime], DATERANGE]] = {
    "month": _calculate_month_range,
    "year": _calculate_year_range,
    "day": _calculate_day_range,
}


def extract_dates(
    filename: str, datetime_range: events.INTERVAL
) -> Union[Tuple[datetime, datetime, None], Tuple[None, None, datetime]]:
    """
    Extracts start & end or single date string from filename.
    """
    DATE_REGEX_STRATEGIES = [
        (r"_(\d{4}_\d{2}_\d{2})", "%Y_%m_%d"),
        (r"_(\d{4}-\d{2}-\d{2})", "%Y-%m-%d"),
        (r"_(\d{8})", "%Y%m%d"),
        (r"_(\d{6})", "%Y%m"),
        (r"_(\d{4})", "%Y"),
    ]

    # Find dates in filename
    dates = []
    for (pattern, dateformat) in DATE_REGEX_STRATEGIES:
        dates_found = re.compile(pattern).findall(filename)
        if not dates_found:
            continue

        for date_str in dates_found:
            dates.append(datetime.strptime(date_str, dateformat))

        break

    num_dates_found = len(dates)

    # No dates found
    if not num_dates_found:
        raise Exception(
            f"No dates provided in {filename=}. "
            "At least one date in format yyyy-mm-dd is required."
        )

    # Many dates found
    if num_dates_found > 1:
        dates.sort()
        start_datetime, *_, end_datetime = dates
        return start_datetime, end_datetime, None

    # Single date found
    single_datetime = dates[0]

    # Convert single date to range
    if datetime_range:
        start_datetime, end_datetime = DATETIME_RANGE_METHODS[datetime_range](
            single_datetime
        )
        return start_datetime, end_datetime, None

    # Return single date
    return None, None, single_datetime
