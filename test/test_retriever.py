import datetime

import pytz
from hamcrest import assert_that, is_, calling, raises

from gmails.retriever import as_us_pacific, as_timestamp

PST_TIME_ZONE = pytz.timezone("US/Pacific")
PST_AWARE_DATETIME = PST_TIME_ZONE.localize(datetime.datetime(2020, 3, 14, 12, 23, 40))


def test_timezone_aware_datetime_in_pst_stays_the_same():
    timezone_aware_datetime = PST_AWARE_DATETIME
    assert_that(as_us_pacific(timezone_aware_datetime), is_(timezone_aware_datetime))


def test_timezone_aware_datetime_in_uct_is_converted_to_pst():
    utc_aware_datetime = datetime.datetime(2020, 3, 14, 19, 23, 40, tzinfo=pytz.UTC)
    assert_that(as_us_pacific(utc_aware_datetime), is_(PST_AWARE_DATETIME))


def test_timezone_naive_datetime_is_treated_as_provided_local_time_zone():
    naive_datetime = datetime.datetime(2020, 3, 14, 15, 23, 40)

    assert_that(as_us_pacific(naive_datetime, pytz.timezone('US/Eastern')), is_(PST_AWARE_DATETIME))


def test_timezone_naive_datetime_and_no_local_time_zone_raise_exception():
    naive_datetime = datetime.datetime(2020, 3, 14, 15, 23, 40)

    assert_that(calling(as_us_pacific).with_args(naive_datetime), raises(ValueError))


def test_date_is_treated_as_provided_local_time_zone():
    naive_date = datetime.date(2020, 3, 14)

    assert_that(as_us_pacific(naive_date, pytz.timezone('US/Eastern')),
                is_(PST_TIME_ZONE.localize(datetime.datetime(2020, 3, 13, 21, 0, 0))))


def test_date_and_no_local_time_zone_raise_exception():
    naive_date = datetime.date(2020, 3, 14)

    assert_that(calling(as_us_pacific).with_args(naive_date), raises(ValueError))


def test_timestamp_is_returned_for_datetime():
    assert_that(as_timestamp(PST_AWARE_DATETIME), is_(1584213820))
