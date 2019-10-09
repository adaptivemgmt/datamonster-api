import datetime
import numpy
import pandas
import pytest

from datamonster_api import format_date


def test_good_date():
    assert format_date("2019-01-01") == "2019-01-01"
    assert format_date("2019/01/01") == "2019-01-01"
    assert format_date(datetime.datetime(2019, 1, 1)) == "2019-01-01"
    assert format_date(pandas.datetime(2019, 1, 1)) == "2019-01-01"
    assert format_date(numpy.datetime64("2019-01-01")) == "2019-01-01"


def test_bad_date():
    items = ["garbage", "2018-10-1010", "2018/10/1010", "///", "---", []]
    for item in items:
        with pytest.raises(ValueError):
            format_date(item)
