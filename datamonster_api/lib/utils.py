import datetime
from dateutil import parser
import numpy
import pandas


ACCEPTED_DATETIMES = [
    "datetime.date",
    "datetime.datetime",
    "numpy.datetime64",
    "pandas.datetime",
    "string (%Y-%m-%d)",
    "string (%Y/%m/%d)",
]


def format_date(date):
    # get us a datetime.datetime object
    if isinstance(date, str):
        date = parser.parse(date)
    if isinstance(date, numpy.datetime64):
        date = date.tolist()

    if isinstance(date, (pandas.datetime, datetime.datetime, datetime.date)):
        return date.strftime("%Y-%d-%m")

    raise ValueError(
        "Unsupported datetime type found [{}] {}. Please pass one of the following: {}".format(
            type(date), date, ACCEPTED_DATETIMES
        )
    )
