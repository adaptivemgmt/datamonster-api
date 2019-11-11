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

ERROR_MESSAGE = "Unsupported date found {{0.__class__}}={{0!r}}. Please pass one of the following {accepted}".format(
    accepted=ACCEPTED_DATETIMES
)


def format_date(date):
    # get us a datetime.datetime object
    if isinstance(date, str):
        try:
            date = parser.parse(date)
        except ValueError:
            raise ValueError(ERROR_MESSAGE.format(date))

    if isinstance(date, numpy.datetime64):
        date = date.tolist()

    if isinstance(date, (pandas.datetime, datetime.datetime, datetime.date)):
        return date.strftime("%Y-%m-%d")

    raise ValueError(ERROR_MESSAGE.format(date))
