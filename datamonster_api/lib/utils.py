import datetime
import decimal
from dateutil import parser
import numpy
import pandas as pd
import numpy as np
import json
import avro.schema
from io import BytesIO
from avro.datafile import DataFileWriter
from avro.io import DatumWriter

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

DATAFRAME_TYPES = {
    np.int64: 'long',
    np.float64: 'double',
    str: 'string',
    datetime.date: 'string',
    datetime.datetime: 'string',
    pd._libs.tslib.Timestamp: 'string',
    decimal.Decimal: 'double'  # These get returned as strings in the json response
}


def format_date(date):
    # get us a datetime.datetime object
    if isinstance(date, str):
        try:
            date = parser.parse(date)
        except ValueError:
            raise ValueError(ERROR_MESSAGE.format(date))

    if isinstance(date, numpy.datetime64):
        date = date.tolist()

    if isinstance(date, (pd.datetime, datetime.datetime, datetime.date)):
        return date.strftime("%Y-%m-%d")

    raise ValueError(ERROR_MESSAGE.format(date))


def dataframe_to_avro_bytes(df, name, namespace):
    data_fields = df.columns
    fields = []
    for field in data_fields:
        data_type = type(df[field][df[field].notnull()].iloc[0])
        item = {
            'name': field,
            'type': DATAFRAME_TYPES[data_type]
        }
        fields.append(item)

    schema = {
        'name': name,
        'namespace': namespace,
        'type': 'record',
        'fields': fields
    }

    df_lst = df.to_dict('records')
    df_lst = [convert_dict_fields_to_str(d) for d in df_lst]

    schema = json.dumps(schema)
    return avro_dumps(df_lst, schema)


def avro_dumps(data, schema):
    """dump the given data into an avro file with the provided schema"""
    schema = avro.schema.Parse(schema)
    fp = BytesIO()
    writer = DataFileWriter(fp, DatumWriter(), schema)
    if isinstance(data, list):
        for item in data:
            writer.append(item)
    else:
        writer.append(data)
    writer.flush()
    contents = fp.getvalue()
    fp.close()
    return contents


def convert_dict_fields_to_str(original, preserve_types=[float, int]):
    """Given a dictionary, convert the specified fields to string"""

    preserve_types = preserve_types or []
    new = {k: v if type(v) in preserve_types else str(v) for k, v in original.items()}
    return new
