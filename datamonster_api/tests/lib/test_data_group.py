import pandas as pd
from datamonster_api import DataGroupColumn


def assert_object_matches_data_group(data_group, data_group_obj):
    assert data_group_obj["_id"] == data_group.id
    assert data_group_obj["name"] == data_group.name
    assert len(data_group_obj["columns"]) == len(data_group.columns)
    dg_col_name_to_type = {col.name: col.type_ for col in data_group.columns}
    for col in data_group_obj["columns"]:
        assert col["name"] in dg_col_name_to_type
        assert col["type_"] == dg_col_name_to_type[col["name"]]


def test_missing_column_is_missing(data_group):
    missing_date_col = pd.DataFrame([
        {'number col': 1, 'string col': 'a'},
        {'number col': 2, 'string col': 'a'},
    ])

    missing, extra, bad_dates = data_group._validate_schema(missing_date_col)
    assert(extra == [])
    assert(bad_dates == [])
    assert(len(missing) == 1)
    assert(str(missing[0]) == str(DataGroupColumn('date col', 'date')))

    missing_number_col = pd.DataFrame([
        {'date col': '2006-06-06', 'string col': 'a'},
        {'date col': '2006-06-06', 'string col': 'a'},
    ])

    missing, extra, bad_dates = data_group._validate_schema(missing_number_col)
    assert(extra == [])
    assert(bad_dates == [])
    assert(len(missing) == 1)
    assert(str(missing[0]) == str(DataGroupColumn('number col', 'number')))

    missing_string_col = pd.DataFrame([
        {'date col': '2006-06-06', 'number col': 1},
        {'date col': '2006-06-06', 'number col': 3},
    ])

    missing, extra, bad_dates = data_group._validate_schema(missing_string_col)
    assert(extra == [])
    assert(bad_dates == [])
    assert(len(missing) == 1)
    assert(str(missing[0]) == str(DataGroupColumn('string col', 'string')))


def test_bad_dates_not_missing_or_extra(data_group):
    bad_date_type = pd.DataFrame([
        {'date col': '2006-6-06', 'string col': 'a', 'number col': 1},
        {'date col': '2006-06-06', 'string col': 'b', 'number col': 2},
    ])

    missing, extra, bad_dates = data_group._validate_schema(bad_date_type)
    assert(len(bad_dates) == 1)
    assert(str(bad_dates[0]) == str(DataGroupColumn('date col', 'date')))
    assert(missing == [])
    assert(extra == [])


def test_bad_number_or_string_type_counted_as_missing_and_extra(data_group):
    bad_number_type = pd.DataFrame([
        {'date col': '2006-06-06', 'string col': 'a', 'number col': '1'},
        {'date col': '2006-06-07', 'string col': 'b', 'number col': '2'},
    ])

    missing, extra, bad_dates = data_group._validate_schema(bad_number_type)
    assert(bad_dates == [])
    assert(len(missing) == 1)
    assert(str(missing[0]) == str(DataGroupColumn('number col', 'number')))
    assert(len(extra) == 1)
    assert(str(extra[0]) == str(DataGroupColumn('number col', 'string')))

    bad_string_type = pd.DataFrame([
        {'date col': '2006-06-06', 'string col': 1, 'number col': 1},
        {'date col': '2006-06-07', 'string col': 2, 'number col': 2},
    ])

    missing, extra, bad_dates = data_group._validate_schema(bad_string_type)
    assert (bad_dates == [])
    assert (len(missing) == 1)
    assert (str(missing[0]) == str(DataGroupColumn('string col', 'string')))
    assert (len(extra) == 1)
    assert (str(extra[0]) == str(DataGroupColumn('string col', 'number')))
