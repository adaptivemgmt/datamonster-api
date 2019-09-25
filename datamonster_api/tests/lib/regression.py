import datetime
import numpy
import pandas
import pytest

from datamonster_api import DataMonster, Aggregation, DataMonsterError
from regression_keys import DM_API_KEY_ID, DM_API_SECRET


dm = DataMonster(DM_API_KEY_ID, DM_API_SECRET, server="http://staging.adaptivemgmt.com")


def assert_data_frame(df, length):
    assert all(
        df.columns
        == [u"dimensions", u"end_date", u"start_date", u"value", u"time_span"]
    )
    assert all(
        df.dtypes.values
        == [
            numpy.dtype("O"),
            numpy.dtype("<M8[ns]"),
            numpy.dtype("<M8[ns]"),
            numpy.dtype("float64"),
            numpy.dtype("<m8[ns]"),
        ]
    )
    assert len(df) == length


def assert_frame_equal(df1, df2):
    from pandas.util.testing import assert_frame_equal

    assert_frame_equal(
        df1.reset_index(drop=True).sort_index(axis=1),
        df2.reset_index(drop=True).sort_index(axis=1),
    )


def test_company():
    company = dm.get_company_by_id(335)
    assert company.pk == 335
    assert company.name == "AMAZON"
    assert company.ticker == "AMZN"
    assert company.type == "Company"
    amzn_data_sources = set(company.datasources)
    amzn_data_source = dm.get_datasource_by_id("cd924848-5c49-4622-95a7-ee6d2cfe24b7")
    assert amzn_data_source.name in {i.name for i in amzn_data_sources}
    assert len(amzn_data_sources) == 241

    company = dm.get_company_by_id(1257)
    assert company.name == "MASTERCARD SECTOR INSIGHTS"
    assert company.type == "Macro"


def test_aggregation():
    company = dm.get_company_by_id(335)
    agg = Aggregation(period="week", company=company)
    assert agg.period == "week"
    assert agg.company == company


def test_data_source():
    """ Test 1010data Blended Credit Dataset
    """
    company = dm.get_company_by_ticker("W")
    assert company.name == "WAYFAIR"
    assert company.ticker == "W"
    assert company.pk == 128

    ds = next(
        dm.get_datasources(query="1010data Blended Credit & Debit Sales Index YoY")
    )
    assert ds.name == "1010data Blended Credit & Debit Sales Index YoY"
    assert ds.id == "3de84b2e-604f-4ea7-901f-61601eef8e0e"
    assert ds.category == "Blended Payment Data"
    assert len(list(ds.companies)) == 190

    df = ds.get_data(company, end_date="2017-09-09")
    assert_data_frame(df, 28)
    records = {
        "dimensions": {u"category": u"Wayfair 6-day Adjusted", u"country": u"US"},
        "end_date": pandas.to_datetime("2014-03-31"),
        "start_date": pandas.to_datetime("2014-03-31"),
        "value": 0.701535,
        "time_span": datetime.timedelta(days=1),
    }
    assert_frame_equal(df.head(1), pandas.DataFrame.from_records([records]))

    df = ds.get_data(company, start_date="2016-01-01", end_date="2017-09-01")
    assert_data_frame(df, 12)
    records = {
        "dimensions": {u"category": u"Wayfair Overall", u"country": u"US"},
        "end_date": pandas.to_datetime("2016-03-31"),
        "start_date": pandas.to_datetime("2016-03-31"),
        "value": 0.684006,
        "time_span": datetime.timedelta(days=1),
    }
    assert_frame_equal(df.head(1), pandas.DataFrame.from_records([records]))

    with pytest.raises(DataMonsterError):
        ds.get_data(company, Aggregation(period="quarter", company=company))


def test_data_source_2():
    """ Test FactSet Actuals Sales Quarterly, one of the more popular data sets
    """
    company = dm.get_company_by_id(335)

    sales = next(dm.get_datasources(query="FactSet Actuals Sales Quarterly"))
    assert sales.id == "bdcac6ae-4f31-4aaf-a92a-12854f09c768"
    assert sales.name == "FactSet Actuals Sales Quarterly"
    assert sales.category == "Company Fundamentals"

    df = sales.get_data(company, end_date="2017-09-01")
    assert_data_frame(df, 70)
    records = {
        "dimensions": {},
        "end_date": pandas.to_datetime("2000-03-31"),
        "start_date": pandas.to_datetime("2000-03-31"),
        "value": 573889.0,
        "time_span": datetime.timedelta(days=1),
    }
    assert_frame_equal(df.head(1), pandas.DataFrame.from_records([records]))

    agg = Aggregation(period="quarter", company=company)
    df = sales.get_data(company, agg, start_date="2010-01-01", end_date="2017-09-01")
    assert_data_frame(df, 30)
    records = {
        "dimensions": {},
        "end_date": pandas.to_datetime("2010-03-31"),
        "start_date": pandas.to_datetime("2010-01-01"),
        "value": 7131000.0,
        "time_span": datetime.timedelta(days=90),
    }
    assert_frame_equal(df.head(1), pandas.DataFrame.from_records([records]))


def test_bigger_data_source():
    """ Test SimilarWeb which is a bigger dataset
    """
    company = dm.get_company_by_id(335)
    agg = Aggregation(period="week", company=company)

    ds = dm.get_datasource_by_id("5899e237-874c-4e77-9d2e-c4b6cff218e8")
    assert ds.name == "SimilarWeb Direct Volume"

    df = ds.get_data(company, end_date="2018-01-01")
    assert_data_frame(df, 7584)

    df = ds.get_data(company, agg, end_date="2018-01-01")
    assert_data_frame(df, 1094)


def test_estimate_data_source():
    """ Test Factset Estimates Sales Quarterly Data, this is currently WIP
    """
    estimate = dm.get_datasource_by_id("0d07adb8-291e-4f4f-9c27-bbe2519e89e7")
    assert estimate.name == "FactSet Estimates Sales Quarterly"
    # estimate.get_data() is WIP
