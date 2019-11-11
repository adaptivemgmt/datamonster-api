import collections
import datetime
import numpy
import pandas
import pytest

from datamonster_api import DataMonster, Aggregation, DataMonsterError
from regression_keys import DM_API_KEY_ID, DM_API_SECRET

QA_ETL_UUID = "57588c68-e262-49b4-b05a-8ae4c30c183b"

dm = DataMonster(DM_API_KEY_ID, DM_API_SECRET, server="http://localhost:5000")


def assert_data_frame(df, length, value_type="float64"):
    assert sorted(df.columns) == [
        "dimensions",
        "end_date",
        "start_date",
        "time_span",
        "value",
    ]
    assert collections.Counter(df.dtypes.values) == collections.Counter(
        [
            numpy.dtype("<M8[ns]"),
            numpy.dtype("<M8[ns]"),
            numpy.dtype(value_type),
            numpy.dtype("<m8[ns]"),
            numpy.dtype("O"),
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
    company = dm.get_company_by_id(79)
    assert company == dm.get_company_by_ticker("AAP")
    assert company.pk == 79
    assert company.name == "ADVANCE AUTO PARTS"
    assert company.ticker == "AAP"
    assert company.type == "Company"
    assert company.quarters
    assert type(company.quarters) == list
    assert company.quarters[0] == "04-21-2001"

    company = dm.get_company_by_id(1257)
    assert company.name == "MASTERCARD SECTOR INSIGHTS"
    assert company.type == "Macro"


def test_data_source():
    data_source = dm.get_datasource_by_id(QA_ETL_UUID)
    assert data_source == dm.get_datasource_by_name("Static QA Fountain")
    assert data_source.id == QA_ETL_UUID
    assert data_source.name == "Static QA Fountain"
    assert data_source.uri == "/rest/v1/datasource/" + QA_ETL_UUID
    assert data_source.category == "Web Scrape Data"
    assert len(list(data_source.companies)) == 2
    details = data_source.get_details()
    assert details["earliestData"] == "2017-07-01"
    assert details["category"] == "Web Scrape Data"


def test_aggregation():
    company = dm.get_company_by_id(335)
    agg = Aggregation(period="week", company=company)
    assert agg.period == "week"
    assert agg.company == company


def test_get_data():
    data_source = dm.get_datasource_by_id(QA_ETL_UUID)
    company = dm.get_company_by_id(79)

    df = data_source.get_data(company, end_date="2017-09-01")
    assert len(df) == 122

    df = data_source.get_data(company, end_date="2019-01-01")
    assert_data_frame(df, 1096, "int64")
    records = {
        "dimensions": {"country": "USA", "category": "small"},
        "end_date": pandas.to_datetime("2017-07-01"),
        "start_date": pandas.to_datetime("2017-07-01"),
        "value": 701,
        "time_span": datetime.timedelta(days=1),
    }
    assert_frame_equal(df.head(1), pandas.DataFrame.from_records([records]))

    df = data_source.get_data(company, start_date="2018-12-10", end_date="2019-01-01")
    assert_data_frame(df, 42, "int64")
    records = {
        "dimensions": {"country": "USA", "category": "large"},
        "end_date": pandas.to_datetime("2018-12-10"),
        "start_date": pandas.to_datetime("2018-12-10"),
        "value": 1210,
        "time_span": datetime.timedelta(days=1),
    }
    assert_frame_equal(df.head(1), pandas.DataFrame.from_records([records]))

    df = data_source.get_data(
        company, Aggregation(period="quarter", company=company), end_date="2019-01-01"
    )
    assert_data_frame(df, 24)
    records = {
        "dimensions": {"country": "USA", "category": "small"},
        "end_date": pandas.to_datetime("2017-09-30"),
        "start_date": pandas.to_datetime("2017-07-01"),
        "value": 813.553191,
        "time_span": datetime.timedelta(days=92),
    }
    assert_frame_equal(df.head(1), pandas.DataFrame.from_records([records]))


def test_get_data_qa_today():
    data_source = dm.get_datasource_by_id(QA_ETL_UUID)
    company = dm.get_company_by_id(79)
    yest = datetime.datetime.now() - datetime.timedelta(days=1)
    df = data_source.get_data(company, start_date=yest)
    assert_data_frame(df, 2, "int64")
    parsed_date = pandas.to_datetime(yest.strftime("%Y-%m-%d"))
    records = [
        {
            "dimensions": {
                "category": "large" if yest.day % 2 == 0 else "small",
                "country": "USA",
            },
            "end_date": parsed_date,
            "start_date": parsed_date,
            "value": int("{}{}".format(yest.month, yest.day)),
            "time_span": datetime.timedelta(days=1),
        },
        {
            "dimensions": {
                "category": "large" if yest.day % 2 == 0 else "small",
                "country": "GB",
            },
            "end_date": parsed_date,
            "start_date": parsed_date,
            "value": int("1{}{}".format(yest.month, yest.day)),
            "time_span": datetime.timedelta(days=1),
        },
    ]
    assert_frame_equal(df, pandas.DataFrame.from_records(records))


def test_get_data_simple():
    """ Test 1010data Blended Credit Dataset
    """
    company = dm.get_company_by_ticker("W")
    ds = next(
        dm.get_datasources(query="1010data Blended Credit & Debit Sales Index YoY")
    )
    assert ds.name == "1010data Blended Credit & Debit Sales Index YoY"
    assert ds.id == "3de84b2e-604f-4ea7-901f-61601eef8e0e"
    assert ds.category == "Blended Payment Data"
    assert ds.type == "datasource"
    assert len(list(ds.companies)) == 190

    df = ds.get_data(company, end_date="2017-09-09")
    assert_data_frame(df, 28)
    records = {
        "dimensions": {"category": "Wayfair Overall", "country": "US"},
        "end_date": pandas.to_datetime("2014-03-31"),
        "start_date": pandas.to_datetime("2014-03-31"),
        "value": 0.694088518955128,
        "time_span": datetime.timedelta(days=1),
    }
    assert_frame_equal(df.head(1), pandas.DataFrame.from_records([records]))

    df = ds.get_data(company, start_date="2016-01-01", end_date="2017-09-01")
    assert_data_frame(df, 12)
    records = {
        "dimensions": {"category": "Wayfair Overall", "country": "US"},
        "end_date": pandas.to_datetime("2016-03-31"),
        "start_date": pandas.to_datetime("2016-03-31"),
        "value": 0.684296477362873,
        "time_span": datetime.timedelta(days=1),
    }
    assert_frame_equal(df.head(1), pandas.DataFrame.from_records([records]))

    with pytest.raises(DataMonsterError):
        ds.get_data(company, Aggregation(period="quarter", company=company))


def test_get_data_factset():
    """ Test `FactSet Actuals Sales Quarterly` one of the more popular data sets
    """
    company = dm.get_company_by_id(335)
    sales = next(dm.get_datasources(query="FactSet Actuals Sales Quarterly"))
    assert sales.id == "bdcac6ae-4f31-4aaf-a92a-12854f09c768"
    assert sales.name == "FactSet Actuals Sales Quarterly"

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


def test_get_data_bigger():
    """ Test `SimilarWeb Direct Volume` which is a bigger dataset
    """
    company = dm.get_company_by_id(335)
    ds = dm.get_datasource_by_id("5899e237-874c-4e77-9d2e-c4b6cff218e8")

    df = ds.get_data(company, end_date="2018-01-01")
    assert_data_frame(df, 7584)

    agg = Aggregation(period="week", company=company)
    df = ds.get_data(company, agg, end_date="2018-01-01")
    assert_data_frame(df, 1094)


def test_get_data_estimate():
    """ Test `Factset Estimates Sales Quarterly` which is a non datamonster data source
    """

    def assert_estimate_data_frame(df, length):
        assert sorted(df.columns) == [
            "average",
            "currency_code",
            "end_date",
            "estimate_count",
            "high",
            "low",
            "median",
            "section_pk",
            "start_date",
            "std_dev",
            "target_date",
        ]
        assert sorted(
            df.dtypes.values
            == [
                numpy.dtype("float64"),
                numpy.dtype("<M8[ns]"),
                numpy.dtype("int64"),
                numpy.dtype("int64"),
                numpy.dtype("float64"),
                numpy.dtype("float64"),
                numpy.dtype("float64"),
                numpy.dtype("<M8[ns]"),
                numpy.dtype("float64"),
                numpy.dtype("<M8[ns]"),
                numpy.dtype("O"),
            ]
        )
        assert len(df) == length

    estimate = dm.get_datasource_by_id("0d07adb8-291e-4f4f-9c27-bbe2519e89e7")
    assert estimate.name == "FactSet Estimates Sales Quarterly"
    assert estimate.type == "Datamonster Estimates"
    company = dm.get_company_by_id(335)

    df = estimate.get_data(company, end_date="2018-01-01")
    assert_estimate_data_frame(df, 4349)

    with pytest.raises(DataMonsterError):
        agg = Aggregation(period="week", company=company)
        df = estimate.get_data(company, agg, end_date="2018-01-01")
