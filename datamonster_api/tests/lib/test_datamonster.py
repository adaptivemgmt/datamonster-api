import datetime
import pandas
import pytest
from six.moves.urllib.parse import urlparse, parse_qs

from datamonster_api import Aggregation, DataMonsterError, DataMonster


def _assert_object_matches_datasource(datasource, datasource_obj):
    assert datasource_obj["id"] == datasource.id
    assert datasource_obj["name"] == datasource.name
    assert datasource_obj["category"] == datasource.category
    assert datasource_obj["uri"] == datasource.uri


def test_equality(datasource, other_datasource):
    assert datasource == datasource
    assert other_datasource != datasource


def test_datamonster_datasource_mapper():
    from pandas.util.testing import assert_frame_equal

    df = pandas.DataFrame.from_records([])
    print(assert_frame_equal(DataMonster.datamonster_data_mapper({}, {}, df), df))
    assert_frame_equal(DataMonster.datamonster_data_mapper({}, {}, df), df)

    df = pandas.DataFrame.from_records([{"apple": 1, "banana": 2, "cherry": 3}])
    with pytest.raises(DataMonsterError):
        DataMonster.datamonster_data_mapper({"garbage": "g"}, {"split": "apple"}, df)

    with pytest.raises(DataMonsterError):
        DataMonster.datamonster_data_mapper(
            {"fruits": "value"}, {"fruits": ["apple", "banana"]}, df
        )


def test_get_datasources_1(mocker, dm, single_page_datasource_results, company):
    """Test getting datasources. multiple pages. various filters"""

    # The resulting datasources should always be the same
    def assert_results_good(results):
        results = list(results)
        assert len(results) == 2

        _assert_object_matches_datasource(
            results[0], single_page_datasource_results["results"][0]
        )

        _assert_object_matches_datasource(
            results[1], single_page_datasource_results["results"][1]
        )

    dm.client.get = mocker.Mock(return_value=single_page_datasource_results)

    # ++ No queries, no company
    dm.client.get.reset_mock()
    datasources = dm.get_datasources()

    assert_results_good(datasources)
    assert dm.client.get.call_count == 1
    assert dm.client.get.call_args[0][0] == "/rest/v1/datasource"

    # ++ text query, no company
    dm.client.get.reset_mock()
    datasources = dm.get_datasources(query="abc")

    assert_results_good(datasources)
    assert dm.client.get.call_count == 1
    assert dm.client.get.call_args[0][0] == "/rest/v1/datasource?q=abc"

    # ++ no text query, company
    dm.client.get.reset_mock()
    datasources = dm.get_datasources(company=company)

    assert_results_good(datasources)
    assert dm.client.get.call_count == 1
    assert dm.client.get.call_args[0][0] == "/rest/v1/datasource?companyId={}".format(
        company.id
    )

    # ++ text query, company
    dm.client.get.reset_mock()
    datasources = dm.get_datasources(query="abc", company=company)

    assert_results_good(datasources)
    assert dm.client.get.call_count == 1
    assert dm.client.get.call_args[0][
        0
    ] == "/rest/v1/datasource?q=abc&companyId={}".format(company.id)


def test_get_datasources_2(mocker, dm, single_page_datasource_results, company):
    """Test getting datasources. error states"""

    # Bad datasource
    with pytest.raises(DataMonsterError) as excinfo:
        dm.get_datasources(company="abc")

    assert "company argument must be a Company object" in excinfo.value.args[0]


def test_get_datasource_by_name(mocker, dm, datasource, other_datasource):
    """Test getting datasource by name"""
    dm.get_datasources = mocker.Mock(return_value=[datasource])
    assert datasource == dm.get_datasource_by_name("name")

    dm.get_datasources = mocker.Mock(return_value=[])
    with pytest.raises(DataMonsterError):
        datasource = dm.get_datasource_by_name("garbage")

    dm.get_datasources = mocker.Mock(return_value=[datasource, other_datasource])
    with pytest.raises(DataMonsterError):
        datasource = dm.get_datasource_by_name("garbage")


def test_get_datasource_by_id(mocker, dm, datasource_details_result):
    """Test getting datasource by uuid"""

    dm.client.get = mocker.Mock(return_value=datasource_details_result)

    datasource = dm.get_datasource_by_id("abc")

    # Make sure we hit the right endpoint
    assert dm.client.get.call_count == 1
    assert dm.client.get.call_args[0][0] == "/rest/v1/datasource/abc"

    # a couple of spot checks.
    assert datasource.category == "category"
    assert datasource.cadence == "daily"
    assert datasource.splitColumns == ["category", "country"]
    assert datasource.type == "datasource"

    assert datasource.earliestData == "2015-01-01"
    assert datasource.latestData == "2018-10-01"

    # Make sure we didn't go through the client again for the details
    assert dm.client.get.call_count == 1


def test_get_raw_data_1(
    mocker, dm, avro_data_file, company, datasource, datasource_details_result
):
    """Test getting raw data -- happy case"""
    datasource.get_details = mocker.Mock(return_value=datasource_details_result)
    dm.client.get = mocker.Mock(return_value=avro_data_file)

    schema, df = dm.get_raw_data(datasource, section_pk=company.id)

    # Check that we called the client correctly
    expected_path = "/rest/v1/datasource/{}/rawdata?section_pk={}".format(
        datasource.id, company.id
    )
    assert dm.client.get.call_count == 1
    assert dm.client.get.call_args[0][0] == expected_path
    assert dm.client.get.call_args[0][1] == {"Accept": "avro/binary"}

    assert len(df.columns) == 5
    assert sorted(df.columns) == [
        "avg_dollar_per_cust",
        "category",
        "country",
        "period_end",
        "period_start",
    ]
    assert len(df) == 8

    assert df.iloc[0]["category"] == "Amazon ex. Whole Foods"
    assert df.iloc[0]["country"] == "US"
    assert df.iloc[0]["avg_dollar_per_cust"] == 38.5165896068141
    assert df.iloc[0]["period_start"].date() == datetime.date(2019, 1, 2)
    assert df.iloc[0]["period_end"].date() == datetime.date(2019, 1, 3)

    assert df.iloc[7]["category"] == "Amazon Acquisition Adjusted"
    assert df.iloc[7]["country"] == "US"
    assert df.iloc[7]["avg_dollar_per_cust"] == 40.692421507668499
    assert df.iloc[7]["period_start"].date() == datetime.date(2019, 1, 2)
    assert df.iloc[7]["period_end"].date() == datetime.date(2019, 1, 3)


def test_get_data_1(
    mocker, dm, avro_data_file, company, datasource, datasource_details_result
):
    """Test getting data -- happy case"""
    datasource.get_details = mocker.Mock(return_value=datasource_details_result)
    dm.client.get = mocker.Mock(return_value=avro_data_file)

    df = dm.get_data(datasource, company)

    # Check that we called the client correctly
    expected_path = "/rest/v1/datasource/{}/rawdata?section_pk={}".format(
        datasource.id, company.id
    )
    assert dm.client.get.call_count == 1
    assert dm.client.get.call_args[0][0] == expected_path
    assert dm.client.get.call_args[0][1] == {"Accept": "avro/binary"}

    assert len(df.columns) == 5
    assert sorted(df.columns) == [
        "dimensions",
        "end_date",
        "start_date",
        "time_span",
        "value",
    ]
    assert len(df) == 8

    assert df.iloc[0]["dimensions"] == {"category": "Whole Foods", "country": "US"}
    assert df.iloc[0]["value"] == 52.6278787878788
    assert df.iloc[0]["start_date"].date() == datetime.date(2019, 1, 1)
    assert df.iloc[0]["time_span"].to_pytimedelta() == datetime.timedelta(days=1)
    assert df.iloc[0]["end_date"].date() == datetime.date(2019, 1, 1)

    assert df.iloc[7]["dimensions"] == {
        "category": "Amazon Acquisition Adjusted",
        "country": "US",
    }
    assert df.iloc[7]["value"] == 40.692421507668499
    assert df.iloc[7]["start_date"].date() == datetime.date(2019, 1, 2)
    assert df.iloc[7]["time_span"].to_pytimedelta() == datetime.timedelta(days=1)
    assert df.iloc[7]["end_date"].date() == datetime.date(2019, 1, 2)


def test_get_data_2(mocker, dm, avro_data_file, company, other_company, datasource):
    """Test getting data -- bad aggregations"""
    # ** aggregation period is invalid
    agg = Aggregation(period=123, company=None)
    with pytest.raises(DataMonsterError) as excinfo:
        dm.get_data(datasource, company, agg)

    assert "Bad Aggregation Period" in excinfo.value.args[0]

    # ** aggregation company is invalid
    agg = Aggregation(period="month", company=123)
    with pytest.raises(DataMonsterError) as excinfo:
        dm.get_data(datasource, company, agg)

    assert "Aggregation company must be Company" in excinfo.value.args[0]

    # ** fiscal quarter aggregation -- no company
    agg = Aggregation(period="fiscalQuarter", company=None)
    with pytest.raises(DataMonsterError) as excinfo:
        dm.get_data(datasource, company, agg)

    assert (
        "Company must be specified for a fiscalQuarter aggregation"
        in excinfo.value.args[0]
    )

    # ** fiscal quarter aggregation -- different company
    agg = Aggregation(period="fiscalQuarter", company=other_company)
    with pytest.raises(DataMonsterError) as excinfo:
        dm.get_data(datasource, company, agg)

    assert (
        "Aggregating by the fiscal quarter of a different company not yet supported"
        in excinfo.value.args[0]
    )


def test_get_data_3(
    mocker,
    dm,
    avro_data_file,
    company,
    other_company,
    datasource,
    datasource_details_result,
):
    """Test getting data -- good aggregations"""

    # ** monthly aggregation
    dm.client.get = mocker.Mock(return_value=avro_data_file)
    datasource.get_details = mocker.Mock(return_value=datasource_details_result)
    agg = Aggregation(period="month", company=None)

    dm.get_data(datasource, company, agg)

    url = urlparse(dm.client.get.call_args[0][0])
    query = parse_qs(url.query)

    assert url.path == "/rest/v1/datasource/{}/rawdata".format(datasource.id)
    assert len(query) == 2
    assert query["aggregation"] == ["month"]
    assert query["section_pk"] == [company.id]

    # ** fiscal quarter aggregation -- good company
    dm.client.get = mocker.Mock(return_value=avro_data_file)
    agg = Aggregation(period="fiscalQuarter", company=company)

    dm.get_data(datasource, company, agg)

    url = urlparse(dm.client.get.call_args[0][0])
    query = parse_qs(url.query)

    assert url.path == "/rest/v1/datasource/{}/rawdata".format(datasource.id)
    assert len(query) == 2
    assert query["aggregation"] == ["fiscalQuarter"]
    assert query["section_pk"] == [company.id]


def test_get_data_4(
    mocker,
    dm,
    avro_data_file,
    company,
    other_company,
    datasource,
    datasource_details_result,
):
    """Test getting data -- date filters"""

    # ** monthly aggregation, start date
    dm.client.get = mocker.Mock(return_value=avro_data_file)
    datasource.get_details = mocker.Mock(return_value=datasource_details_result)

    agg = Aggregation(period="month", company=None)

    dm.get_data(datasource, company, agg, start_date=datetime.date(2000, 1, 1))

    url = urlparse(dm.client.get.call_args[0][0])
    query = parse_qs(url.query)

    assert url.path == "/rest/v1/datasource/{}/rawdata".format(datasource.id)
    assert len(query) == 3
    assert query["aggregation"] == ["month"]
    assert query["section_pk"] == [company.id]
    assert query["period_start__gte"] == ["2000-01-01"]

    # ** start and end date
    dm.client.get = mocker.Mock(return_value=avro_data_file)

    dm.get_data(
        datasource,
        company,
        start_date=datetime.date(2000, 1, 1),
        end_date=datetime.date(2001, 1, 1),
    )

    url = urlparse(dm.client.get.call_args[0][0])
    query = parse_qs(url.query)

    assert url.path == "/rest/v1/datasource/{}/rawdata".format(datasource.id)
    assert len(query) == 3
    assert query["section_pk"] == [company.id]
    assert query["period_start__gte"] == ["2000-01-01"]
    assert query["period_end__lt"] == ["2001-01-01"]
