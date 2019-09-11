import datetime
import pytest

from six.moves.urllib.parse import urlparse, parse_qs

from datamonster_api import Aggregation, DataMonsterError


def _assert_object_matches_datasource(datasource, datasource_obj):
    assert datasource_obj["id"] == datasource._id
    assert datasource_obj["name"] == datasource.name
    assert datasource_obj["category"] == datasource.category
    assert datasource_obj["uri"] == datasource._uri


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

    dm._client.get = mocker.Mock(return_value=single_page_datasource_results)

    # ++ No queries, no company
    dm._client.get.reset_mock()
    datasources = dm.get_datasources()

    assert_results_good(datasources)
    assert dm._client.get.call_count == 1
    assert dm._client.get.call_args[0][0] == "/rest/v1/datasource"

    # ++ text query, no company
    dm._client.get.reset_mock()
    datasources = dm.get_datasources(query="abc")

    assert_results_good(datasources)
    assert dm._client.get.call_count == 1
    assert dm._client.get.call_args[0][0] == "/rest/v1/datasource?q=abc"

    # ++ no text query, company
    dm._client.get.reset_mock()
    datasources = dm.get_datasources(company=company)

    assert_results_good(datasources)
    assert dm._client.get.call_count == 1
    assert dm._client.get.call_args[0][0] == "/rest/v1/datasource?companyId={}".format(
        company._id
    )

    # ++ text query, company
    dm._client.get.reset_mock()
    datasources = dm.get_datasources(query="abc", company=company)

    assert_results_good(datasources)
    assert dm._client.get.call_count == 1
    assert dm._client.get.call_args[0][
        0
    ] == "/rest/v1/datasource?q=abc&companyId={}".format(company._id)


def test_get_datasources_2(mocker, dm, single_page_datasource_results, company):
    """Test getting datasources. error states"""

    # Bad datasource
    with pytest.raises(DataMonsterError) as excinfo:
        dm.get_datasources(company="abc")

    assert "company argument must be a Company object" in excinfo.value.args[0]


def test_get_datasource_by_id(mocker, dm, datasource_details_result):
    """Test getting datasource by uuid"""

    dm._client.get = mocker.Mock(return_value=datasource_details_result)

    datasource = dm.get_datasource_by_id("abc")

    # Make sure we hit the right endpoint
    assert dm._client.get.call_count == 1
    assert dm._client.get.call_args[0][0] == "/rest/v1/datasource/abc"

    # a couple of spot checks.
    assert datasource.category == "category"
    assert datasource.cadence == "daily"

    # Make sure we didn't go through the client again for the details
    assert dm._client.get.call_count == 1


def test_get_data_1(mocker, dm, avro_data_file, company, datasource):
    """Test getting data -- happy case"""

    dm._client.get = mocker.Mock(return_value=avro_data_file)

    df = dm.get_data(datasource, company)

    # Check that we called the client correctly
    expected_path = "/rest/v1/datasource/{}/data?companyId={}".format(
        datasource._id, company._id
    )
    assert dm._client.get.call_count == 1
    assert dm._client.get.call_args[0][0] == expected_path
    assert dm._client.get.call_args[0][1] == {"Accept": "avro/binary"}

    # Check the columns
    assert len(df.columns) == 5
    assert sorted(df.columns) == [
        "dimensions",
        "end_date",
        "start_date",
        "time_span",
        "value",
    ]
    # size sanity check
    assert len(df) == 12

    # Check the first row
    assert df.iloc[0]["dimensions"] == {"category": u""}
    assert df.iloc[0]["value"] == 0.318149
    assert df.iloc[0]["start_date"].date() == datetime.date(2018, 1, 5)
    assert df.iloc[0]["time_span"].to_pytimedelta() == datetime.timedelta(days=1)
    assert df.iloc[0]["end_date"].date() == datetime.date(2018, 1, 5)

    # Check the last row
    assert df.iloc[11]["dimensions"] == {"category": u"Acquisition Adjusted"}
    assert df.iloc[11]["value"] == 0.383680
    assert df.iloc[11]["start_date"].date() == datetime.date(2018, 1, 10)
    assert df.iloc[11]["time_span"].to_pytimedelta() == datetime.timedelta(days=1)
    assert df.iloc[11]["end_date"].date() == datetime.date(2018, 1, 10)


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


def test_get_data_3(mocker, dm, avro_data_file, company, other_company, datasource):
    """Test getting data -- good aggregations"""

    # ** monthly aggregation
    dm._client.get = mocker.Mock(return_value=avro_data_file)
    agg = Aggregation(period="month", company=None)

    dm.get_data(datasource, company, agg)

    url = urlparse(dm._client.get.call_args[0][0])
    query = parse_qs(url.query)

    assert url.path == "/rest/v1/datasource/{}/data".format(datasource._id)
    assert len(query) == 2
    assert query["aggregation"] == ["month"]
    assert query["companyId"] == [company._id]

    # ** fiscal quarter aggregation -- good company
    dm._client.get = mocker.Mock(return_value=avro_data_file)
    agg = Aggregation(period="fiscalQuarter", company=company)

    dm.get_data(datasource, company, agg)

    url = urlparse(dm._client.get.call_args[0][0])
    query = parse_qs(url.query)

    assert url.path == "/rest/v1/datasource/{}/data".format(datasource._id)
    assert len(query) == 2
    assert query["aggregation"] == ["fiscalQuarter"]
    assert query["companyId"] == [company._id]


def test_get_data_4(mocker, dm, avro_data_file, company, other_company, datasource):
    """Test getting data -- date filters"""

    # ** monthly aggregation, start date
    dm._client.get = mocker.Mock(return_value=avro_data_file)
    agg = Aggregation(period="month", company=None)

    dm.get_data(datasource, company, agg, start_date=datetime.date(2000, 1, 1))

    url = urlparse(dm._client.get.call_args[0][0])
    query = parse_qs(url.query)

    assert url.path == "/rest/v1/datasource/{}/data".format(datasource._id)
    assert len(query) == 3
    assert query["aggregation"] == ["month"]
    assert query["companyId"] == [company._id]
    assert query["startDate"] == ["2000-01-01"]

    # ** start and end date
    dm._client.get = mocker.Mock(return_value=avro_data_file)

    dm.get_data(
        datasource,
        company,
        start_date=datetime.date(2000, 1, 1),
        end_date=datetime.date(2001, 1, 1),
    )

    url = urlparse(dm._client.get.call_args[0][0])
    query = parse_qs(url.query)

    assert url.path == "/rest/v1/datasource/{}/data".format(datasource._id)
    assert len(query) == 3
    assert query["companyId"] == [company._id]
    assert query["startDate"] == ["2000-01-01"]
    assert query["endDate"] == ["2001-01-01"]
