import datetime
import pandas
import pytest

from datamonster_api import Aggregation, DataMonsterError, DataMonster, DataGroupColumn
from test_data_group import assert_object_matches_data_group


def _assert_object_matches_datasource(datasource, datasource_obj):
    assert datasource_obj["id"] == datasource.id
    assert datasource_obj["name"] == datasource.name
    assert datasource_obj["category"] == datasource.category
    assert datasource_obj["uri"] == datasource.uri


def test_equality(datasource, other_datasource):
    assert datasource == datasource
    assert other_datasource != datasource

    d = {}
    d[datasource] = 1
    d[other_datasource] = 2
    assert len(d) == 2
    assert d[datasource] == 1


def test_datamonster_datasource_mapper():
    from pandas.util.testing import assert_frame_equal

    df = pandas.DataFrame.from_records([])
    print(assert_frame_equal(DataMonster._datamonster_data_mapper({}, {}, df), df))
    assert_frame_equal(DataMonster._datamonster_data_mapper({}, {}, df), df)

    df = pandas.DataFrame.from_records([{"apple": 1, "banana": 2, "cherry": 3}])
    with pytest.raises(DataMonsterError):
        DataMonster._datamonster_data_mapper({"garbage": "g"}, {"split": "apple"}, df)

    with pytest.raises(DataMonsterError):
        DataMonster._datamonster_data_mapper(
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


def test_get_data_raw_1(mocker, dm, avro_data_file, company, datasource, datasource_details_result):
    """Test getting raw data -- calendar quarterly aggregation"""

    datasource.get_details = mocker.Mock(return_value=datasource_details_result)
    dm.client.post = mocker.Mock(return_value=avro_data_file)

    filters = {
        'category': ['Apple iTunes'],
        'country': ['US'],
        'section_pk': [company.id]
    }

    # Expected values for network calls
    expected_path = "/rest/v2/datasource/{}/rawdata".format(datasource.id)
    expected_post_data = {
        'timeAggregation': {
            'cadence': 'fiscal quarterly',
            'aggregationType': 'sum',
            'includePTD': False,
            'sectionPk': company.id,
        },
        'valueAggregation': None,
        'filters': filters,
        'forecast': False
    }

    agg = Aggregation(period='fiscalQuarter', company=company)
    schema, df = dm.get_data_raw(datasource, aggregation=agg, filters=filters)

    assert dm.client.post.call_count == 1
    assert dm.client.post.call_args[0][0] == expected_path
    assert dm.client.post.call_args[0][1] == expected_post_data
    assert dm.client.post.call_args[0][2] == {"Accept": "avro/binary", 'Content-Type': 'application/json'}

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


def test_get_data_raw_2(mocker, dm, avro_data_file, company, datasource, datasource_details_result):
    """Test getting raw data -- no aggregation"""

    datasource.get_details = mocker.Mock(return_value=datasource_details_result)
    dm.client.post = mocker.Mock(return_value=avro_data_file)

    filters = {
        'category': ['Apple iTunes'],
        'country': ['US'],
        'section_pk': [company.id]
    }

    # Expected values for network calls
    expected_path = "/rest/v2/datasource/{}/rawdata".format(datasource.id)
    expected_post_data = {
        'timeAggregation': None,
        'valueAggregation': None,
        'filters': filters,
        'forecast': False
    }

    schema, df = dm.get_data_raw(datasource, filters)

    assert dm.client.post.call_count == 1
    assert dm.client.post.call_args[0][0] == expected_path
    assert dm.client.post.call_args[0][1] == expected_post_data
    assert dm.client.post.call_args[0][2] == {"Accept": "avro/binary", 'Content-Type': 'application/json'}

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
    dm.client.post = mocker.Mock(return_value=avro_data_file)

    # Expected values
    expected_path = "/rest/v2/datasource/{}/rawdata".format(datasource.id)
    expected_post_data = {
        'timeAggregation': None,
        'valueAggregation': None,
        'filters': {'section_pk': [int(company.id)]},
        'forecast': False
    }

    df = dm.get_data(datasource, company)

    # Check that we called the client correctly
    assert dm.client.post.call_count == 1
    assert dm.client.post.call_args[0][0] == expected_path
    assert dm.client.post.call_args[0][1] == expected_post_data
    assert dm.client.post.call_args[0][2] == {"Accept": "avro/binary", 'Content-Type': 'application/json'}

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
    """Test getting data -- bad aggregation"""

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
    """Test getting data -- monthly aggregation"""

    # ** monthly aggregation
    dm.client.post = mocker.Mock(return_value=avro_data_file)
    datasource.get_details = mocker.Mock(return_value=datasource_details_result)
    agg = Aggregation(period="month", company=None)

    # Expected values
    expected_path = "/rest/v2/datasource/{}/rawdata".format(datasource.id)
    expected_post_data = {
        'timeAggregation': {
            'cadence': 'monthly',
            'aggregationType': 'sum',
            'includePTD': False
        },
        'valueAggregation': None,
        'filters': {'section_pk': [int(company.id)]},
        'forecast': False
    }

    dm.get_data(datasource, company, agg)
    assert dm.client.post.call_args[0][0] == expected_path
    assert dm.client.post.call_args[0][1] == expected_post_data

    # ** fiscal quarter aggregation -- good company
    dm.client.post = mocker.Mock(return_value=avro_data_file)
    agg = Aggregation(period="fiscalQuarter", company=company)
    expected_post_data = {
        'timeAggregation': {
            'cadence': 'fiscal quarterly',
            'aggregationType': 'sum',
            'includePTD': False,
            'sectionPk': company.id,
        },
        'valueAggregation': None,
        'filters': {'section_pk': [int(company.id)]},
        'forecast': False
    }

    dm.get_data(datasource, company, agg)

    assert dm.client.post.call_args[0][0] == expected_path
    assert dm.client.post.call_args[0][1] == expected_post_data


def test_get_data_4(mocker, dm, other_avro_data_file, company, datasource, datasource_details_result):
    """Test getting data -- date filters"""

    dm.client.post = mocker.Mock(return_value=other_avro_data_file)
    datasource.get_details = mocker.Mock(return_value=datasource_details_result)

    # ** start date
    df = dm.get_data(datasource, company, start_date=datetime.date(2019, 12, 30))

    assert len(df) == 2
    assert df.iloc[0].start_date.date() == datetime.date(2019, 12, 30)
    assert df.iloc[1].start_date.date() == datetime.date(2019, 12, 31)

    # ** end date
    df = dm.get_data(datasource, company, end_date=datetime.date(2014, 1, 2))
    assert len(df) == 2
    assert df.iloc[0].start_date.date() == datetime.date(2014, 1, 1)
    assert df.iloc[1].start_date.date() == datetime.date(2014, 1, 2)

    # ** start and end date

    df = dm.get_data(
        datasource,
        company,
        start_date=datetime.date(2014, 1, 15),
        end_date=datetime.date(2014, 1, 16),
    )
    assert len(df) == 2
    assert df.iloc[0].start_date.date() == datetime.date(2014, 1, 15)
    assert df.iloc[1].start_date.date() == datetime.date(2014, 1, 16)


def test_get_data_group_by_id(mocker, dm, data_group_details_result):
    """Test getting data group by pk"""

    dm.client.get = mocker.Mock(return_value=data_group_details_result)

    data_group = dm.get_data_group_by_id(123)

    # Make sure we hit the right endpoint
    assert dm.client.get.call_count == 1
    assert dm.client.get.call_args[0][0] == '/rest/v1/data_group/123'

    # a couple of spot checks.
    assert data_group.name == 'Test By Id'
    assert data_group.id == 123
    assert len(data_group.columns) == 7
    for c in data_group.columns:
        assert isinstance(c, DataGroupColumn)


def test_get_data_groups(mocker, dm, single_page_data_group_results):
    """Test getting datagroups."""

    def assert_results_good(results):
        results = list(results)
        assert len(results) == 2

        assert_object_matches_data_group(results[0], single_page_data_group_results["results"][0])

        assert_object_matches_data_group(results[1], single_page_data_group_results["results"][1])

    dm.client.get = mocker.Mock(return_value=single_page_data_group_results)

    # ++ No query
    dm.client.get.reset_mock()
    data_groups = dm.get_data_groups()

    assert_results_good(data_groups)
    assert dm.client.get.call_count == 1
    assert dm.client.get.call_args[0][0] == "/rest/v1/data_group"

    # ++ text query
    dm.client.get.reset_mock()
    data_groups = dm.get_data_groups(query="test")

    assert_results_good(data_groups)
    assert dm.client.get.call_count == 1
    assert dm.client.get.call_args[0][0] == "/rest/v1/data_group?q=test"
