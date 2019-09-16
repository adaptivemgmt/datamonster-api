import datetime
import pytest

from six.moves.urllib.parse import urlparse, parse_qs

from datamonster_api import Aggregation, DataMonsterError, Datasource


##############################################
#           Company Tests
##############################################
def _assert_object_matches_company(company, company_obj):
    assert company_obj["id"] == company.id
    assert company_obj["name"] == company.name
    assert company_obj["ticker"] == company.ticker
    assert company_obj["uri"] == company.uri


def test_get_companies_1(mocker, dm, single_page_company_results, datasource):

    """Test getting companies. single page. various filters"""

    # The resulting companies should always be the same
    def assert_results_good(results):
        results = list(results)
        assert len(results) == 2

        _assert_object_matches_company(
            results[0], single_page_company_results["results"][0]
        )
        _assert_object_matches_company(
            results[1], single_page_company_results["results"][1]
        )

    dm.client.get = mocker.Mock(return_value=single_page_company_results)
    # No queries, no datasource
    dm.client.get.reset_mock()
    companies = dm.get_companies()

    assert_results_good(companies)
    assert dm.client.get.call_count == 1
    assert dm.client.get.call_args[0][0] == "/rest/v1/company"

    # text query, no datasource
    dm.client.get.reset_mock()
    companies = dm.get_companies(query="abc")

    assert_results_good(companies)
    assert dm.client.get.call_count == 1
    assert dm.client.get.call_args[0][0] == "/rest/v1/company?q=abc"

    # no text query, datasource
    dm.client.get.reset_mock()
    companies = dm.get_companies(datasource=datasource)

    assert_results_good(companies)
    assert dm.client.get.call_count == 1
    assert dm.client.get.call_args[0][0] == "/rest/v1/company?datasourceId={}".format(
        datasource.id
    )

    # text query, datasource
    dm.client.get.reset_mock()
    companies = dm.get_companies(query="abc", datasource=datasource)

    assert_results_good(companies)
    assert dm.client.get.call_count == 1
    assert dm.client.get.call_args[0][
        0
    ] == "/rest/v1/company?q=abc&datasourceId={}".format(datasource.id)


def test_get_companies_2(mocker, dm, multi_page_company_results):
    """Test getting companies -- multiple pages"""

    dm.client.get = mocker.Mock(side_effect=multi_page_company_results)
    companies = dm.get_companies()
    companies = list(companies)

    assert dm.client.get.call_count == 2

    assert dm.client.get.call_args_list[0][0][0] == "/rest/v1/company"
    assert (
        dm.client.get.call_args_list[1][0][0]
        == multi_page_company_results[0]["pagination"]["nextPageURI"]
    )
    assert len(companies) == 4
    _assert_object_matches_company(
        companies[0], multi_page_company_results[0]["results"][0]
    )
    _assert_object_matches_company(
        companies[1], multi_page_company_results[0]["results"][1]
    )
    _assert_object_matches_company(
        companies[2], multi_page_company_results[1]["results"][0]
    )
    _assert_object_matches_company(
        companies[3], multi_page_company_results[1]["results"][1]
    )


def test_get_companies_3(dm):
    """Test getting companies. error states"""

    # Bad datasource
    with pytest.raises(DataMonsterError) as excinfo:
        dm.get_companies(datasource="abc")

    assert "datasource argument must be a Datasource object" in excinfo.value.args[0]


def test_get_companies_by_ticker_1(mocker, dm, multi_page_company_results):
    """Test getting company by ticker"""

    # matches case
    dm.client.get = mocker.Mock(side_effect=multi_page_company_results)
    company = dm.get_company_by_ticker("c3")
    _assert_object_matches_company(company, multi_page_company_results[1]["results"][0])

    # does not match case
    dm.client.get = mocker.Mock(side_effect=multi_page_company_results)
    company = dm.get_company_by_ticker("C3")
    _assert_object_matches_company(company, multi_page_company_results[1]["results"][0])


def test_get_companies_by_ticker_2(mocker, dm, multi_page_company_results):
    """Test getting company by ticker -- company doesn't exist"""

    # Bad datasource
    with pytest.raises(DataMonsterError) as excinfo:
        dm.client.get = mocker.Mock(side_effect=multi_page_company_results)
        dm.get_company_by_ticker("c5")

    assert "Could not find company with ticker" in excinfo.value.args[0]


##############################################
#           Datasource Tests
##############################################
def _assert_object_matches_datasource(datasource, datasource_obj):
    assert datasource_obj["id"] == datasource.id
    assert datasource_obj["name"] == datasource.name
    assert datasource_obj["category"] == datasource.category
    assert datasource_obj["uri"] == datasource.uri


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

    # Make sure we didn't go through the client again for the details
    assert dm.client.get.call_count == 1


def test_get_data_1(mocker, dm, avro_data_file, company, datasource):
    """Test getting data -- happy case"""

    dm.client.get = mocker.Mock(return_value=avro_data_file)
    datasource.get_details = mocker.Mock(return_value={"splitColumns": ["category"]})

    df = dm.get_data(datasource, company)

    # Check that we called the client correctly
    expected_path = "/rest/v1/datasource/{}/rawdata?companyId={}".format(
        datasource.id, company.id
    )
    assert dm.client.get.call_count == 1
    assert dm.client.get.call_args[0][0] == expected_path
    assert dm.client.get.call_args[0][1] == {"Accept": "avro/binary"}

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
    assert len(df) == 8

    # Check the first row
    assert df.iloc[0]["dimensions"] == {"category": "Whole Foods"}
    assert df.iloc[0]["value"] == 52.6278787878788
    assert df.iloc[0]["start_date"].date() == datetime.date(2019, 1, 1)
    assert df.iloc[0]["time_span"].to_pytimedelta() == datetime.timedelta(days=1)
    assert df.iloc[0]["end_date"].date() == datetime.date(2019, 1, 1)

    # Check the last row
    assert df.iloc[7]["dimensions"] == {"category": "Amazon Acquisition Adjusted"}
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


def test_get_data_3(mocker, dm, avro_data_file, company, other_company, datasource):
    """Test getting data -- good aggregations"""

    # ** monthly aggregation
    dm.client.get = mocker.Mock(return_value=avro_data_file)
    datasource.get_details = mocker.Mock(return_value={"splitColumns": ["category"]})
    agg = Aggregation(period="month", company=None)

    dm.get_data(datasource, company, agg)

    url = urlparse(dm.client.get.call_args[0][0])
    query = parse_qs(url.query)

    assert url.path == "/rest/v1/datasource/{}/rawdata".format(datasource.id)
    assert len(query) == 2
    assert query["aggregation"] == ["month"]
    assert query["companyId"] == [company.id]

    # ** fiscal quarter aggregation -- good company
    dm.client.get = mocker.Mock(return_value=avro_data_file)
    agg = Aggregation(period="fiscalQuarter", company=company)

    dm.get_data(datasource, company, agg)

    url = urlparse(dm.client.get.call_args[0][0])
    query = parse_qs(url.query)

    assert url.path == "/rest/v1/datasource/{}/rawdata".format(datasource.id)
    assert len(query) == 2
    assert query["aggregation"] == ["fiscalQuarter"]
    assert query["companyId"] == [company.id]


def test_get_data_4(mocker, dm, avro_data_file, company, other_company, datasource):
    """Test getting data -- date filters"""

    # ** monthly aggregation, start date
    dm.client.get = mocker.Mock(return_value=avro_data_file)
    datasource.get_details = mocker.Mock(return_value={"splitColumns": ["category"]})
    agg = Aggregation(period="month", company=None)

    dm.get_data(datasource, company, agg, start_date=datetime.date(2000, 1, 1))

    url = urlparse(dm.client.get.call_args[0][0])
    query = parse_qs(url.query)

    assert url.path == "/rest/v1/datasource/{}/rawdata".format(datasource.id)
    assert len(query) == 3
    assert query["aggregation"] == ["month"]
    assert query["companyId"] == [company.id]
    assert query["startDate"] == ["2000-01-01"]

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
    assert query["companyId"] == [company.id]
    assert query["startDate"] == ["2000-01-01"]
    assert query["endDate"] == ["2001-01-01"]


##############################################
#      Tests for "get_dimensions*" methods
##############################################


class __NoCanSerialize(object):
    def __repr__(self):
        return "Object of type __NoCanSerialize"


def _assert_equal_dimension_dicts(expected, mocked):
    assert expected["split_combination"] == mocked["splitCombination"]
    assert expected["max_date"] == mocked["maxDate"]
    assert expected["min_date"] == mocked["minDate"]
    assert expected["row_count"] == mocked["rowCount"]


def _assert_dict_and_DimensionSet_metadata_match(resp_dict, dim_set):
    assert resp_dict["dimensionCount"] == len(dim_set)
    assert resp_dict["maxDate"] == dim_set.max_date
    assert resp_dict["minDate"] == dim_set.min_date
    assert resp_dict["rowCount"] == dim_set.row_count


# ------------------------------------------------------
# Tests for `DataMonster.get_dimensions_for_datasource`
# ------------------------------------------------------


def test_filters_param_not_dict(dm, datasource):
    """
    """
    with pytest.raises(DataMonsterError) as excinfo:
        dm.get_dimensions_for_datasource(datasource, filters=[0, 1, 2])

    assert "`filters` must be a dict, got list instead" in excinfo.value.args


def test_filters_param_not_json_serializable(dm, datasource):
    """
    """
    with pytest.raises(DataMonsterError) as excinfo:
        dm.get_dimensions_for_datasource(
            datasource, filters={"somekey": __NoCanSerialize()}
        )

    errtext = "Problem with filters when getting dimensions: Object of type __NoCanSerialize is not JSON serializable"
    assert errtext in excinfo.value.args


def test_get_dimensions_for_datasource_single_page(
    mocker, dm, single_page_dimensions_result, datasource
):
    """Test getting dimensions. single page"""

    # The resulting dimensions should always be the same
    def assert_results_good(dimensions):
        dimensions = list(dimensions)
        assert len(dimensions) == 3

        _assert_equal_dimension_dicts(
            dimensions[0], single_page_dimensions_result["results"][0]
        )
        _assert_equal_dimension_dicts(
            dimensions[1], single_page_dimensions_result["results"][1]
        )
        _assert_equal_dimension_dicts(
            dimensions[2], single_page_dimensions_result["results"][2]
        )

    # No filters
    dm.client.get = mocker.Mock(return_value=single_page_dimensions_result)
    dm.client.get.reset_mock()
    dimensions = dm.get_dimensions_for_datasource(datasource)

    _assert_dict_and_DimensionSet_metadata_match(
        single_page_dimensions_result, dimensions
    )
    assert_results_good(dimensions)

    assert dm.client.get.call_count == 1
    assert dm.client.get.call_args[0][0] == "/rest/v1/datasource/{}/dimensions".format(
        datasource.id
    )


def test_get_dimensions_for_datasource_multi_page(
    mocker, dm, multi_page_dimensions_results, datasource
):
    """Test getting dimensions. multi-page"""

    dm.client.get = mocker.Mock(side_effect=multi_page_dimensions_results)
    dimensions = dm.get_dimensions_for_datasource(datasource)
    _assert_dict_and_DimensionSet_metadata_match(
        multi_page_dimensions_results[0], dimensions
    )
    _assert_dict_and_DimensionSet_metadata_match(
        multi_page_dimensions_results[1], dimensions
    )

    dimensions = list(dimensions)

    assert dm.client.get.call_count == 2

    assert dm.client.get.call_args_list[0][0][
        0
    ] == "/rest/v1/datasource/{}/dimensions".format(datasource.id)
    assert (
        dm.client.get.call_args_list[1][0][0]
        == multi_page_dimensions_results[0]["pagination"]["nextPageURI"]
    )

    _assert_equal_dimension_dicts(
        dimensions[0], multi_page_dimensions_results[0]["results"][0]
    )
    _assert_equal_dimension_dicts(
        dimensions[1], multi_page_dimensions_results[0]["results"][1]
    )
    _assert_equal_dimension_dicts(
        dimensions[2], multi_page_dimensions_results[1]["results"][0]
    )


# ------------------------------------------------------
# Tests for `Datasource.get_dimensions`
# ------------------------------------------------------


def test_ds_get_dimensions_bad_company(datasource):
    """
    `company` arg neither a `Company`, nor an `Iterable` of `Company`s, nor None
    """
    with pytest.raises(DataMonsterError) as excinfo:
        datasource.get_dimensions(company="string pertaining to a company")

    assert (
        "company argument must be a `Company`, or a list or tuple of `Company`s"
        in excinfo.value.args
    )


def test_ds_get_dimensions_non_company_in_list(
    datasource, company_with_int_id, other_company_with_int_id
):
    """
    `company`an `Iterable` with an element that's not a `Company`
    """
    with pytest.raises(DataMonsterError) as excinfo:
        datasource.get_dimensions(
            company=[company_with_int_id, other_company_with_int_id, (1, 2, 3)]
        )

    assert (
        "Every item in `company` argument must be a `Company`; (1, 2, 3) is not"
        in excinfo.value.args
    )


def test_ds_get_dimensions_bad_kwarg(mocker, dm, company_with_int_id):
    """
    :param company: a `Company`, an `Iterable` of `Company`s, or None
    """
    new_datasource = Datasource("id", "name", "category", "uri", dm)

    with pytest.raises(DataMonsterError) as excinfo:
        new_datasource.get_dimensions(
            company=company_with_int_id, widget=__NoCanSerialize()
        )

    errtext = "Problem with filters when getting dimensions: Object of type __NoCanSerialize is not JSON serializable"
    assert errtext in excinfo.value.args
