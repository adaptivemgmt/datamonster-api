import datetime
import pytest

from six.moves.urllib.parse import urlparse, parse_qs

from datamonster_api import Aggregation, DataMonsterError


##############################################
#           Company Tests
##############################################
def _assert_object_matches_company(company, company_obj):
    assert company_obj['id'] == company.id
    assert company_obj['name'] == company.name
    assert company_obj['ticker'] == company.ticker
    assert company_obj['uri'] == company.uri


def test_get_companies_1(mocker, dm, single_page_company_results, datasource):
    """Test getting companies. single page. various filters"""

    # The resulting companies should always be the same
    def assert_results_good(results):
        results = list(results)
        assert len(results) == 2

        _assert_object_matches_company(results[0], single_page_company_results['results'][0])
        _assert_object_matches_company(results[1], single_page_company_results['results'][1])

    dm.client.get = mocker.Mock(return_value=single_page_company_results)
    # No queries, no datasource
    dm.client.get.reset_mock()
    companies = dm.get_companies()

    assert_results_good(companies)
    assert dm.client.get.call_count == 1
    assert dm.client.get.call_args[0][0] == '/rest/v1/company'

    # text query, no datasource
    dm.client.get.reset_mock()
    companies = dm.get_companies(query='abc')

    assert_results_good(companies)
    assert dm.client.get.call_count == 1
    assert dm.client.get.call_args[0][0] == '/rest/v1/company?q=abc'

    # no text query, datasource
    dm.client.get.reset_mock()
    companies = dm.get_companies(datasource=datasource)

    assert_results_good(companies)
    assert dm.client.get.call_count == 1
    assert dm.client.get.call_args[0][0] == '/rest/v1/company?datasourceId={}'.format(datasource.id)

    # text query, datasource
    dm.client.get.reset_mock()
    companies = dm.get_companies(query='abc', datasource=datasource)

    assert_results_good(companies)
    assert dm.client.get.call_count == 1
    assert dm.client.get.call_args[0][0] == '/rest/v1/company?q=abc&datasourceId={}'.format(datasource.id)


def test_get_companies_2(mocker, dm, multi_page_company_results):
    """Test getting companies -- multiple pages"""

    dm.client.get = mocker.Mock(side_effect=multi_page_company_results)
    companies = dm.get_companies()
    companies = list(companies)

    assert dm.client.get.call_count == 2

    assert dm.client.get.call_args_list[0][0][0] == '/rest/v1/company'
    assert dm.client.get.call_args_list[1][0][0] == multi_page_company_results[0]['pagination']['nextPageURI']
    assert len(companies) == 4
    _assert_object_matches_company(companies[0], multi_page_company_results[0]['results'][0])
    _assert_object_matches_company(companies[1], multi_page_company_results[0]['results'][1])
    _assert_object_matches_company(companies[2], multi_page_company_results[1]['results'][0])
    _assert_object_matches_company(companies[3], multi_page_company_results[1]['results'][1])


def test_get_companies_3(dm):
    """Test getting companies. error states"""

    # Bad datasource
    with pytest.raises(DataMonsterError) as excinfo:
        dm.get_companies(datasource='abc')

    assert 'datasource argument must be a Datasource object' in excinfo.value.args[0]


def test_get_companies_by_ticker_1(mocker, dm, multi_page_company_results):
    """Test getting company by ticker"""

    # matches case
    dm.client.get = mocker.Mock(side_effect=multi_page_company_results)
    company = dm.get_company_by_ticker('c3')
    _assert_object_matches_company(company, multi_page_company_results[1]['results'][0])

    # does not match case
    dm.client.get = mocker.Mock(side_effect=multi_page_company_results)
    company = dm.get_company_by_ticker('C3')
    _assert_object_matches_company(company, multi_page_company_results[1]['results'][0])


def test_get_companies_by_ticker_2(mocker, dm, multi_page_company_results):
    """Test getting company by ticker -- company doesn't exist"""

    # Bad datasource
    with pytest.raises(DataMonsterError) as excinfo:
        dm.client.get = mocker.Mock(side_effect=multi_page_company_results)
        dm.get_company_by_ticker('c5')

    assert 'Could not find company with ticker' in excinfo.value.args[0]


##############################################
#           Datasource Tests
##############################################
def _assert_object_matches_datasource(datasource, datasource_obj):
    assert datasource_obj['id'] == datasource.id
    assert datasource_obj['name'] == datasource.name
    assert datasource_obj['category'] == datasource.category
    assert datasource_obj['uri'] == datasource.uri


def test_get_datasources_1(mocker, dm, single_page_datasource_results, company):
    """Test getting datasources. multiple pages. various filters"""

    # The resulting datasources should always be the same
    def assert_results_good(results):
        results = list(results)
        assert len(results) == 2

        _assert_object_matches_datasource(
            results[0],
            single_page_datasource_results['results'][0])

        _assert_object_matches_datasource(
            results[1],
            single_page_datasource_results['results'][1])

    dm.client.get = mocker.Mock(return_value=single_page_datasource_results)

    # ++ No queries, no company
    dm.client.get.reset_mock()
    datasources = dm.get_datasources()

    assert_results_good(datasources)
    assert dm.client.get.call_count == 1
    assert dm.client.get.call_args[0][0] == '/rest/v1/datasource'

    # ++ text query, no company
    dm.client.get.reset_mock()
    datasources = dm.get_datasources(query='abc')

    assert_results_good(datasources)
    assert dm.client.get.call_count == 1
    assert dm.client.get.call_args[0][0] == '/rest/v1/datasource?q=abc'

    # ++ no text query, company
    dm.client.get.reset_mock()
    datasources = dm.get_datasources(company=company)

    assert_results_good(datasources)
    assert dm.client.get.call_count == 1
    assert dm.client.get.call_args[0][0] == '/rest/v1/datasource?companyId={}'.format(company.id)

    # ++ text query, company
    dm.client.get.reset_mock()
    datasources = dm.get_datasources(query='abc', company=company)

    assert_results_good(datasources)
    assert dm.client.get.call_count == 1
    assert dm.client.get.call_args[0][0] == '/rest/v1/datasource?q=abc&companyId={}'.format(company.id)


def test_get_datasources_2(mocker, dm, single_page_datasource_results, company):
    """Test getting datasources. error states"""

    # Bad datasource
    with pytest.raises(DataMonsterError) as excinfo:
        dm.get_datasources(company='abc')

    assert 'company argument must be a Company object' in excinfo.value.args[0]


def test_get_datasource_by_id(mocker, dm, datasource_details_result):
    """Test getting datasource by uuid"""

    dm.client.get = mocker.Mock(return_value=datasource_details_result)

    datasource = dm.get_datasource_by_id('abc')

    # Make sure we hit the right endpoint
    assert dm.client.get.call_count == 1
    assert dm.client.get.call_args[0][0] == '/rest/v1/datasource/abc'

    # a couple of spot checks.
    assert datasource.category == 'category'
    assert datasource.cadence == 'daily'

    # Make sure we didn't go through the client again for the details
    assert dm.client.get.call_count == 1


def test_get_data_1(mocker, dm, avro_data_file, company, datasource):
    """Test getting data -- happy case"""

    dm.client.get = mocker.Mock(return_value=avro_data_file)

    df = dm.get_data(datasource, company)

    # Check that we called the client correctly
    expected_path = '/rest/v1/datasource/{}/data?companyId={}'.format(
        datasource.id,
        company.id
    )
    assert dm.client.get.call_count == 1
    assert dm.client.get.call_args[0][0] == expected_path
    assert dm.client.get.call_args[0][1] == {'Accept': 'avro/binary'}

    # Check the columns
    assert len(df.columns) == 4
    assert 'dimensions' in df.columns
    assert 'start_date' in df.columns
    assert 'time_span' in df.columns
    assert 'value' in df.columns

    # size sanity check
    assert len(df) == 12

    # Check the first row
    assert df.iloc[0]['dimensions'] == {'category': u''}
    assert df.iloc[0]['value'] == 0.318149
    assert df.iloc[0]['start_date'].date() == datetime.date(2018, 1, 5)
    assert df.iloc[0]['time_span'].to_pytimedelta() == datetime.timedelta(days=1)

    # Check the last row
    assert df.iloc[11]['dimensions'] == {'category': u'Acquisition Adjusted'}
    assert df.iloc[11]['value'] == 0.383680
    assert df.iloc[11]['start_date'].date() == datetime.date(2018, 1, 10)
    assert df.iloc[11]['time_span'].to_pytimedelta() == datetime.timedelta(days=1)


def test_get_data_2(mocker, dm, avro_data_file, company, other_company, datasource):
    """Test getting data -- bad aggregations"""

    # ** aggregation period is invalid
    agg = Aggregation(period=123, company=None)
    with pytest.raises(DataMonsterError) as excinfo:
        dm.get_data(datasource, company, agg)

    assert 'Bad Aggregation Period' in excinfo.value.args[0]

    # ** aggregation company is invalid
    agg = Aggregation(period='month', company=123)
    with pytest.raises(DataMonsterError) as excinfo:
        dm.get_data(datasource, company, agg)

    assert 'Aggregation company must be Company' in excinfo.value.args[0]

    # ** fiscal quarter aggregation -- no company
    agg = Aggregation(period='fiscalQuarter', company=None)
    with pytest.raises(DataMonsterError) as excinfo:
        dm.get_data(datasource, company, agg)

    assert 'Company must be specified for a fiscalQuarter aggregation' in excinfo.value.args[0]

    # ** fiscal quarter aggregation -- different company
    agg = Aggregation(period='fiscalQuarter', company=other_company)
    with pytest.raises(DataMonsterError) as excinfo:
        dm.get_data(datasource, company, agg)

    assert 'Aggregating by the fiscal quarter of a different company not yet supported' in excinfo.value.args[0]


def test_get_data_3(mocker, dm, avro_data_file, company, other_company, datasource):
    """Test getting data -- good aggregations"""

    # ** monthly aggregation
    dm.client.get = mocker.Mock(return_value=avro_data_file)
    agg = Aggregation(period='month', company=None)

    dm.get_data(datasource, company, agg)

    url = urlparse(dm.client.get.call_args[0][0])
    query = parse_qs(url.query)

    assert url.path == '/rest/v1/datasource/{}/data'.format(datasource.id)
    assert len(query) == 2
    assert query['aggregation'] == ['month']
    assert query['companyId'] == [company.id]

    # ** fiscal quarter aggregation -- good company
    dm.client.get = mocker.Mock(return_value=avro_data_file)
    agg = Aggregation(period='fiscalQuarter', company=company)

    dm.get_data(datasource, company, agg)

    url = urlparse(dm.client.get.call_args[0][0])
    query = parse_qs(url.query)

    assert url.path == '/rest/v1/datasource/{}/data'.format(datasource.id)
    assert len(query) == 2
    assert query['aggregation'] == ['fiscalQuarter']
    assert query['companyId'] == [company.id]


def test_get_data_4(mocker, dm, avro_data_file, company, other_company, datasource):
    """Test getting data -- date filters"""

    # ** monthly aggregation, start date
    dm.client.get = mocker.Mock(return_value=avro_data_file)
    agg = Aggregation(period='month', company=None)

    dm.get_data(datasource, company, agg, start_date=datetime.date(2000, 1, 1))

    url = urlparse(dm.client.get.call_args[0][0])
    query = parse_qs(url.query)

    assert url.path == '/rest/v1/datasource/{}/data'.format(datasource.id)
    assert len(query) == 3
    assert query['aggregation'] == ['month']
    assert query['companyId'] == [company.id]
    assert query['startDate'] == ['2000-01-01']

    # ** start and end date
    dm.client.get = mocker.Mock(return_value=avro_data_file)

    dm.get_data(datasource, company, start_date=datetime.date(2000, 1, 1), end_date=datetime.date(2001, 1, 1))

    url = urlparse(dm.client.get.call_args[0][0])
    query = parse_qs(url.query)

    assert url.path == '/rest/v1/datasource/{}/data'.format(datasource.id)
    assert len(query) == 3
    assert query['companyId'] == [company.id]
    assert query['startDate'] == ['2000-01-01']
    assert query['endDate'] == ['2001-01-01']

##############################################
#      Tests for "get_splits*" methods
##############################################

def test_summarize_splits_dict_no_results_key(splits):
    pass    # TODO

def test_summarize_splits_dict_some_result_has_no_split_combinations_key(splits):
    pass    # TODO

def test_summarize_splits_dict_normal(splits):
    pass    # TODO


def test_check_filters_param_huge_filters():
    """
    See if we can "run out of RAM" with a huge `filters`. Try to get json.loads(filters)
    to fail because filters is too big.
    Some `huge_filters`, maybe a fixture - a generated, trivial but enormous,
    structurally valid `filters` dict (valid as a `filters` parameter to `FilterSet()` ctor).

    The exact definition of "valid `filters` dict" is unspecified, at least DM-side.
    Just "definition by ostentation": it is what it does, and no more, with no guarantees
    about what it is or might be tomorrow.
    """

    pass    # TODO -- see docstring


# Todo: can we test this?
#
def test_dm_get_splits_for_datasource_not_a_data_fountain(mocker, dm, datasource, filters):
    pass    # TODO

def test_dm_get_splits_for_datasource_bad_filters(mocker, dm, datasource, filters):
    pass    # TODO

def test_dm_get_splits_for_datasource_filters_is_none(mocker, dm, datasource):
    pass    # TODO

def test_dm_get_splits_for_datasource_nonempty_filters(mocker, dm, datasource, filters):
    pass    # TODO


def test_ds_get_splits_bad_company(mocker, dm, datasource, company, **kwargs):
    """
    :param company: a `Company`, an `Iterable` of `Company`s, or None
    """
    pass    # TODO

def test_ds_get_splits_bad_kwarg(mocker, dm, datasource, company, **kwargs):
    """
    :param company: a `Company`, an `Iterable` of `Company`s, or None
    """
    pass    # TODO

def test_ds_get_splits_no_filtering(mocker, dm, datasource):
    pass    # TODO


def test_ds_get_splits_filtering(mocker, dm, datasource, company, **kwargs):
    pass    # TODO


def test_dm_get_splits_for_datasource_company_no_kwargs(mocker, dm, datasource, company):
    """
    :param company: a `Company`, an `Iterable` of `Company`s, or None
    """
    pass    # TODO

def test_dm_get_splits_for_datasource_company_kwargs(mocker, dm, datasource, company, **kwargs):
    """
    :param company: a `Company`, an `Iterable` of `Company`s, or None
    """
    pass    # TODO

# TODO? Maybe don't need to test the "_summary" methods (anyway, not much) if the basic `get_splits`
# |     method and `summarize_splits_dict` are tested.
