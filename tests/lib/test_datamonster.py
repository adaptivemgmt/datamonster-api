import datetime
import pytest

from six.moves.urllib.parse import urlparse, parse_qs

from lib.errors import DataMonsterError
from lib.aggregation import Aggregation


##############################################
#           Company Tests
##############################################
def _assert_object_matches_company(company, company_obj):
    assert company_obj['id'] == company._id
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

    assert dm.client.get.call_count == 1
    assert dm.client.get.call_args[0][0] == '/rest/company'
    assert_results_good(companies)

    # text query, no datasource
    dm.client.get.reset_mock()
    companies = dm.get_companies(query='abc')

    assert dm.client.get.call_count == 1
    assert dm.client.get.call_args[0][0] == '/rest/company?q=abc'
    assert_results_good(companies)

    # no text query, datasource
    dm.client.get.reset_mock()
    companies = dm.get_companies(datasource=datasource)

    assert dm.client.get.call_count == 1
    assert dm.client.get.call_args[0][0] == '/rest/company?datasourceId={}'.format(datasource._id)
    assert_results_good(companies)

    # text query, datasource
    dm.client.get.reset_mock()
    companies = dm.get_companies(query='abc', datasource=datasource)

    assert dm.client.get.call_count == 1
    assert dm.client.get.call_args[0][0] == '/rest/company?q=abc&datasourceId={}'.format(datasource._id)
    assert_results_good(companies)


def test_get_companies_2(mocker, dm, multi_page_company_results):
    """Test getting companies -- multiple pages"""

    dm.client.get = mocker.Mock(side_effect=multi_page_company_results)
    companies = dm.get_companies()
    companies = list(companies)

    assert dm.client.get.call_count == 2

    assert dm.client.get.call_args_list[0][0][0] == '/rest/company'
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
    assert datasource_obj['id'] == datasource._id
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

    assert dm.client.get.call_count == 1
    assert dm.client.get.call_args[0][0] == '/rest/datasource'
    assert_results_good(datasources)

    # ++ text query, no company
    dm.client.get.reset_mock()
    datasources = dm.get_datasources(query='abc')

    assert dm.client.get.call_count == 1
    assert dm.client.get.call_args[0][0] == '/rest/datasource?q=abc'
    assert_results_good(datasources)

    # ++ no text query, company
    dm.client.get.reset_mock()
    datasources = dm.get_datasources(company=company)

    assert dm.client.get.call_count == 1
    assert dm.client.get.call_args[0][0] == '/rest/datasource?companyId={}'.format(company._id)
    assert_results_good(datasources)

    # ++ text query, company
    dm.client.get.reset_mock()
    datasources = dm.get_datasources(query='abc', company=company)

    assert dm.client.get.call_count == 1
    assert dm.client.get.call_args[0][0] == '/rest/datasource?q=abc&companyId={}'.format(company._id)
    assert_results_good(datasources)


def test_get_datasources_2(mocker, dm, single_page_datasource_results, company):
    """Test getting datasources. error states"""

    # Bad datasource
    with pytest.raises(DataMonsterError) as excinfo:
        dm.get_datasources(company='abc')

    assert 'company argument must be a Company object' in excinfo.value.args[0]


def test_get_data_1(mocker, dm, avro_data_file, company, datasource):
    """Test getting data -- happy case"""

    dm.client.get = mocker.Mock(return_value=avro_data_file)

    df = dm.get_data(datasource, company)

    # Check that we called the client correctly
    expected_path = '/rest/datasource/{}/data?companyId={}'.format(
        datasource._id,
        company._id
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

    assert url.path == '/rest/datasource/{}/data'.format(datasource._id)
    assert len(query) == 2
    assert query['aggregation'] == ['month']
    assert query['companyId'] == [company._id]

    # ** fiscal quarter aggregation -- good company
    dm.client.get = mocker.Mock(return_value=avro_data_file)
    agg = Aggregation(period='fiscalQuarter', company=company)

    dm.get_data(datasource, company, agg)

    url = urlparse(dm.client.get.call_args[0][0])
    query = parse_qs(url.query)

    assert url.path == '/rest/datasource/{}/data'.format(datasource._id)
    assert len(query) == 2
    assert query['aggregation'] == ['fiscalQuarter']
    assert query['companyId'] == [company._id]


def test_get_data_4(mocker, dm, avro_data_file, company, other_company, datasource):
    """Test getting data -- date filters"""

    # ** monthly aggregation, start date
    dm.client.get = mocker.Mock(return_value=avro_data_file)
    agg = Aggregation(period='month', company=None)

    dm.get_data(datasource, company, agg, start_date=datetime.date(2000, 1, 1))

    url = urlparse(dm.client.get.call_args[0][0])
    query = parse_qs(url.query)

    assert url.path == '/rest/datasource/{}/data'.format(datasource._id)
    assert len(query) == 3
    assert query['aggregation'] == ['month']
    assert query['companyId'] == [company._id]
    assert query['startDate'] == ['2000-01-01']

    # ** start and end date
    dm.client.get = mocker.Mock(return_value=avro_data_file)

    dm.get_data(datasource, company, start_date=datetime.date(2000, 1, 1), end_date=datetime.date(2001, 1, 1))

    url = urlparse(dm.client.get.call_args[0][0])
    query = parse_qs(url.query)

    assert url.path == '/rest/datasource/{}/data'.format(datasource._id)
    assert len(query) == 3
    assert query['companyId'] == [company._id]
    assert query['startDate'] == ['2000-01-01']
    assert query['endDate'] == ['2001-01-01']