import pytest

from lib.errors import DataMonsterError


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
        assert len(companies) == 2

        _assert_object_matches_company(companies[0], single_page_company_results['results'][0])
        _assert_object_matches_company(companies[1], single_page_company_results['results'][1])

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
    assert dm.client.get.call_args[0][0] == '/rest/company?datasource={}'.format(datasource._id)
    assert_results_good(companies)

    # text query, datasource
    dm.client.get.reset_mock()
    companies = dm.get_companies(query='abc', datasource=datasource)

    assert dm.client.get.call_count == 1
    assert dm.client.get.call_args[0][0] == '/rest/company?q=abc&datasource={}'.format(datasource._id)
    assert_results_good(companies)


def test_get_companies_2(mocker, dm, multi_page_company_results):
    """Test getting companies -- multiple pages"""

    dm.client.get = mocker.Mock(side_effect=multi_page_company_results)
    companies = dm.get_companies()

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

    dm.client.get = mocker.Mock(side_effect=multi_page_company_results)
    company = dm.get_company_by_ticker('c3')

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


def test_get_data(mocker, dm, avro_data_file, company, datasource):
    """Test getting data"""

    dm.client.get = mocker.Mock(return_value=avro_data_file)

    dm.get_data(company, datasource)
