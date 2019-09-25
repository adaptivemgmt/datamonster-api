import pytest

from datamonster_api import DataMonsterError


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
