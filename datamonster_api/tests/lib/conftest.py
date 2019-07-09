import os
import pytest

from datamonster_api import DataMonster, Company, Datasource


@pytest.fixture
def datadir():
    """finds the data directory for a test"""

    script_dir = os.path.dirname(os.path.realpath(__file__))
    return os.path.join(script_dir, 'data')


@pytest.fixture
def dm():
    return DataMonster('key_id', 'secret')


@pytest.fixture
def datasource():
    return Datasource('id', 'name', 'category', 'uri', None)


@pytest.fixture
def company():
    return Company('id', 'ticker', 'name', 'uri', None)


@pytest.fixture
def other_company():
    return Company('other_id', 'other_ticker', 'other_name', 'other_uri', None)


@pytest.fixture
def single_page_company_results():
    return {
        "pagination": {
            "totalResults": 2,
            "pageSize": 100,
            "currentPage": 0,
            "nextPageURI": None,
            "previousPageURI": None,
        },
        "results": [{
            "name": "Company 1",
            "ticker": "c1",
            "id": "1",
            "uri": "https://dm.datamonster.com/rest/v1/company/1"
        }, {
            "name": "Company 2",
            "ticker": "c2",
            "id": "2",
            "uri": "https://dm.datamonster.com/rest/v1/company/2"
        }]
    }


@pytest.fixture
def multi_page_company_results():
    return [{
        "pagination": {
            "totalResults": 4,
            "pageSize": 2,
            "currentPage": 0,
            "nextPageURI": '/rest/v1/company?p=1',
            "previousPageURI": None,
        },
        "results": [{
            "name": "Company 1",
            "ticker": None,
            "id": "1",
            "uri": "https://dm.datamonster.com/rest/v1/company/1"
        }, {
            "name": "Company 2",
            "ticker": "c2",
            "id": "2",
            "uri": "https://dm.datamonster.com/rest/v1/company/2"
        }]
    }, {
        "pagination": {
            "totalResults": 4,
            "pageSize": 2,
            "currentPage": 1,
            "nextPageURI": None,
            "previousPageURI": '/rest/v1/company?p=0',
        },
        "results": [{
            "name": "Company 3",
            "ticker": "c3",
            "id": "3",
            "uri": "https://dm.datamonster.com/rest/v1/company/3"
        }, {
            "name": "Company 4",
            "ticker": "c4",
            "id": "4",
            "uri": "https://dm.datamonster.com/rest/v1/company/4"
        }]
    }]


@pytest.fixture
def single_page_datasource_results():
    return {
        "pagination": {
            "totalResults": 2,
            "pageSize": 100,
            "currentPage": 0,
            "nextPageURI": None,
            "previousPageURI": None,
        },
        "results": [{
            "id": "id 1",
            "name": "name 1",
            "type": "datasource",
            "category": "category",
            "uri": "uri 1",
        }, {
            "id": "id 2",
            "name": "name 2",
            "type": "datasource",
            "category": "category",
            "uri": "uri 2",
        }]
    }


@pytest.fixture
def multi_page_datasource_results():
    return [{
        "pagination": {
            "totalResults": 4,
            "pageSize": 2,
            "currentPage": 0,
            "nextPageURI": "/rest/v1/datasources/?p=1",
            "previousPageURI": None,
        },
        "results": [{
            "id": "id 1",
            "name": "name 1",
            "type": "datasource",
            "category": "category",
            "uri": "uri 1",
        }, {
            "id": "id 2",
            "name": "name 2",
            "type": "datasource",
            "category": "category",
            "uri": "uri 2",
        }]
    }, {
        "pagination": {
            "totalResults": 4,
            "pageSize": 2,
            "currentPage": 1,
            "nextPageURI": None,
            "previousPageURI": "/rest/v1/datasources/?p=0",
        },
        "results": [{
            "id": "id 3",
            "name": "name 3",
            "type": "datasource",
            "category": "other category",
            "uri": "uri 3",
        }, {
            "id": "id 4",
            "name": "name 4",
            "type": "datasource",
            "category": "category",
            "uri": "uri 4",
        }]
    }]


@pytest.fixture
def datasource_details_result():

    return {
        "id": "id",
        "name": "name",
        "type": "datasource",
        "category": "category",
        "earliestData": "2015-01-01",
        "latestData": "2018-10-01",
        "cadence": "daily",
        "aggregationType": "sum",
        "splitColumns": [
            "country",
            "domain"
        ]
    }


@pytest.fixture
def avro_data_file(datadir):
    with open(os.path.join(datadir, 'avro_data_file'), 'rb') as fp:
        return fp.read()


# ------------------------
# dimensions fixtures
# ------------------------

__uuid = 'ac3fa676-5934-4d8a-969c-62471e91b710'

@pytest.fixture
def fake_uuid():
    return __uuid


@pytest.fixture
def company_with_int_id():
    return Company('1', 'ticker', 'name', 'uri', None)


@pytest.fixture
def other_company_with_int_id():
    return Company('2', 'other_ticker', 'other_name', 'other_uri', None)


@pytest.fixture
def single_page_dimensions_result():
    return {
        "pagination": {
            "totalResults": 3,
            "pageSize": 100,
            "currentPage": 0,
            "nextPageURI": None,
            "previousPageURI": None,
        },
        'results': [{
            'split_combination': {'country': 'US', 'category': 'neat', 'section_pk': [1]},
            'max_date': '2019-01-01',
            'min_date': '2015-01-01',
            'row_count': 10
        }, {
            'split_combination': {'country': 'UK', 'category': 'swell',
                                  'section_pk': [2]},
            'max_date': '2019-01-01',
            'min_date': '2015-01-01',
            'row_count': 10
        }, {
            'split_combination': {'country': 'CA', 'category': 'bad', 'section_pk': [3]},
            'max_date': '2019-02-01',
            'min_date': '2015-02-01',
            'row_count': 10
        }],
        'maxDate': '2019-02-01',
        'minDate': '2015-01-01',
        'rowCount': 30,
        'dimensionCount': 3
    }


@pytest.fixture
def multi_page_dimensions_results():
    return [
        # page 0
        {
            "pagination": {
                "totalResults": 3,
                "pageSize": 2,
                "currentPage": 0,
                "nextPageURI": '/rest/v1/datasource/__UUID__/dimensions?page=1&pagesize=2',
                "previousPageURI": None,
            },
            'results': [{
                'split_combination': {'country': 'US', 'category': 'swell', 'section_pk': [1]},
                'max_date': '2019-01-01',
                'min_date': '2015-01-01',
                'row_count': 10
            }, {
                'split_combination': {'country': 'UK', 'category': 'swell', 'section_pk': [2]},
                'max_date': '2019-01-01',
                'min_date': '2015-01-01',
                'row_count': 10
            }],
            'maxDate': '2019-02-01',
            'minDate': '2015-01-01',
            'rowCount': 30,
            'dimensionCount': 3
        },
        # page 1
        {
            "pagination": {
                "totalResults": 3,
                "pageSize": 1,
                "currentPage": 1,
                "nextPageURI": None,
                "previousPageURI": '/rest/v1/datasource/__UUID__/dimensions?page=0&pagesize=2',
            },
            'results': [{
                'split_combination': {'country': 'CA', 'category': 'bad', 'section_pk': [3]},
                'max_date': '2019-02-01',
                'min_date': '2015-02-01',
                'row_count': 10
            }],
            'maxDate': '2019-02-01',
            'minDate': '2015-01-01',
            'rowCount': 30,
            'dimensionCount': 3
        }
    ]


@pytest.fixture
def large_filter_dict():
    N = 500000000   # 500_000_000
    return {str(i): i for i in range(N)}
