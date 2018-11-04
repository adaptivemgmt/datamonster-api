import os
import pytest

from lib.company import Company
from lib.datamonster import DataMonster
from lib.datasource import Datasource


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
            "uri": "https://dm.datamonster.com/rest/company/1"
        }, {
            "name": "Company 2",
            "ticker": "c2",
            "id": "2",
            "uri": "https://dm.datamonster.com/rest/company/2"
        }]
    }


@pytest.fixture
def multi_page_company_results():
    return [{
        "pagination": {
            "totalResults": 4,
            "pageSize": 2,
            "currentPage": 0,
            "nextPageURI": '/rest/company?p=1',
            "previousPageURI": None,
        },
        "results": [{
            "name": "Company 1",
            "ticker": "c1",
            "id": "1",
            "uri": "https://dm.datamonster.com/rest/company/1"
        }, {
            "name": "Company 2",
            "ticker": "c2",
            "id": "2",
            "uri": "https://dm.datamonster.com/rest/company/2"
        }]
    }, {
        "pagination": {
            "totalResults": 4,
            "pageSize": 2,
            "currentPage": 1,
            "nextPageURI": None,
            "previousPageURI": '/rest/company?p=0',
        },
        "results": [{
            "name": "Company 3",
            "ticker": "c3",
            "id": "3",
            "uri": "https://dm.datamonster.com/rest/company/3"
        }, {
            "name": "Company 4",
            "ticker": "c4",
            "id": "4",
            "uri": "https://dm.datamonster.com/rest/company/4"
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
            "nextPageURI": "/rest/datasources/?p=1",
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
            "previousPageURI": "/rest/datasources/?p=0",
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
def avro_data_file(datadir):
    with open(os.path.join(datadir, 'avro_data_file')) as fp:
        return fp.read()
