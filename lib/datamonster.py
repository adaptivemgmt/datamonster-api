import fastavro
import pandas
import six

from .client import Client
from .company import Company
from .datasource import Datasource
from .errors import DataMonsterError


class DataMonster(object):
    """Datamonster object. Main entry point to the library"""

    company_path = '/rest/company'
    datasource_path = '/rest/datasource'

    ##############################################
    #           Generic functions
    ##############################################
    def __init__(self, key_id, secret):
        """Must initialize with your key_id and secret"""

        self.client = Client(key_id, secret)
        self.key_id = key_id
        self.secret = secret

    def _get_paginated_results(self, url):
        """Get the paginated results starting with this url"""

        next_page = url
        results = []
        while next_page is not None:
            resp = self.client.get(next_page)
            results += resp['results']
            next_page = resp['pagination']['nextPageURI']

        return results

    def _check_param(self, company=None, datasource=None):
        if company is not None and not isinstance(company, Company):
            raise DataMonsterError("company argument must be a Company object")

        if datasource is not None and not isinstance(company, Datasource):
            raise DataMonsterError("datasource argument must be a Datasource object")

    ##############################################
    #           Company functions
    ##############################################
    def _company_result_to_object(self, company):
        return Company(company['id'], company['ticker'], company['name'], company['uri'], self)

    def get_company_by_ticker(self, ticker):
        """Get a single company by ticker

        :param ticker: Ticker to search for

        :returns: Single Company object if any companies exactly match the ticker.  Raises DatamonsterError otherwise.
        """

        companies = self.get_companies(ticker)
        for company in companies:
            if company.ticker == ticker:
                return company

        raise DataMonsterError("Could not find company with ticker {}".format(ticker))

    def get_companies(self, query=None, datasource=None):
        """Get available companies

        :param query: Optional query that will restrict companies by ticker or name
        :param datasource: Optional Datasource object that restricts companies to those covered by the given datasource

        :returns: List of Company objects
        """

        params = {}
        if query:
            params['q'] = query
        if datasource:
            self._check_param(datasource=datasource)
            params['datasource'] = datasource._id

        url = self.company_path
        if params:
            url = ''.join([url, '?', six.moves.urllib.parse.urlencode(params)])

        companies = self._get_paginated_results(url)
        return map(self._company_result_to_object, companies)

    ##############################################
    #           Datasource functions
    ##############################################
    def _datasource_result_to_object(self, datasource):
        return Datasource(
            datasource['id'],
            datasource['name'],
            datasource['category'],
            datasource['uri'],
            self)

    def get_datasources(self, query=None, company=None):
        """Get available datasources

        :param query: Optional query that will restrict datasources by name or provider name
        :param company: Optional Company object that restricts datasource to those that cover the given company

        :returns: List of Datasource objects
        """

        params = {}
        if query:
            params['q'] = query
        if company:
            self._check_param(company=company)
            params['companyId'] = company._id

        url = self.datasource_path
        if params:
            url = ''.join([url, '?', six.moves.urllib.parse.urlencode(params)])

        datasources = self._get_paginated_results(url)
        return map(self._datasource_result_to_object, datasources)

    def get_data(self, datasource, company, aggregation=None, start_date=None, end_date=None):
        """Get available datasources

        :param datasource: Datasource object to get the data for
        :param company: Company object to filter the datasource on
        :param aggregation: Optional Aggregation object to specify the aggregation of the data
        :param start_date: Optional filter for the start date of the data
        :param end_date: Optional filter for the end date of the data

        :returns: pandas DataFrame
        """

        self._check_param(company=company, datasource=datasource)

        params = {
            'companyId': company._id,
        }

        if start_date is not None:
            params['start_date'] = start_date

        if end_date is not None:
            params['end_date'] = end_date

        url = '{}/{}/data?{}'.format(self.datasource_path, datasource._id, six.moves.urllib.parse.urlencode(params))
        headers = {'Accept': 'avro/binary'}
        resp = self.client.get(url, headers)

        return self._avro_to_df(resp.content)

    def _avro_to_df(self, avro_buffer):
        """Read an avro structure into a dataframe"""

        fp = six.BytesIO(avro_buffer)
        reader = fastavro.reader(fp)
        records = [r for r in reader]
        df = pandas.DataFrame.from_records(records)
        return df
