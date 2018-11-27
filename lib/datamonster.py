import fastavro
import pandas
import six

from .aggregation import aggregation_sanity_check
from .client import Client
from .company import Company
from .datasource import Datasource
from .errors import DataMonsterError


class DataMonster(object):
    """Datamonster object. Main entry point to the library"""

    company_path = '/rest/v1/company'
    datasource_path = '/rest/v1/datasource'

    ##############################################
    #           Generic functions
    ##############################################
    def __init__(self, key_id, secret, server=None, verify=True):
        """Must initialize with your key_id and secret"""

        self.client = Client(key_id, secret, server, verify)
        self.key_id = key_id
        self.secret = secret

    def _get_paginated_results(self, url):
        """Get the paginated results starting with this url"""

        next_page = url
        while next_page is not None:
            resp = self.client.get(next_page)
            for result in resp['results']:
                yield result
            next_page = resp['pagination']['nextPageURI']

    def _check_param(self, company=None, datasource=None):
        if company is not None and not isinstance(company, Company):
            raise DataMonsterError("company argument must be a Company object")

        if datasource is not None and not isinstance(datasource, Datasource):
            raise DataMonsterError("datasource argument must be a Datasource object")

    ##############################################
    #           Company functions
    ##############################################
    def get_company_by_ticker(self, ticker):
        """Get a single company by ticker

        :param ticker: Ticker to search for

        :returns: Single Company object if any companies exactly match the ticker.  Raises DatamonsterError otherwise.
        """

        ticker = ticker.lower()
        companies = self.get_companies(ticker)
        for company in companies:
            if company.ticker is not None and company.ticker.lower() == ticker:
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
            params['datasourceId'] = datasource.id

        url = self.company_path
        if params:
            url = ''.join([url, '?', six.moves.urllib.parse.urlencode(params)])

        companies = self._get_paginated_results(url)
        return six.moves.map(self._company_result_to_object, companies)

    def get_company_details(self, company_id):
        """Get details for the given company

        :param company_id: The ID of the company for which we get the details
        :returns: dictionary object with the company details
        """

        path = '{}/{}'.format(self.company_path, company_id)
        return self.client.get(path)

    def _company_result_to_object(self, company):
        return Company(company['id'], company['ticker'], company['name'], company['uri'], self)

    ##############################################
    #           Datasource functions
    ##############################################
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
            params['companyId'] = company.id

        url = self.datasource_path
        if params:
            url = ''.join([url, '?', six.moves.urllib.parse.urlencode(params)])

        datasources = self._get_paginated_results(url)
        return six.moves.map(self._datasource_result_to_object, datasources)

    def get_datasource_by_id(self, datasource_id):
        """Given an ID, fill in a datasource"""
        datasource = self.get_datasource_details(datasource_id)
        datasource['uri'] = self._get_datasource_path(datasource_id)
        return self._datasource_result_to_object(datasource, has_details=True)

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
            'companyId': company.id,
        }

        if start_date is not None:
            params['startDate'] = start_date

        if end_date is not None:
            params['endDate'] = end_date

        if aggregation:
            aggregation_sanity_check(aggregation)
            if aggregation.period == 'fiscalQuarter':
                if aggregation.company is None:
                    raise DataMonsterError("Company must be specified for a fiscalQuarter aggregation")
                if aggregation.company.id != company.id:
                    raise DataMonsterError("Aggregating by the fiscal quarter of a different company not yet supported")

            if aggregation.period is not None:
                params['aggregation'] = aggregation.period

        url = '{}/{}/data?{}'.format(self.datasource_path, datasource.id, six.moves.urllib.parse.urlencode(params))
        headers = {'Accept': 'avro/binary'}
        resp = self.client.get(url, headers)

        return self._avro_to_df(resp)

    def get_datasource_details(self, datasource_id):
        """Get details for the given datasource

        :param datasource_id: The ID of the datasource for which we get the details
        :returns: dictionary object with the datasource details
        """

        path = self._get_datasource_path(datasource_id)
        return self.client.get(path)

    def _get_datasource_path(self, datasource_id):
        return '{}/{}'.format(self.datasource_path, datasource_id)

    def _datasource_result_to_object(self, datasource, has_details=False):
        ds_inst = Datasource(
            datasource['id'],
            datasource['name'],
            datasource['category'],
            datasource['uri'],
            self)

        if has_details:
            ds_inst.set_details(datasource)

        return ds_inst

    def _avro_to_df(self, avro_buffer):
        """Read an avro structure into a dataframe"""

        fp = six.BytesIO(avro_buffer)
        reader = fastavro.reader(fp)
        records = [r for r in reader]
        df = pandas.DataFrame.from_records(records)

        # Convert date columns to datetime64 columns
        df['upperDate'] = df['upperDate'].astype('datetime64[ns]')
        df['lowerDate'] = df['lowerDate'].astype('datetime64[ns]')

        # Create the timespan. Note we add 1 day because both dates are inclusive
        df['upperDate'] += pandas.DateOffset(1)
        df['time_span'] = df['upperDate'] - df['lowerDate']

        # Remove the upperDate to reduce confusion
        del df['upperDate']

        # Rename the start_date. There's a more performant way to do this somewhere
        df['start_date'] = df['lowerDate']
        del df['lowerDate']

        return df
