import fastavro
import pandas
import six
from six import iteritems
import json

from .aggregation import aggregation_sanity_check
from .client import Client
from .company import Company
from .datasource import Datasource
from .errors import DataMonsterError


class DataMonster(object):
    """DataMonster object. Main entry point to the library"""

    company_path = '/rest/v1/company'
    datasource_path = '/rest/v1/datasource'
    dimensions_path = '/rest/v1/datasource/{}/dimensions'

    ##############################################
    #           Generic methods
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
    #           Company methods
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

    def get_company_by_id(self, company_id):
        """Get a single company by id

        :param company_id: (str) company_id to search for, str form of pk, e.g. '718'

        :returns: Single Company if any company matches the id.  Raises DatamonsterError otherwise.
        """
        company = self.get_company_details(company_id)
        company['uri'] = self._get_company_path(company_id)
        return self._company_result_to_object(company, has_details=True)

    def get_company_by_pk(self, company_pk):
        """Get a single company by pk

        :param company_pk: (int) pk to search for

        :returns: Single Company if any company has this pk.  Raises DatamonsterError otherwise.
        """
        return self.get_company_by_id(str(company_pk))

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
        path = self._get_company_path(company_id)
        return self.client.get(path)

    def _get_company_path(self, company_id):
        return '{}/{}'.format(self.company_path, company_id)

    def _company_result_to_object(self, company, has_details=False):
        company_inst = Company(
            company['id'],
            company['ticker'],
            company['name'],
            company['uri'],
            self
        )

        if has_details:
            company_inst.set_details(company)
        return company_inst

    ##############################################
    #           Datasource methods
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
        """Get data for datasource.

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
        """Get details (metadata) for the given datasource

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
        """Read an avro structure into a dataframe
        """
        fp = six.BytesIO(avro_buffer)
        reader = fastavro.reader(fp)
        records = [r for r in reader]
        df = pandas.DataFrame.from_records(records)

        if len(df) == 0:
            return df

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

    ##############################################
    #           Dimensions methods
    ##############################################

    def get_dimensions_for_datasource(self, datasource, filters=None):
        """Get dimensions ("splits") for the data source (data fountain).
        :param datasource: an Oasis data fountain `Datasource`.
        :param filters: ((Dict[str, T] or None): a dict of key/value pairs to filter
                dimensions by; both keys and values are `str`s.
                Example:
                    {'salary_range': "< 10K",
                     'merchant_business_line': "Amazon",
                     'Prime account_type': "Credit Card"}

        :returns: (dict or None)
            for Oasis data fountains, a dict of all dimensions/splits for this data fountain;
            for Legacy `Datasource`s, this method returns `None`.
        :raises: re-raises `DataMonsterError` if self.client.get() raises that
        """
        self._check_param(datasource=datasource)

        params = {}
        if filters:
            params['filters'] = self.to_json_checked(filters)

        url = self._get_dimensions_path(uuid=datasource.id)
        if params:
            url = ''.join([url, '?', six.moves.urllib.parse.urlencode(params)])

        # Let DataMonsterError from self.client.get() happen -- we don't occlude these
        # Formerly `self.client.get(url)` to get all at once.
        dinension_results = self._get_paginated_results(url)
        return six.moves.map(self._dimension_result_pks_to_tickers, dinension_results)

    def _dimension_result_pks_to_tickers(self, result):
        """
        :param result: a dimension dict, with keys
            'split_combination'
            'max_date'
            'min_date'
            'row_count'
        :return: the dict, mutated:
            if 'section_pk' in result['split_combination'], its value
                result['split_combination']['section_pk"]
            is a list of section_pk's (though we acommodate a single pk or None);
            we replace each pk with
                self._pk_to_ticker(pk)          if ticker is not None,
                with str(pk) + '-NO_TICKER'     if ticker is None
        """
        combo = result['split_combination']
        if 'section_pk' in combo:
            value = combo.pop('section_pk')
            if value is not None:
                combo['ticker'] = (
                    self._pk_to_ticker(value)
                    if isinstance(value, int) else
                    six.moves.map(self._pk_to_ticker, value)    # isinstance(value, list) -- List[int] in fact
                )
        return result

    def _pk_to_ticker(self, pk):
        """
        :param pk: int -- a section_pk
        :return: str --
                dm.get_company_from_pk(pk).ticker   if that is not None,
                str(pk) + '-NO_TICKER'              otherwise (ticker is None)
        """
        ticker = self.get_company_by_pk(pk).ticker
        if not ticker:
            ticker = str(pk) + '-NO_TICKER'
        return ticker

    @staticmethod
    def to_json_checked(filters):
        """
        Not "private" because Datasource.get_dimensions() uses it too

        :param filters: dict
        :return: JSON string encoding `filters`, the normal exit if filters is
            JSON-serializable and satisfies the following conditions:
            -- `filters` has all string keys (six.text_type), and
            -- for every key in filters,
                    where T = int
                              if key == 'section_pk" else
                              six.text_type,
               filters[key] is of type T
               OR
               it's a list of T
               (OR it's None)

        These conditions ensure that `filters` is, at least in theory, JSON-serializable,
        which it prrrrobably is, unless other issues obtain, e.g. it's enormous.

        :raises: DataMonsterError if the conditions just described don't hold.
            We try to provide an informative error messages.
        """
        for key, value in iteritems(filters):
            # keys must be strings
            if not isinstance(key, six.text_type):
                raise DataMonsterError(
                    "`filters` problem when getting dimensions: key '{}' is not a string."
                        .format(key))

            # Check value: if key == 'section_pk', value must be let's say list of int,
            # or (being lenient) an int or None. If key != 'section_pk', value must be
            # a string or list of strings (or None)
            type_ = int if key == 'section_pk' else six.text_type
            if not (value is None or
                    isinstance(value, type_) or
                    (isinstance(value, list) and
                     all(isinstance(elt, type_) for elt in value))
            ):
                type_name = type_.__name__
                raise DataMonsterError(
                    "`filters` problem when getting dimensions: "
                    "value '{}' of key '{}' must be a {} or list of {}s or None"
                        .format(value, key, type_name, type_name))
        try:
            return json.dumps(filters)
        except Exception as e:
            # `filters` could NOT be JSON-serialized/-encoded,
            # but no particular key or value was a problem
            raise DataMonsterError(
                "Unexpected problem with `filters` when getting dimensions: {} -- {}".format(
                    type(e).__name__, str(e))
            )

    def _get_dimensions_path(self, uuid):
        return self.dimensions_path.format(uuid)
