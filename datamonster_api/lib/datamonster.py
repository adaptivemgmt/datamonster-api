import fastavro
import pandas
import six
import json

from .aggregation import aggregation_sanity_check
from .client import Client
from .company import Company
from .datasource import Datasource
from .errors import DataMonsterError

__all__ = ['DataMonster', 'DimensionSet']


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

        :returns: Single Company object if any companies exactly match the ticker.  Raises DataMonsterError otherwise.
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

        :returns: Single Company if any company matches the id.  Raises DataMonsterError otherwise.
        """
        company = self.get_company_details(company_id)
        company['uri'] = self._get_company_path(company_id)
        return self._company_result_to_object(company, has_details=True)

    def get_company_by_pk(self, company_pk):
        """Get a single company by pk

        :param company_pk: (int) pk to search for

        :returns: Single Company if any company has this pk.  Raises DataMonsterError otherwise.
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


    def get_dimensions_for_datasource(self, datasource, filters=None,
                                      _pk2ticker=False):
        """Get dimensions ("splits") for the data source (data fountain)
        from the DataMonster REST endpoint '/datasource/<uuid>/dimensions?filters=...
        where the filters string is optional.

        :param datasource: an Oasis data fountain `Datasource`.
        :param filters: ((dict or None): a dict of key/value pairs to filter
                dimensions by.
        :param _pk2ticker: (bool) If True, convert 'section_pk' items to 'tickers' items;
            if False, don't. Datasource.get_dimensions() delegates to this method,
            and calls with _pk2ticker=True

        Return the dimensions for this data source, filtered by `filters`.

        :return: a `DimensionSet` object - say, ``dimset` - an iterable through a dimension dicts,
            filtered as requested. The object has a additional metadata:

                `max_date`:  (string) max of the `max_date`s of the dimension dicts;
                `min_date`:  (string) min of the `min_date`s of the dimension dicts;
                `row_count`:  (int) sum of the `row_count`s of the dimension dicts;
                `len(dimset)`: (int) number of dimension dicts in the collection

            Each dimension dict has these keys:
            'max_date', 'min_date', 'row_count', 'split_combination'.
            The first two are dates, as strings in ISO format; `'row_count'` is an int;
            `'split_combination'` is a dict, containing keys for this datasource --
            things you can filter for.

            EXAMPLE
            --------
            Assuming `dm` is a DataMonster object, and given this datasource and company:

                datasource = next(dm.get_datasources(query='1010data Credit Sales Index'))
                the_gap = dm.get_company_by_ticker('GPS')

            this call to `get_dimensions`

                dimset = dm.get_dimensions_for_datasource(
                                datasource,
                                filters={'section_pk': the_gap.pk,
                                         'category': 'Banana Republic'})

            returns an iterable, `dimset`, to a collection with just one dimensions dict.
            The following loop

                for dim in dimset:
                    pprint(dim)

            prettyprints the single dimension dict:

                {'max_date': '2019-06-21',
                 'min_date': '2014-01-01',
                 'row_count': 1998,
                 'split_combination': {'category': 'Banana Republic',
                                       'country': 'US',
                                       'section_pk': 707}}]

        :raises: DataMonsterError if `filters` is not a dict or is not JSON-serializable.
            Re-raises `DataMonsterError` if self.client.get() raises that.
        """
        self._check_param(datasource=datasource)

        params = {}
        if filters:
            params['filters'] = self.to_json_checked(filters)

        url = self._get_dimensions_path(uuid=datasource.id)
        if params:
            url = ''.join([url, '?', six.moves.urllib.parse.urlencode(params)])

        # Let any DataMonsterError from self.client.get() happen -- we don't occlude them
        return DimensionSet(url, self, _pk2ticker=_pk2ticker)

    def _convert_section_pks_to_tickers(self, dimension, pk2ticker_memos):
        """
        :param dimension: a dimension dict, with a key 'split_combination'.
        :param pk2ticker_memos: (dict) maps section_pk => ticker
            section_pk's already in this dict will use the tickers already looked up and saved;
            new section_pk's will have their tickers saved here

        :return: the dict, mutated:
            if 'section_pk' in result['split_combination'], its value
                result['split_combination']['section_pk"]
            is a list of section_pk's (though we acommodate a single pk or None);
            we replace each pk with
                self._pk_to_ticker(pk)          if ticker is not None,
                with str(pk) + '-NO_TICKER'     if ticker is None
        """
        combo = dimension['split_combination']
        if 'section_pk' in combo:
            value = combo.pop('section_pk')
            if value is not None:
                combo['ticker'] = (
                    self._pk_to_ticker(value, pk2ticker_memos)
                    if isinstance(value, int) else          # isinstance(value, list) -- List[int] in fact
                    six.moves.map(lambda pk: self._pk_to_ticker(pk, pk2ticker_memos), value)
                )
        return dimension

    def _pk_to_ticker(self, pk, pk2ticker_memos):
        """
        :param pk: int -- a section_pk
        :param pk2ticker_memos: (dict) maps section_pk => ticker
            section_pk's already in this dict will use the tickers already looked up and saved;
            newly-encountered section_pk's will have their tickers saved here

        :return: str --
                dm.get_company_from_pk(pk).ticker   if that is not None,
                str(pk) + '-NO_TICKER'              otherwise (actual ticker is None or empty)
        """
        if pk not in pk2ticker_memos:
            company = self.get_company_by_pk(pk)
            ticker = company.ticker
            if not ticker:
                ticker = company.name
            pk2ticker_memos[pk] = ticker

        return pk2ticker_memos[pk]

    @staticmethod
    def to_json_checked(filters):
        """
        Not "private" because Datasource.get_dimensions() uses it too

        :param filters: dict
        :return: JSON string encoding `filters`. Normal exit if `filters` is
            JSON-serializable.

        :raises: DataMonsterError if `filters` isn't a dict or can't be JSON-encoded.
        """
        if not isinstance(filters, dict):
            raise DataMonsterError(
                "`filters` must be a dict, got {} instead".format(type(filters).__name__)
            )
        try:
            return json.dumps(filters)
        except TypeError as e:
            raise DataMonsterError(
                "Problem with filters when getting dimensions: {}".format(e)
            )

    def _get_dimensions_path(self, uuid):
        return self.dimensions_path.format(uuid)


class DimensionSet(object):
    def __init__(self, url, dm, _pk2ticker):
        """
        :param url: (string) URL for REST endpoint
        :param dm: DataMonster object
        :param _pk2ticker: (bool) If True, convert 'section_pk' items to 'tickers' items
        """
        self._url_orig = url

        resp0 = dm.client.get(url)

        self._min_date = resp0['min_date']
        self._max_date = resp0['max_date']
        self._row_count = resp0['row_count']
        self._dimension_count = resp0['dimension_count']
        self._resp = resp0

        self._dm = dm
        self._pk2ticker = _pk2ticker

    @property
    def min_date(self):
        return self._min_date

    @property
    def max_date(self):
        return self._max_date

    @property
    def row_count(self):
        return self._row_count

    def __len__(self):
        return self._dimension_count

    def __iter__(self):
        pk2ticker_memos = {}
        while True:
            resp = self._resp       # shorthand
            if not resp:
                return

            results_this_page = resp['results']
            next_page_uri = resp['pagination']['nextPageURI']

            if not results_this_page:
                break

            for dimension in results_this_page:
                if self._pk2ticker:
                    dimension = self._dm._convert_section_pks_to_tickers(dimension, pk2ticker_memos)
                yield dimension

            if next_page_uri is None:
                break

            self._resp = self._dm.client.get(next_page_uri)

        # So that attempts to reuse the iterator get nothing.
        # Without this, the last page could be re-yielded
        self._resp = None
