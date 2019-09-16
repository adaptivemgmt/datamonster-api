import datetime
import fastavro
import pandas
import six
import json

from .aggregation import aggregation_sanity_check
from .client import Client
from .company import Company
from .datasource import Datasource
from .errors import DataMonsterError

__all__ = ["DataMonster", "DimensionSet"]


class DataMonster(object):
    """DataMonster object. Main entry point to the library"""

    company_path = "/rest/v1/company"
    datasource_path = "/rest/v1/datasource"
    dimensions_path = "/rest/v1/datasource/{}/dimensions"
    rawdata_path = "{}/rawdata?{}"

    REQUIRED_FIELDS = {"lower_date", "upper_date", "value"}

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
            for result in resp["results"]:
                yield result
            next_page = resp["pagination"]["nextPageURI"]

    @staticmethod
    def _check_param(company=None, datasource=None):
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

        :return: Single Company object if any companies exactly match the ticker.
            Raises DataMonsterError otherwise.
        """
        ticker = ticker.lower()
        companies = self.get_companies(ticker)
        for company in companies:
            if company.ticker is not None and company.ticker.lower() == ticker:
                return company

        raise DataMonsterError("Could not find company with ticker {}".format(ticker))

    def get_company_by_id(self, company_id):
        """Get a single company by id

        :param company_id: (str or int) company_id to search for;
                           str form of pk, e.g. '718', or pk e.g. 707

        :return: Single Company if any company matches the id.  Raises DataMonsterError otherwise.
        """
        company = self.get_company_details(company_id)
        company["uri"] = self._get_company_path(company_id)
        return self._company_result_to_object(company, has_details=True)

    def get_companies(self, query=None, datasource=None):
        """Get available companies

        :param query: Optional query that will restrict companies by ticker or name
        :param datasource: Optional Datasource object that restricts companies to those
            covered by the given datasource

        :return: List of Company objects
        """
        params = {}
        if query:
            params["q"] = query
        if datasource:
            self._check_param(datasource=datasource)
            params["datasourceId"] = datasource.id

        url = self.company_path
        if params:
            url = "".join([url, "?", six.moves.urllib.parse.urlencode(params)])

        companies = self._get_paginated_results(url)
        return six.moves.map(self._company_result_to_object, companies)

    def get_company_details(self, company_id):
        """Get details for the given company

        :param company_id: (str or int) The ID of the company for which we get the details
        :return: dictionary object with the company details
        """
        path = self._get_company_path(company_id)
        return self.client.get(path)

    def _get_company_path(self, company_id):
        """
        :param company_id: (str or int)
        :return: URL for REST endpoint that returns details for this company
        """
        return "{}/{}".format(self.company_path, company_id)

    def _company_result_to_object(self, company, has_details=False):
        company_inst = Company(
            company["id"], company["ticker"], company["name"], company["uri"], self
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
        :param company: Optional Company object that restricts datasource to those that cover
            the given company

        :return: List of Datasource objects
        """
        params = {}
        if query:
            params["q"] = query
        if company:
            self._check_param(company=company)
            params["companyId"] = company.id

        url = self.datasource_path
        if params:
            url = "".join([url, "?", six.moves.urllib.parse.urlencode(params)])

        datasources = self._get_paginated_results(url)
        return six.moves.map(self._datasource_result_to_object, datasources)

    def get_datasource_by_id(self, datasource_id):
        """Given an ID, fill in a datasource"""
        datasource = self.get_datasource_details(datasource_id)
        datasource["uri"] = self._get_datasource_path(datasource_id)
        return self._datasource_result_to_object(datasource, has_details=True)

    def get_datasource_details(self, datasource_id):
        """Get details (metadata) for the given datasource

        :param datasource_id: The ID of the datasource for which we get the details
        :return: dictionary object with the datasource details
        """
        path = self._get_datasource_path(datasource_id)
        return self.client.get(path)

    def _get_datasource_path(self, datasource_id):
        return "{}/{}".format(self.datasource_path, datasource_id)

    def _get_rawdata_path(self, datasource_id, params):
        return self.rawdata_path.format(
            self._get_datasource_path(datasource_id),
            six.moves.urllib.parse.urlencode(params),
        )

    def _get_dimensions_path(self, uuid):
        return self.dimensions_path.format(uuid)

    def _datasource_result_to_object(self, datasource, has_details=False):
        ds_inst = Datasource(
            datasource["id"],
            datasource["name"],
            datasource["category"],
            datasource["uri"],
            self,
        )

        if has_details:
            ds_inst.set_details(datasource)

        return ds_inst

    def get_data(
        self, datasource, company, aggregation=None, start_date=None, end_date=None
    ):
        """Get data for datasource.

        :param datasource: Datasource object to get the data for
        :param company: Company object to filter the datasource on
        :param aggregation: Optional Aggregation object to specify the aggregation of the data
        :param start_date: Optional filter for the start date of the data
        :param end_date: Optional filter for the end date of the data

        :return: pandas DataFrame
        """
        # todo: support multiple companies
        self._check_param(company=company, datasource=datasource)

        params = {"companyId": company.id}

        if start_date is not None:
            params["startDate"] = start_date

        if end_date is not None:
            params["endDate"] = end_date

        if aggregation:
            aggregation_sanity_check(aggregation)
            if aggregation.period == "fiscalQuarter":
                if aggregation.company is None:
                    raise DataMonsterError(
                        "Company must be specified for a fiscalQuarter " "aggregation"
                    )
                if aggregation.company.id != company.id:
                    raise DataMonsterError(
                        "Aggregating by the fiscal quarter of a different "
                        "company not yet supported"
                    )

            if aggregation.period is not None:
                params["aggregation"] = aggregation.period

        headers = {"Accept": "avro/binary"}
        url = self._get_rawdata_path(datasource.id, params)
        resp = self.client.get(url, headers, stream=True)
        split_columns = datasource.get_details()["splitColumns"]
        return self._avro_to_df(resp.content, split_columns)

    def _avro_to_df(self, avro_buffer, split_columns):
        """Read an avro structure into a dataframe

        Transforms dates and columns to a stanard and agreed upon format
        """

        def parse_row(row, data_col, start_col, end_col):
            return {
                "value": row[data_col],
                "start_date": pandas.to_datetime(row[start_col]),
                "end_date": pandas.to_datetime(row[end_col]),
                "dimensions": {
                    split_key: row[split_key] for split_key in split_columns
                },
            }

        reader = fastavro.reader(six.BytesIO(avro_buffer))
        metadata = reader.schema["structure"]
        if not metadata:
            raise DataMonsterError(
                "DataMonster does not currently support this request"
            )

        if not set(metadata.keys()).issuperset(self.REQUIRED_FIELDS):
            raise DataMonsterError(
                "DataMonster does not currently support this request"
            )

        records = [r for r in reader]

        if not records:
            return pandas.DataFrame.from_records(records)

        start_col, end_col, data_col = [
            metadata[col].pop() for col in self.REQUIRED_FIELDS
        ]
        records = [parse_row(row, data_col, start_col, end_col) for row in records]

        df = pandas.DataFrame.from_records(records)
        df["time_span"] = df["end_date"] - df["start_date"]
        # Change end_date to not be inclusive
        df["end_date"] -= datetime.timedelta(days=1)

        return df

    ##############################################
    #           Dimensions methods
    ##############################################

    def get_dimensions_for_datasource(
        self, datasource, filters=None, add_company_info_from_pks=False
    ):
        """Get dimensions ("splits") for the data source (data fountain)
        from the DataMonster REST endpoint '/datasource/<uuid>/dimensions?filters=...
        where the filters string is optional.

        :param datasource: an Oasis data fountain `Datasource`.
        :param filters: ((dict or None): a dict of key/value pairs to filter
                dimensions by.
        :param add_company_info_from_pks: (bool) If True, create `'ticker'` items
            from `'section_pk'` items in (`'split_combination'` subdicts of) dimension dicts,
            and create a mapping from section pk's to `Company`s, available as `pk2company`
            on the returned `DimensionSet`; if False, don't.
            `Datasource.get_dimensions()` delegates to this method, and calls with
            `add_company_info_from_pks=True`.

        :return: a `DimensionSet` object - an iterable through a collection
            of dimension dicts, filtered as requested.

            Each dimension dict has these keys:
            'max_date', 'min_date', 'row_count', 'split_combination'.
            The first two are dates, as strings in ISO format; `'row_count'` is an int;
            `'split_combination'` is a dict, containing keys for this datasource --
            things you can filter for.

            EXAMPLE

            Assuming `dm` is a DataMonster object, and given this datasource and company::

                datasource = next(dm.get_datasources(query='1010data Credit Sales Index'))
                the_gap = dm.get_company_by_ticker('GPS')

            this call to `get_dimensions_for_datasource`::

                dimset = dm.get_dimensions_for_datasource(
                                datasource,
                                filters={'section_pk': the_gap.pk,
                                         'category': 'Banana Republic'})

            returns an iterable, `dimset`, to a collection with just one dimensions dict.
            Assuming `from pprint import pprint`, the following loop::

                for dim in dimset:
                    pprint(dim)

            prettyprints the single dimension dict::

                {'max_date': '2019-06-21',
                 'min_date': '2014-01-01',
                 'row_count': 1998,s
                 'split_combination': {'category': 'Banana Republic',
                                       'country': 'US',
                                       'section_pk': 707}}]

        :raises: DataMonsterError if `filters` is not a dict or is not JSON-serializable.
            Re-raises `DataMonsterError` if self.client.get() raises that.
        """
        self._check_param(datasource=datasource)

        params = {}
        if filters:
            params["filters"] = self.to_json_checked(filters)

        url = self._get_dimensions_path(uuid=datasource.id)
        if params:
            url = "".join([url, "?", six.moves.urllib.parse.urlencode(params)])

        # Let any DataMonsterError from self.client.get() happen -- we don't occlude them
        return DimensionSet(
            url, self, add_company_info_from_pks=add_company_info_from_pks
        )

    @staticmethod
    def to_json_checked(filters):
        """
        Not "private" because `Datasource.get_dimensions()` uses it too

        :param filters: dict
        :return: JSON string encoding `filters`. Normal exit if `filters` is
            JSON-serializable.

        :raises: DataMonsterError if `filters` isn't a dict or can't be JSON-encoded.
        """
        if not isinstance(filters, dict):
            raise DataMonsterError(
                "`filters` must be a dict, got {} instead".format(
                    type(filters).__name__
                )
            )
        try:
            return json.dumps(filters)
        except TypeError as e:
            raise DataMonsterError(
                "Problem with filters when getting dimensions: {}".format(e)
            )


class DimensionSet(object):
    """
    An iterable through a collection of *dimension dicts*, with additional metadata:

    `max_date`:
        (string) max of the `max_date`s of the dimension dicts;

    `min_date`:
        (string) min of the `min_date`s of the dimension dicts;

    `row_count`:
        (int) sum of the `row_count`s of the dimension dicts;

    `len(dimension_set)`:
        (int) number of dimension dicts in the collection

    `has_extra_company_info`:
        (bool) the value passed as `add_company_info_from_pks` to the constructor, coerced
            to `bool`.

    `pk2company`:
        (dict) Empty if `has_extra_company_info` is `False`.
        If `has_extra_company_info`, this dict maps
        company pk's (int id's) to their corresponding `Company`s,
        for all pk's in `'section_pk'` items of dimension dicts in the collection.
        During an iteration, `pk2company` contains all pk's from `'section_pk'` values in
        dimension dicts *encountered so far*. Thus, `pk2company` is initially empty, and
        isn't fully populated until the iteration completes.

    Each dimension dict has these keys:
    'max_date', 'min_date', 'row_count', 'split_combination'.
    The first two are dates, as strings in ISO format; `'row_count'` is an int;
    `'split_combination'` is a dict.
    """

    def __init__(self, url, dm, add_company_info_from_pks):
        """
        :param url: (string) URL for REST endpoint
        :param dm: DataMonster object
        :param add_company_info_from_pks: (bool) If True, create ticker items from
         'section_pk' items.
        """
        self._url_orig = url

        resp0 = dm.client.get(url)

        self._min_date = resp0["minDate"]
        self._max_date = resp0["maxDate"]
        self._row_count = resp0["rowCount"]
        self._dimension_count = resp0["dimensionCount"]
        self._resp = resp0

        self._dm = dm
        self._add_company_info_from_pks = bool(add_company_info_from_pks)

        # Populated during iteration, maps pk => Company.
        # Contents are not "settled" until iteration is complete.
        self._pk2company = {}

    def __str__(self):
        has_extra_info_str = (
            "; extra company info" if self.has_extra_company_info else ""
        )

        "{}: {} dimensions, {} rows, from {} to {}{}".format(
            self.__class__.__name__,
            len(self),
            self._row_count,
            self._min_date,
            self._max_date,
            has_extra_info_str,
        )

    @property
    def pk2company(self):
        """Empty if `has_extra_company_info` is `False`.
        If `has_extra_company_info`, this dict maps company pk's (int id's) to `Company`
        objects. If `pk` is a key in the dict, then `self.pk2company[pk].pk == pk`.
        The pk's in `pk2company` are those in the `'section_pk'` items of dimension dicts
        in this collection. (`'section_pk'` items are in the `'split_combination'` subdict
        of a dimension dict.)

        During an iteration, `pk2company` contains all pk's from `'section_pk'` values in
        dimension dicts *that have been yielded so far*. Thus, `pk2company` is initially
        empty, and isn't fully populated until the iteration completes.

        Note that making a `list` of a `DimensionSet` performs a complete iteration.

        :return: (dict)
        """
        return self._pk2company

    @property
    def min_date(self):
        """min of the `min_date`s of the dimension dicts
        :return type: str
        """
        return self._min_date

    @property
    def max_date(self):
        """
        (str) max of the `max_date`s of the dimension dicts
        """
        return self._max_date

    @property
    def row_count(self):
        """
        (int) sum of the `row_count`s of the dimension dicts
        """
        return self._row_count

    @property
    def has_extra_company_info(self):
        """
        (bool) The value passed as `add_company_info_from_pks` to the constructor, coerced
            to `bool`.
        """
        return self._add_company_info_from_pks

    def __len__(self):
        """
        (int) number of *dimension dicts* in the collection
        """
        return self._dimension_count

    def __iter__(self):
        """Generator that iterates through the dimension dicts in the collection.

        Populates self.pk2company during iteration:
            `section_pk`s already in this dict will use the tickers (/names) of `Company`s
            already looked up and saved;
            newly-encountered `section_pk`s will have their corresponding `Company`s saved here
        """
        while True:
            resp = self._resp  # shorthand
            if not resp:
                return

            results_this_page = resp["results"]
            next_page_uri = resp["pagination"]["nextPageURI"]

            if not results_this_page:
                break

            for dimension in results_this_page:
                # do `_camel2snake` *before* possible pk->ticker conversion,
                # as `_create_ticker_items_from_section_pks` assumes snake_case
                # ('split_combination')
                dimension = DimensionSet._camel2snake(dimension)
                if self._add_company_info_from_pks:
                    self._create_ticker_items_from_section_pks(dimension)
                yield dimension

            if next_page_uri is None:
                break

            self._resp = self._dm.client.get(next_page_uri)

        # So that attempts to reuse the iterator get nothing.
        # Without this, the last page could be re-yielded
        self._resp = None

    @staticmethod
    def _camel2snake(dimension_dict):
        """Return a dict with four keys changed from camelCase to snake_case;
        `dimension_dict` unchanged
        """
        camel2snake = {
            "splitCombination": "split_combination",
            "maxDate": "max_date",
            "minDate": "min_date",
            "rowCount": "row_count",
        }
        return {camel2snake[k]: dimension_dict[k] for k in dimension_dict}

    def _create_ticker_items_from_section_pks(self, dimension):
        """
        :param dimension: a dimension dict, with a key 'split_combination'.

        :return: `None`
        Mutates the dict `dimension:
            if 'section_pk' in `dimension['split_combination']`, its value::

                dimension['split_combination']['section_pk"]

            is a 'section_pk'` or a list of them (we accommodate `None`, too).
            We add a `'ticker'` item to dimension['split_combination'] whose value is
            the ticker or tickers for the pk's in the value of 'section_pk'` --
            more precisely, the value corresponding to any `pk` is:

            `self._pk_to_ticker(pk)`
                  if ticker is not `None`,

            name of company with key `pk`
               if ticker is `None`
        """
        combo = dimension["split_combination"]
        if "section_pk" in combo:
            value = combo.get("section_pk")  # type: int or list[int]
            if value is not None:
                combo["ticker"] = (
                    self._pk_to_ticker(value)
                    if isinstance(value, int)
                    else list(six.moves.map(lambda pk: self._pk_to_ticker(pk), value))
                )
        return dimension

    def _pk_to_ticker(self, pk):
        """
        :param pk: int -- a section_pk

        :return: str --

            `self._dm.get_company_from_id(pk).ticker`
                if that is not `None`,

            name of company with key `pk`
                otherwise (actual ticker is `None` or empty)

            Note that `self._pk2company` basically holds memos for this method: for each `pk`,
            `self._dm.get_company_from_id(pk)` is only called once.
        """
        if pk not in self._pk2company:
            self._pk2company[pk] = self._dm.get_company_by_id(pk)

        company = self._pk2company[pk]
        return company.ticker or company.name
