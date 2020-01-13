import datetime
import fastavro
import json
import pandas
import six

from .aggregation import aggregation_sanity_check
from .client import Client
from .company import Company
from .data_group import DataGroup, DataGroupColumn
from .datasource import Datasource
from .errors import DataMonsterError
from .utils import format_date

__all__ = ["DataMonster", "DimensionSet"]


class DataMonster(object):
    """DataMonster object. Main entry point to the library

    :param key_id: (str) a user's public key
    :param secret: (str) a user's secret key
    :param server: (optional, str) default to dm.adaptivemgmt.com
    :param verify: (optional, bool) whether to verify the server's TLS certificate
    """

    company_path = "/rest/v1/company"
    datasource_path = "/rest/v1/datasource"
    dimensions_path = "/rest/v1/datasource/{}/dimensions"
    rawdata_path = "{}/rawdata?{}"
    data_group_path = '/rest/v1/data_group'

    DATAMONSTER_SCHEMA_FIELDS = {
        "lower_date": "start_date",
        "upper_date": "end_date",
        "value": "value",
    }

    def __init__(self, key_id, secret, server=None, verify=True):
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

        :return: Single ``Company`` object if any companies exactly match the ticker (case insensitive)

        :raises: ``DataMonsterError`` if no companies match ticker
        """
        ticker = ticker.lower()
        companies = self.get_companies(ticker)
        for company in companies:
            if company.ticker is not None and company.ticker.lower() == ticker:
                return company

        raise DataMonsterError("Could not find company with ticker {}".format(ticker))

    def get_company_by_id(self, company_id):
        """Get a single company by id

        :param company_id: (str or int) unique internal identifier for the desired company.
                           Can take str form e.g. '718', or int form, e.g. 707.
                           In order to find the id of a frequently used company,
                           find the company by ticker and call ``.pk`` on the resulting ``Company`` object

        :return: Single ``Company`` object if any company matches the id

        :raises: ``DataMonsterError`` if no company matches id
        """
        company = self.get_company_details(company_id)
        company["uri"] = self._get_company_path(company_id)
        return self._company_result_to_object(company, has_details=True)

    def get_companies(self, query=None, datasource=None):
        """Get available companies

        :param query: Optional query that will restrict companies by ticker or name
        :param datasource: Optional ``Datasource`` object that restricts companies to those
            covered by the given data source

        :return: Iterator of ``Company`` objects
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

        :param company_id: (str or int) unique internal identifier for company.
                           See `this method <api.html#datamonster_api.DataMonster.get_company_by_id>`__
                           for more info on company_id
        :return: (dict) details (metadata) for this company, providing basic information as stored in DataMonster
        """
        path = self._get_company_path(company_id)
        return self.client.get(path)

    def _get_company_path(self, company_id):
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

        :param query: (str) Optional query that will restrict data sources by name or provider name
        :param company: Optional ``Company`` object that restricts data sources to those that cover
            the given company

        :return: Iterator of ``Datasource`` objects
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

    def get_datasource_by_name(self, name):
        """Given a name, try to find a data source of that name

        :param name: (str)

        :return: Single ``Datasource`` object with the given name

        :raises: ``DataMonsterError`` if no data source matches the given name
        """
        for ds in self.get_datasources(query=name):
            if ds.name.lower() == name.lower():
                return ds
        raise DataMonsterError(
            "Did not find a data source matching the name {!r}".format(name)
        )

    def get_datasource_by_id(self, datasource_id):
        """Given a data source uuid, return the corresponding ``Datasource`` object

        :param datasource_id: (str)

        :return: Single ``Datasource`` object with the given id

        :raises: ``DataMonsterError`` if no data source matches the given id
        """
        datasource = self.get_datasource_details(datasource_id)
        datasource["uri"] = self._get_datasource_path(datasource_id)
        return self._datasource_result_to_object(datasource, has_details=True)

    def get_datasource_details(self, datasource_id):
        """Get details (metadata) for the data source corresponding to the given id

        :param datasource_id: (str)

        :return: (dict) details (metadata) for this data source,
            providing basic information as stored in DataMonster
        """
        path = self._get_datasource_path(datasource_id)
        return self.client.get(path)

    def _get_datasource_path(self, datasource_id):
        return "{}/{}".format(self.datasource_path, datasource_id)

    def _get_data_group_path(self, data_group_id):
        return '{}/{}'.format(self.data_group_path, data_group_id)

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
        """Get data for data source

        :param datasource: ``Datasource`` object to get the data for
        :param company: ``Company`` object to filter the data source on
        :param aggregation: Optional ``Aggregation`` object to specify the aggregation of the data
        :param start_date: Optional filter for the start date of the data
        :param end_date: Optional filter for the end date of the data

        See `here <quickstart.html#>`__ for example usage.

        :return: pandas.DataFrame
        """
        # todo: support multiple companies
        self._check_param(company=company, datasource=datasource)

        params = {"section_pk": company.id}

        if start_date is not None:
            if not datasource.upperDateField:
                raise DataMonsterError("This data source does not support date queries")
            key = "{}__gte".format(datasource.upperDateField)
            params[key] = format_date(start_date)

        if end_date is not None:
            if not datasource.lowerDateField:
                raise DataMonsterError("This data source does not support date queries")
            key = "{}__lt".format(datasource.lowerDateField)
            params[key] = format_date(end_date)

        if aggregation:
            aggregation_sanity_check(aggregation, company=company)
            if aggregation.period is not None:
                params["aggregation"] = aggregation.period

        schema, df = self.get_raw_data(datasource, **params)

        if datasource.type == "datasource":
            df = self._datamonster_data_mapper(
                self.DATAMONSTER_SCHEMA_FIELDS, schema, df
            )

        if "end_date" in df:
            df.sort_values(by="end_date", inplace=True)
        return df

    def get_raw_data(self, datasource, **kwargs):
        """Get raw data for all companies available in the data source.

        :param datasource: ``Datasource`` object to get the data for
        :param kwargs: unparsed ``kwargs`` to get passed as query parameters

        :return: (schema, pandas.DataFrame)

        See `here <examples.html#get-raw-data>`__ for example usage.
        """
        headers = {"Accept": "avro/binary"}
        url = self._get_rawdata_path(datasource.id, kwargs)
        resp = self.client.get(url, headers, stream=True)
        return self._avro_to_df(resp.content, datasource.fields)

    def _avro_to_df(self, avro_buffer, data_types):
        """Read an avro structure into a dataframe and minimially parse it

        returns: (schema, pandas.Dataframe)
        """

        def parse_row(row):
            return {
                col["name"]: pandas.to_datetime(row[col["name"]])
                if col["data_type"] == "date"
                else row[col["name"]]
                for col in data_types
            }

        reader = fastavro.reader(six.BytesIO(avro_buffer))
        metadata = reader.writer_schema.get("structure", ())

        if not metadata:
            raise DataMonsterError(
                "DataMonster does not currently support this request"
            )

        records = [parse_row(r) for r in reader]
        return metadata, pandas.DataFrame.from_records(records)

    @staticmethod
    def _datamonster_data_mapper(mapping_fields, schema, df):
        """mapping function applied to a ``DataMonster`` data source to format the data

        :param mapping_fields (dict): mapping of column names to rename from in the schema
        :param schema (dict): avro schema of the data
        :param df (pandas.DataFrame): data to manipulate

        :return: pandas.DataFrame
        """
        if df.empty:
            return df

        if not set(schema.keys()).issuperset(mapping_fields.keys()):
            raise DataMonsterError(
                "DataMonster does not currently support this request"
            )

        split_columns = schema.get("split", [])
        rename_columns = {}
        for key, val in mapping_fields.items():
            if len(schema[key]) != 1:
                raise DataMonsterError(
                    "Expected a single defined column for {!r}. Got {!r}".format(
                        key, schema[key]
                    )
                )
            rename_columns[schema[key][0]] = val

        df.rename(columns=rename_columns, inplace=True)
        df["dimensions"] = df.apply(
            lambda row, *splits: {split: row[split] for split in splits},
            args=(split_columns),
            axis=1,
        )

        df["time_span"] = df["end_date"] - df["start_date"]
        df["end_date"] -= datetime.timedelta(
            days=1
        )  # Change the format of the end_date
        drop_columns = [col for col in split_columns + ["section_pk"] if col in df]
        df.drop(columns=drop_columns, inplace=True)
        return df

    def get_dimensions_for_datasource(
        self, datasource, filters=None, add_company_info_from_pks=False
    ):
        """Get dimensions ("splits") for the data source
            from the DataMonster REST endpoint ``/datasource/<uuid>/dimensions?filters=...``
            where the ``filters`` string is optional.

        :param datasource:  ``Datasource`` object
        :param filters: (dict): a dict of key/value pairs to filter
                dimensions by
        :param add_company_info_from_pks: (bool): Determines whether return value will include tickers for
            the returned companies. If ``False``, only ``section_pk`` s will be returned.

        See `here <examples.html#get-dimensions-for-datasource>`__
        for example usage.

        :return: a ``DimensionSet`` object - an iterable through a collection
            of dimension dicts, filtered as requested. See `this documentation <api.html#datamonster_api.DimensionSet>`_
            for more info on ``DimensionSet`` objects.

        :raises: ``DataMonsterError`` if ``filters`` is not a dict or is not JSON-serializable.
            Re-raises ``DataMonsterError`` if ``self.client.get()`` raises that.
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

    ##############################################
    #           DataGroup methods
    ##############################################

    def get_data_groups(self, query=None):
        """Get available data groups

        :param query: (str) Optional query that will restrict data groups by name or data source name
        :return: Iterator of ``DataGroup`` objects.
        """
        params = {}
        if query is not None:
            params['q'] = query

        url = self.data_group_path
        if params:
            url = ''.join([url, '?', six.moves.urllib.parse.urlencode(params)])

        datagroups = self._get_paginated_results(url)
        return six.moves.map(self._data_group_result_to_object, datagroups)

    def get_data_group_details(self, id):
        """Given a data group id, return the corresponding ``DataGroup`` object

        :param id: (int)

        :return: Single ``DataGroup`` object with the given id

        :raises: ``DataMonsterError`` if no data group matches the given id
        """
        path = self._get_data_group_path(id)
        return self.client.get(path)

    def get_data_group_by_id(self, id):
        dg = self.get_data_group_details(id)
        return self._data_group_result_to_object(dg, has_details=True)

    def _data_group_result_to_object(self, data_group, has_details=False):
        columns = [DataGroupColumn(**column) for column in data_group['columns']]
        dg_inst = DataGroup(
            data_group['_id'],
            data_group['name'],
            columns,
            self
        )

        if has_details:
            dg_inst.set_details(data_group)

        return dg_inst


class DimensionSet(object):
    """
    An iterable through a collection of dimensions dictionaries.

    Each dimension dictionary has 4 keys:
    ``max_date``, ``min_date``, ``row_count``, and ``split_combination``.
    The first two have values that are dates as strings in ISO format;
    ``split_combination`` points to a dict containing data from all other columns;
    ``row_count`` points to an int specifying how many rows match the dates and all splits in ``split_combination``

    """

    def __init__(self, url, dm, add_company_info_from_pks):
        """
        :param url: (string) URL for REST endpoint
        :param dm: DataMonster object
        :param add_company_info_from_pks: (bool) If ``True``, create ticker items from
         ``section_pk`` items.
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

    @property
    def pk2company(self):
        """Empty if ``has_extra_company_info`` is ``False``.
        If ``has_extra_company_info``, this dict maps company pk's (int id's) to ``Company``
        objects. If ``pk`` is a key in the dict, then ``self.pk2company[pk].pk == pk``.
        The pk's in ``pk2company`` are those in the ``section_pk`` items of dimension dicts
        in this collection. (``section_pk`` items are in the ``split_combination`` subdict
        of a dimension dict.)

        During an iteration, ``pk2company`` contains all pk's from ``section_pk`` values in
        dimension dicts *that have been yielded so far*. Thus, ``pk2company`` is initially
        empty, and isn't fully populated until the iteration completes.

        Note that making a *list* of a ``DimensionSet`` performs a complete iteration.

        :return: (dict)
        """
        return self._pk2company

    @property
    def min_date(self):
        """
        :return type: (str) min of the ``min_date`` of the dimension dicts
        """
        return self._min_date

    @property
    def max_date(self):
        """
        :return: (str) max of the ``max_date`` of the dimension dicts
        """
        return self._max_date

    @property
    def row_count(self):
        """
        :return: (int) number of rows matching the filters for this ``DimensionSet``
        """
        return self._row_count

    @property
    def has_extra_company_info(self):
        """
        :return: (bool) The value passed as ``add_company_info_from_pks`` to the constructor, coerced
            to *bool*.
        """
        return self._add_company_info_from_pks

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
