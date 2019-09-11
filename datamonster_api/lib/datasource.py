from .base import BaseClass
from .company import Company
from .errors import DataMonsterError


class Datasource(BaseClass):
    """Datasource object which represents a data source in DataMonster

    :param params: (dict)
    :param dm: DataMonster object

    Attributes include:
      name: (str) datasource name
      category: (str) category associated with the datasource
    """

    def __init__(self, params, dm):
        self._dm = dm
        self._params = params
        self._id = params["id"]
        self._uri = params["uri"]
        self.name = params["name"]
        self.category = params["category"]

    def get_details(self):
        """Get details (metadata) for this datasource

        :return: (dict) details
        """
        return self._dm.get_datasource_details(self._id)

    @property
    def companies(self):
        """get the (memoized) companies for this data source.

        :return: (iter) iterable of Company objects
        """
        if not hasattr(self, "_companies"):
            self._companies = self._dm.get_companies(datasource=self)

        return self._companies

    def get_data(self, company, aggregation=None, start_date=None, end_date=None):
        """Get data for this datasource.

        :param company: Company object to filter the datasource on
        :param aggregation: Optional Aggregation object to specify the aggregation of the data
        :param start_date: Optional filter for the start date of the data
        :param end_date: Optional filter for the end date of the data

        :return: pandas DataFrame
        """
        return self._dm.get_data(self, company, aggregation, start_date, end_date)

    def get_dimensions(self, company=None, add_company_info_from_pks=True, **kwargs):
        """Return the dimensions for this data source,
        restricted to `company` (/companies) if given, and filtered by any kwargs items.
        Not memoized, or we'd be holding onto exhausted iterators AND returning them later.

        :param company: a `Company`, a list or tuple of `Company`s, or `None`.
            If not None, a filters dict will be used when getting dimensions,
            and it will have a 'section_pk' key, with value

                company.pk               if company is a `Company`,
                [c.pk for c in company]  if company is a list of `Company`s.

        :param add_company_info_from_pks: This method delegates to
            `self._dm.get_dimensions_for_datasource()`, passing this as the value of
            the keyword parameter of the same name.
            This parameter provides a way to skip the lookup and storage of what can be,
            for some `Datasource`s, a large number of `Company`s.

        :param kwargs: Additional items to filter by, e.g. `category='Banana Republic'`

        :return: a `DimensionSet` object - an iterable through a collection
            of dimension dicts, filtered as requested.
            Each dimension dict has these keys:
            'max_date', 'min_date', 'row_count', 'split_combination'.
            The first two are dates, as strings in ISO format; `'row_count'` is an int;
            `'split_combination'` is a dict, containing keys for this datasource --
            things you can filter for using keyword arguments.

            EXAMPLE

            Assuming `dm` is a DataMonster object, and given this datasource and company:

                datasource = next(dm.get_datasources(query='1010data Credit Sales Index'))
                the_gap = dm.get_company_by_ticker('GPS')

            this call to `get_dimensions`::

                dimset = datasource.get_dimensions( company=the_gap,
                                                    category='Banana Republic' )

            returns an iterable, `dimset`, to this list with just one dimensions dict::

                {'max_date': '2019-06-21',
                 'min_date': '2014-01-01',
                 'row_count': 1998,
                 'split_combination': {'category': 'Banana Republic',
                                       'country': 'US',
                                       'ticker': 'GPS'}}]

            In each 'split_combination' subdict as supplied by Oasis, if there is a
            `'section_pk'` key, its value will be a company primary key (pk, an int),
            or a list of company primary keys, or `None`.

            We add a new key `'ticker'`, whose values are tickers of the companies
            designated by the pk or pk's::

            `dm.get_company_from_pk(pk).ticker`
                if that is not None,

            name of company with key `pk`
                otherwise (actual ticker is `None` or empty)

        :raises: can raise `DataMonsterError` if company is not of an expected type,
            or if some kwarg item is not JSON-serializable.
        """
        filters = kwargs
        if company:
            if isinstance(company, Company):
                filters["section_pk"] = company.pk
            elif isinstance(company, (list, tuple)):
                # loop, rather than `all` and a comprehension, for better error reporting
                pk_list = []
                for cc in company:
                    if not isinstance(cc, Company):
                        raise DataMonsterError(
                            "Every item in `company` argument must be a `Company`; {!r} is not".format(
                                cc
                            )
                        )
                    pk_list.append(cc.pk)
                filters["section_pk"] = pk_list
            else:
                raise DataMonsterError(
                    "company argument must be a `Company`, or a list or tuple of `Company`s"
                )
        add_company_info_from_pks = bool(add_company_info_from_pks)
        return self._dm.get_dimensions_for_datasource(
            self,
            filters=filters,
            add_company_info_from_pks=bool(add_company_info_from_pks),
        )
