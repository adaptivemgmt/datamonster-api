from .base import BaseClass
from .company import Company
from .errors import DataMonsterError


class Datasource(BaseClass):
    """Representation of a data source in Data Monster

    :param _id: (str) uniquer internal identifier for the data source
    :param name: (dict) name of the data source, including the vendor for the data
    :param category: (list) associated categories
    :param uri: (str) Data Monster resource identifier associated with the data source
    :param dm: ``DataMonster`` object

    *property* **name**
        **Returns** (str) name of data source, including vendor
    *property* **category**
        **Returns** (str) category associated with the data source, e.g.,
        `Web Scrape Data` or `Uploaded Data`
    """

    def __init__(self, _id, name, category, uri, dm):
        self.id = _id
        self.name = name
        self.category = category
        self.uri = uri
        self.dm = dm

    def __hash__(self):
        return hash(self.id)

    def __eq__(self, obj):
        return isinstance(obj, Datasource) and self.id == obj.id

    def get_details(self):
        """
        Get details (metadata) for this data source,
        providing basic information as stored in Data Monster

        :return: (dict)
        """
        return self.dm.get_datasource_details(self.id)

    @property
    def companies(self):
        """
        :return: (iter) iterable of ``Company`` objects associated with this data source, memoized
        """
        if not hasattr(self, "_companies"):
            self._companies = self.dm.get_companies(datasource=self)

        return self._companies

    def get_data(self, company, aggregation=None, start_date=None, end_date=None):
        """Get data for this data source.

        :param company: ``Company`` object to filter the data source on
        :param aggregation: Optional ``Aggregation`` object to specify the aggregation of the data
        :param start_date: Optional string to act as a filter for the start date of the data; accepted formats include:
            YYYY-MM-DD, MM/DD/YYYY, or pandas or regular ``datetime`` object
        :param end_date: Optional string to act as a filter for the end date of the data; accepted formats include:
            YYYY-MM-DD or MM/DD/YYYY, or pandas or regular ``datetime`` object
        :return: pandas.DataFrame
        """
        return self.dm.get_data(self, company, aggregation, start_date, end_date)

    def get_dimensions(self, company=None, add_company_info_from_pks=True, **kwargs):
        """Return the dimensions for this data source,
            restricted to the given company or companies and filtered by any kwargs items. Not memoized.

        :param company: a ``Company`` object, a list or tuple of ``Company`` objects, or ``None``.
            If not ``None`` the return value will only include rows corresponding to the given companies.
        :param add_company_info_from_pks: Determines whether return value will include tickers for
            the returned companies. If ``False``, only ``section_pk`` s will be returned.
        :param kwargs: Additional items to filter by, e.g. ``category='Banana Republic'``

        :return: a ``DimensionSet`` object - an iterable through a collection
            of dimension dicts, filtered as requested. See `this documentation <api.html#datamonster_api.DimensionSet>`_
            for more info.

        See `here <examples.html#get-dimensions-for-datasource>`_
        for example usage of a similar function.

        :raises: can raise ``DataMonsterError`` if company is not of an expected type,
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
        return self.dm.get_dimensions_for_datasource(
            self,
            filters=filters,
            add_company_info_from_pks=bool(add_company_info_from_pks),
        )
