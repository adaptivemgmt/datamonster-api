import json
from .base import BaseClass
from .company import Company
from .errors import DataMonsterError
from .utils import summarize_splits_dict

try:                # Py3
    from collections import Iterable
except ImportError:
    from collections.abc import Iterable


class Datasource(BaseClass):
    """Class for a datasource"""

    def __init__(self, id_, name, category, uri, dm):
        self.id = id_
        self.name = name
        self.category = category
        self.uri = uri
        self.dm = dm

    def get_details(self):
        """Get details (metadata) for this datasource
        """
        return self.dm.get_datasource_details(self.id)

    @property
    def companies(self):
        """Return the (memoized) companies for this data source.
        """
        if not hasattr(self, '_companies'):
            self._companies = self.dm.get_companies(datasource=self)

        return self._companies

    def get_data(self, company, aggregation=None, start_date=None, end_date=None):
        """Get data for this datasource.

        :param company: Company object to filter the datasource on
        :param aggregation: Optional Aggregation object to specify the aggregation of the data
        :param start_date: Optional filter for the start date of the data
        :param end_date: Optional filter for the end date of the data

        :return: pandas DataFrame
        """
        return self.dm.get_data(self, company, aggregation, start_date, end_date)

    def get_splits(self, company=None, **kwargs):
        """Return the (memoized) splits for this data source,
        restricted to `company` (/companies) if given, and filtered by any kwargs items.

        :param company: a `Company`, an iterable of `Company`s [list, tuple, ...], or None.
            If not None, a filters dict will be used when getting splits,
            and it will have a 'section_pk' key, with value
                company.pk               if company is a `Company`,
                [c.pk for c in company]  if company is a list of `Company`s.
        :params kwargs: Additional items to filter by, e.g. `category='Banana Republic'`
        :return: splits dict, filtered as requested
        :raises: can raise DataMonsterError if company is not of an expected type,
            or if some kwarg item is not json-encodable
        """
        filters = kwargs
        if company:
            if isinstance(company, Company):
                filters['section_pk'] = company.pk
            elif isinstance(company, Iterable):
                # loop, rather than `all` and a comprehension, for better error reporting
                company_list = []
                for c in company:
                    if not isinstance(c, Company):
                        raise DataMonsterError(
                            'company argument must be a company or a sequence of companies')
                    company_list.append(c)
                filters['section_pk'] = company_list

        # Now do the deed, and memoize
        if not hasattr(self, '_splits'):
            self._splits = {}
        assert isinstance(self._splits, dict)

        try:
            filters_key = json.dumps(filters)
        except:
            self.dm.check_filters_param(filters)    # will raise

        if filters_key not in self._splits:
            self._splits[filters_key] = self.dm.get_splits_for_datasource(self,
                                                                          filters=filters)
        return self._splits[filters_key]

    def get_splits_summary(self, company=None, **kwargs):
        """Return a dict summarizing the splits that would be returned by
                `self.get_splits(company=None, **kwargs)`
        Calling this method memoizes the summarized splits dict, so that a subsequent
        call to
            datasource.get_splits(company, **kwargs)
        doesn't requery, but simply returns the memo.

        Parameters are as for `get_splits`:

        :param company: a `Company`, an iterable of `Company`s [list, tuple, ...], or None.
            If not None, a filters dict will be used when getting splits,
            and it will have a 'section_pk' key, with value
                company.pk               if company is a `Company`,
                [c.pk for c in company]  if company is a list of `Company`s.
        :params kwargs: Additional items to filter by, e.g. `category='Banana Republic'`

        :return: a dict of the form
            { 'split_count': N,
              'columns': { split_col_name_0: set_of_values_for_this_col,
                            ...
                           split_col_name_i: set_of_values_for_this_col,
                           ...
                           'section_pk': {section_pk, ...}
                           ...
                         }
            }
        :raises: can raise DataMonsterError if company is not of an expected type,
            or if some kwarg item is not json-encodable
        """
        splits = self.get_splits(self, company=company, **kwargs)
        return summarize_splits_dict(splits)
