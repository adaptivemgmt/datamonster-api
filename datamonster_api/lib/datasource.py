import json
from .base import BaseClass


class Datasource(BaseClass):
    """Class for a datasource"""

    def __init__(self, id_, name, category, uri, dm):
        self.id = id_
        self.name = name
        self.category = category
        self.uri = uri
        self.dm = dm

    def get_details(self):
        return self.dm.get_datasource_details(self.id)

    @property
    def companies(self):
        """Return the (memoized) companies for this data source.
        """
        # NOTE: corrected & upgraded the docstring. -BTO

        if not hasattr(self, '_companies') or self.dm.always_query:
            self._companies = self.dm.get_companies(datasource=self)

        return self._companies

    def get_data(self, company, aggregation=None, start_date=None, end_date=None):
        return self.dm.get_data(self, company, aggregation, start_date, end_date)

    def get_splits(self, filters=None):
        """Get the splits for this data source. These are memoized per `filters`.

        :returns: (dict)
            for Oasis data fountains, a dict of all splits for this data fountain;
            for Legacy `Datasource`s, this method returns {}.
        """
        if not hasattr(self, '_splits'):
            self._splits = {}
        assert isinstance(self._splits, dict)
        filters_key = json.dumps(filters)
        if filters_key not in self._splits or self.dm.always_query:
            self._splits[filters_key] = self.dm.get_splits_for_datasource(
                self, filters=filters)
        return self._splits[filters_key]
