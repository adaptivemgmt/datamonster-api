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

        # TODO: What if the memoized data, here & for splits, gets stale?
        #       However uncommon that might be.
        # todo: Perhaps offer a way to "clear the cache", force a refetch?

        if not hasattr(self, '_companies'):
            self._companies = self.dm.get_companies(datasource=self)

        return self._companies

    def get_data(self, company, aggregation=None, start_date=None, end_date=None):
        return self.dm.get_data(self, company, aggregation, start_date, end_date)


# ##### Provisional / ideas:
    @property
    def splits(self):
        """Get the (memoized) splits for this data source.

        Returns (dict or None):
            for Oasis data fountains, a dict of all splits for this data fountain.
            For Legacy `Datasource`s, this method returns `None`.
        """

        if not hasattr(self, '_companies'):
            self._splits = self.dm.get_splits(datasource=self)

        return self._splits
