from .base import BaseClass


class Datasource(BaseClass):
    """Class for a datasource"""

    def __init__(self, _id, name, category, uri, _dm):
        self._id = _id
        self.name = name
        self.category = category
        self.uri = uri
        self.dm = _dm

    def get_details(self):
        return self._dm.get_datasource_details(self._id)

    @property
    def companies(self):
        """Get the data sources for this company"""

        if not hasattr(self, '_companies'):
            self._companies = self._dm.get_companies(datasource=self)

        return self._companies

    def get_data(self, company, aggregation=None, start_date=None, end_date=None):
        return self._dm.get_data(self, company, aggregation, start_date, end_date)
