from base import BaseClass


class Datasource(BaseClass):
    """Class for a datasource"""

    def __init__(self, _id, name, category, uri, dm):
        self._id = _id
        self.name = name
        self.category = category
        self.uri = uri
        self.dm = dm

    def get_details(self):
        return self.dm.get_datasource_details(self._id)

    @property
    def companies(self):
        """Get the data sources for this company"""

        if not hasattr(self, '_companies'):
            self._companies = self.dm.get_companies(datasource=self)

        return self._companies

    def get_data(self, company, aggregation=None, start_date=None, end_date=None):
        return self.dm.get_data(self, company, aggregation=None, start_date=None, end_date=None)
