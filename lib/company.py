from .base import BaseClass


class Company(BaseClass):
    """Class for a company"""

    _details = None

    def __init__(self, _id, ticker, name, uri, _dm):
        self._id = _id
        self.ticker = ticker
        self.name = name
        self.uri = uri
        self._dm = _dm

    def get_details(self):
        return self._dm.get_company_details(self._id)

    @property
    def datasources(self):
        """Get the data sources for this company"""

        if not hasattr(self, '_datasources'):
            self._datasources = self._dm.get_datasources(company=self)

        return self._datasources
