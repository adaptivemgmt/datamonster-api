from .base import BaseClass


class Company(BaseClass):
    """Class for a company"""

    _details = None

    def __init__(self, id_, ticker, name, uri, dm):
        self.id = id_
        self.ticker = ticker
        self.name = name
        self.uri = uri
        self.dm = dm

    def __str__(self):
        ticker_str = '({}) '.format(self.ticker) if self.ticker else ''
        return '<{}: {}{}>'.format(self.__class__.__name__, ticker_str, self.name)

    def get_details(self):
        """Get details (metadata) for this company
        """
        return self.dm.get_company_details(self.id)

    @property
    def datasources(self):
        """Get the data sources for this company
        """
        if not hasattr(self, '_datasources'):
            self._datasources = self.dm.get_datasources(company=self)

        return self._datasources

    @property
    def pk(self):
        """
        :return: (int) the primary key (pk)
        """
        return int(self.id)
