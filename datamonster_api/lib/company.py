from .base import BaseClass


class Company(BaseClass):
    """Company object which represents a company in DataMonster

    :param params: (dict)
    :param dm: DataMonster object

    Attributes include:
      ticker: (str) company ticker
      name: (str) company name
      quarters: (list) list of company quarter dates
    """

    _details = None

    def __init__(self, _id, ticker, name, uri, dm):
        self.id = _id
        self.ticker = ticker
        self.name = name
        self.uri = uri
        self.dm = dm

    def __repr__(self):
        ticker_str = "[{}] ".format(self.ticker) if self.ticker else ""
        return "<{}: {}{}>".format(self.__class__.__name__, ticker_str, self.name)

    def __eq__(self, obj):
        return isinstance(obj, Company) and self.get_details() == obj.get_details()

    def get_details(self):
        """Get details (metadata) for this company

        :return: (dict) details
        """
        return self.dm.get_company_details(self.id)

    @property
    def datasources(self):
        """Get the data sources for this company

        :return: (iter) iterable of Datasource objects
        """
        if not hasattr(self, "_datasources"):
            self._datasources = self.dm.get_datasources(company=self)

        return self._datasources

    @property
    def pk(self):
        """
        :return: (int) the primary key (pk)
        """
        return int(self.id)
