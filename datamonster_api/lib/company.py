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

    def __init__(self, params, dm):
        self._dm = dm
        self._params = params
        self._id = params["id"]
        self._uri = params["uri"]
        self.ticker = params["ticker"]
        self.name = params["name"]
        self.quarters = params.get("quarters", [])

    def __repr__(self):
        ticker_str = "[{}] ".format(self.ticker) if self.ticker else ""
        return "<{}: {}{}>".format(self.__class__.__name__, ticker_str, self.name)

    def get_details(self):
        """Get details (metadata) for this company

        :return: (dict) details
        """
        return self._dm.get_company_details(self._id)

    @property
    def datasources(self):
        """Get the data sources for this company

        :return: (iter) iterable of Datasource objects
        """
        if not hasattr(self, "_datasources"):
            self._datasources = self._dm.get_datasources(company=self)

        return self._datasources

    @property
    def section_type(self):
        """
        :return: (str) the section type
        """
        return self._params["type"]

    @property
    def pk(self):
        """
        :return: (int) the primary key (pk)
        """
        return int(self._id)
