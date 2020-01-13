from .base import BaseClass


class Company(BaseClass):
    """Representation of a company in DataMonster

    :param _id: (str) unique internal identifier for the company
    :param ticker: (str) ticker of the company
    :param name: (str) name of the company
    :param uri: (str) DataMonster resource identifier associated with the company
    :param dm: ``DataMonster`` object

    *property* **ticker**
        **Returns:** (str) company ticker, ``None`` if company is private
    *property* **name**
        **Returns:** (str) company name, including the associated vendor
    *property* **quarters**
        **Returns:** (list) list of company quarter dates, including 4 projected dates.
            Empty if company is private
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

    def __hash__(self):
        return int(self.id)

    def __eq__(self, obj):
        return isinstance(obj, Company) and self.id == obj.id

    @property
    def pk(self):
        """
        :return: (int) the unique internal identifier for the company (corresponds to ``_id``)
        """
        return int(self.id)

    @property
    def datasources(self):
        """
        :return: (iter) iterable of ``Datasource`` objects
            associated with this company to which the user has access, memoized
        """
        if not hasattr(self, "_datasources"):
            self._datasources = self.dm.get_datasources(company=self)

        return self._datasources

    def get_details(self):
        """
        Get details (metadata) for this company,
        providing basic information as stored in DataMonster

        :return: (dict)
        """
        return self.dm.get_company_details(self.id)
