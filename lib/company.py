class Company(object):
    """Class for a company"""

    def __init__(self, _id, ticker, name, uri, dm):
        self._id = _id
        self.ticker = ticker
        self.name = name
        self.uri = uri
        self.dm = dm
