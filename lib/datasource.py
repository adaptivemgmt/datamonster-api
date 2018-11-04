class Datasource(object):
    """Class for a datasource"""

    def __init__(self, _id, name, category, uri, dm):
        self._id = _id
        self.name = name
        self.category = category
        self.uri = uri
        self.dm = dm
