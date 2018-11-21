class BaseClass(object):
    """Base of Datasource and Company"""

    _details = None

    def __repr__(self):
        return '<{}: {}>'.format(self.__class__.__name__, self.name)

    def __getattr__(self, name):
        if not self._details:
            self._details = self.get_details()
        if name in self._details:
            return self._details[name]
        else:
            raise AttributeError

    def set_details(self, details):
        self._details = details

    def get_details(self):
        raise NotImplemented()
