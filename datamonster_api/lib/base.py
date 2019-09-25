class BaseClass(object):

    _details = None

    def __repr__(self):
        return "<{}: {}>".format(self.__class__.__name__, self.name)

    def __getattr__(self, name):
        """
        NOTE: this allows us to add properties in the DataMonster rest-api endpoint
        without making changes in the client library to support those changes
        """
        if not self._details:
            self._details = self.get_details()
        if name in self._details:
            return self._details[name]
        else:
            raise AttributeError

    def set_details(self, details):
        self._details = details

    def get_details(self):
        raise NotImplementedError
