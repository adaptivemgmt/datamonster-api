from .base import BaseClass


class DataGroup(BaseClass):
    """Representation of a DataGroup in DataMonster

    :param _id: (int) unique internal identifier for the Data Group
    :param name: (str) name of the Data Group
    :param columns: (list of ``DataGroupColumn`` objects) representing columns of uploaded data
    """

    def __init__(self, _id, name, columns, dm):
        self.id = _id
        self.name = name
        self.columns = columns
        self.dm = dm

