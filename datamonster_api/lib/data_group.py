from .base import BaseClass
import numpy as np

date_regex = r'\d{4}-\d{2}-\d{2}'


class DataGroup(BaseClass):
    """Representation of a DataGroup in DataMonster

    :param _id: (int) unique internal identifier for the Data Group
    :param name: (str) name of the Data Group
    :param columns: (list of ``DataGroupColumn`` objects) representing columns of uploaded data
    :param dm: ``DataMonster`` object
    """

    def __init__(self, _id, name, columns, dm):
        self.id = _id
        self.name = name
        self.columns = columns
        self.dm = dm

    def __hash__(self):
        return hash(self.id)

    def __eq__(self, obj):
        return isinstance(obj, DataGroup) and self.id == obj.id

    def _get_dgc_type(self, column):
        if hasattr(column, 'str') and column.str.match(date_regex).any():
            return 'date'
        elif np.issubdtype(column, np.number):
            return 'number'
        else:
            return 'string'

    def _validate_schema(self, df):
        """Check if the schema of a provided pandas dataframe matches the expected columns"""
        missing = []
        extra = []

        for col in self.columns:
            if not col._exists_in_df(df):
                missing.append(col)

        if len(df.columns) + len(missing) != len(self.columns):
            col_names_to_types = {c.name: c._type for c in self.columns}
            for col in df.columns:
                dgc_type = self._get_dgc_type(df[col])
                if col not in col_names_to_types or dgc_type != col_names_to_types[col]:
                    extra.append(DataGroupColumn(col, dgc_type))

        print(missing, extra)


class DataGroupColumn(BaseClass):
    """Representation of a DataGroupColumn in DataMonster

    :param name: (str) name of the DataGroupColumn
    :param _type: (enum 'string', 'number' or 'date') expected data type of the column
    """

    def __init__(self, name=None, _type=None):

        self.name = name
        self._type = _type

    def _exists_in_df(self, df):

        if self.name in df.columns:
            column = df[self.name]
            if self._type == 'string':
                return True
            elif self._type == 'number':
                return np.issubdtype(column, np.number)
            elif self._type == 'date':
                return column.str.match(date_regex).any()
