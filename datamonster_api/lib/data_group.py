from .base import BaseClass
from .errors import DataMonsterError
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

    def get_details(self):
        """
        Get details (metadata) for this data group,
        providing basic information as stored in DataMonster

        :return: (dict)
        """
        return self.dm.get_data_group_details(self.id)

    @staticmethod
    def _get_dgctype_(column):
        if hasattr(column, 'str') and column.str.match(date_regex).any():
            return 'date'
        elif np.issubdtype(column, np.number):
            return 'number'
        else:
            return 'string'

    @staticmethod
    def _construct_error_message(missing, extras, bad_dates):
        msg = 'Invalid DataFrame Schema:\n'
        if missing:
            msg = msg + '  DataGroup could not find the following column{}:\n'.format(
                's' if len(missing) > 1 else ''
            )
            for miss in missing:
                msg = msg + '    name: "{}", type: {}\n'.format(miss.name, miss.type_)
        if extras:
            msg = msg + '  DataGroup was not expecting the following column{}:\n'.format(
                's' if len(extras) > 1 else ''
            )
            for extra in extras:
                msg = msg + '    name: "{}", type: {}\n'.format(extra.name, extra.type_)
        if bad_dates:
            msg = msg + '  The following column{} expected to contain only YYYY-MM-DD dates but did not:\n'.format(
                's were' if len(bad_dates) > 1 else ' was'
            )
            for bad_date in bad_dates:
                msg = msg + '    name: "{}", type: {}\n'.format(bad_date.name, bad_date.type_)
        return msg

    def _validate_schema(self, df):
        """Check if the schema of a provided pandas dataframe matches the expected columns"""
        missing = []
        extra = []
        bad_dates = []

        # Find missing columns
        for col in self.columns:
            if not col._exists_in_df(df):
                missing.append(col)

        # Find extra columns
        if len(df.columns) + len(missing) != len(self.columns):
            col_names_totype_s = {c.name: c.type_ for c in self.columns}
            for col in df.columns:
                dgctype_ = self._get_dgctype_(df[col])
                if col not in col_names_totype_s or dgctype_ != col_names_totype_s[col]:
                    extra.append(DataGroupColumn(col, dgctype_))

        # Verify date columns are complete
        date_columns = [col for col in self.columns if col.type_ == 'date' and col not in missing]
        for column in date_columns:
            if not hasattr(df[column.name], 'str') or not df[column.name].str.match(date_regex).all():
                bad_dates.append(column)

        if missing or extra or bad_dates:
            raise DataMonsterError(self._construct_error_message(missing, extra, bad_dates))


class DataGroupColumn(object):
    """Representation of a DataGroupColumn in DataMonster

    :param name: (str) name of the DataGroupColumn
    :param type_: (enum 'string', 'number' or 'date') expected data type of the column
    """

    def __init__(self, name=None, type_=None):

        self.name = name
        self.type_ = type_

    def __repr__(self):
        return "<{}: {}>".format(self.__class__.__name__, self.name)

    def _exists_in_df(self, df):
        """Return true if this DataGroupColumn is represented in ``df``

        :param df: Pandas Data Frame
        :return: True iff a column exists in ``df`` that matches self.name and self.type_
        """

        if self.name in df.columns:
            column = df[self.name]
            if self.type_ == 'string':
                return True
            elif self.type_ == 'number':
                return np.issubdtype(column, np.number)
            elif self.type_ == 'date':
                return column.str.match(date_regex).any()
