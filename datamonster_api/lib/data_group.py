from .base import BaseClass
from .errors import DataMonsterError
import numpy as np
from io import BytesIO
from .utils import dataframe_to_avro_bytes

date_regex = r'\d{4}-\d{2}-\d{2}'


class DataGroup(BaseClass):
    """Representation of a DataGroup in DataMonster

    :param _id: (int) unique internal identifier for the Data Group
    :param name: (str) name of the Data Group
    :param columns: (list of ``DataGroupColumn`` objects) representing columns of uploaded data
    :param status: (str, enum) `success` if all Data Sources in the group have successfully loaded
        `processing` if any DataSource in the group is still processing
        `error` if any DataSource in the group is in an error state
        Note: `error` takes precedence over `processing`
    :param dm: ``DataMonster`` object
    """

    def __init__(self, _id, name, columns, status, dm):
        self.id = _id
        self.name = name
        self.columns = columns
        self.status = status
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

    def start_data_refresh(self, data_frame):
        avro_file = BytesIO(dataframe_to_avro_bytes(data_frame, 'upload_data', 'com.adaptivemgmt.upload'))
        files = {'avro_file': avro_file}
        headers = {'Accept': 'avro/binary'}
        try:
            response = self.dm.client.post(self._get_refresh_url(), {}, headers=headers, files=files)
            print(response)
        except Exception as err:
            print(err)

    def _get_refresh_url(self):
        return '{}/refresh'.format(self.dm._get_data_group_path(self.id))

    @staticmethod
    def _get_dgctype_(column):
        if hasattr(column, 'str') and column.str.match(date_regex).any():
            return 'date'
        elif np.issubdtype(column, np.number):
            return 'number'
        elif np.issubdtype(column, np.object_) or np.issubdtype(column, np.str_):
            return 'string'

    @staticmethod
    def _construct_error_message(missing, extras, bad_dates):
        msg = ['Invalid DataFrame Schema:']
        if missing:
            msg.append('  DataGroup could not find the following column{}:'.format(
                's' if len(missing) > 1 else ''
            ))
            for miss in missing:
                msg.append('    name: "{}", type: {}'.format(miss.name, miss.type_))
        if extras:
            msg.append('  DataGroup was not expecting the following column{}:'.format(
                's' if len(extras) > 1 else ''
            ))
            for extra in extras:
                msg.append('    name: "{}", type: {}'.format(extra.name, extra.type_))
        if bad_dates:
            msg.append('  The following column{} expected to contain only YYYY-MM-DD dates but did not:'.format(
                's were' if len(bad_dates) > 1 else ' was'
            ))
            for bad_date in bad_dates:
                msg.append('    name: "{}", type: {}\n'.format(bad_date.name, bad_date.type_))
        return '\n'.join(msg)

    def _validate_schema(self, df):
        """Check if the schema of a provided pandas dataframe matches the expected columns"""
        extra = []

        # Find missing columns
        missing = [col for col in self.columns if not col._exists_in_df(df)]

        # Find extra columns
        if len(df.columns) + len(missing) != len(self.columns):
            col_names_totype_s = {c.name: c.type_ for c in self.columns}
            for col in df.columns:
                dgctype_ = self._get_dgctype_(df[col])
                if col not in col_names_totype_s or dgctype_ != col_names_totype_s[col]:
                    extra.append(DataGroupColumn(col, dgctype_))

        # Verify date columns are complete
        date_columns = [col for col in self.columns if col.type_ == 'date' and col not in missing]
        bad_dates = [column for column in date_columns if
                     not hasattr(df[column.name], 'str') or
                     not df[column.name].str.match(date_regex).all()]

        return missing, extra, bad_dates

    def _accepts(self, df):
        """Check if DataGroup could run a refresh with the given data frame"""
        missing, extra, bad_dates = self._validate_schema(df)
        if missing or bad_dates:
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
                # pandas maps strings to objects
                return np.issubdtype(column, np.object_) or np.issubdtype(column, np.str_)
            elif self.type_ == 'number':
                return np.issubdtype(column, np.number)
            elif self.type_ == 'date':
                return column.str.match(date_regex).any()
            else:
                raise DataMonsterError('Unrecognized column type of column {}'.format(self.name))
