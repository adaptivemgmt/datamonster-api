import pytest

from datamonster_api import DataMonsterError


def assert_object_matches_data_group(data_group, data_group_obj):
    assert data_group_obj["_id"] == data_group.id
    assert data_group_obj["name"] == data_group.name
    assert len(data_group_obj["columns"]) == len(data_group.columns)
    dg_col_name_to_type = {col.name: col.type_ for col in data_group.columns}
    for col in data_group_obj["columns"]:
        assert col["name"] in dg_col_name_to_type
        assert col["type_"] == dg_col_name_to_type[col["name"]]

