import pytest

from datamonster_api import DataMonsterError, Datasource


class __NoCanSerialize(object):
    def __repr__(self):
        return "Object of type __NoCanSerialize"


def _assert_equal_dimension_dicts(expected, mocked):
    assert expected["split_combination"] == mocked["splitCombination"]
    assert expected["max_date"] == mocked["maxDate"]
    assert expected["min_date"] == mocked["minDate"]
    assert expected["row_count"] == mocked["rowCount"]


def _assert_dict_and_DimensionSet_metadata_match(resp_dict, dim_set):
    assert resp_dict["dimensionCount"] == len(dim_set)
    assert resp_dict["maxDate"] == dim_set.max_date
    assert resp_dict["minDate"] == dim_set.min_date
    assert resp_dict["rowCount"] == dim_set.row_count


def test_filters_param_not_dict(dm, datasource):
    """
    """
    with pytest.raises(DataMonsterError) as excinfo:
        dm.get_dimensions_for_datasource(datasource, filters=[0, 1, 2])

    assert "`filters` must be a dict, got list instead" in excinfo.value.args


def test_filters_param_not_json_serializable(dm, datasource):
    """
    """
    with pytest.raises(DataMonsterError) as excinfo:
        dm.get_dimensions_for_datasource(
            datasource, filters={"somekey": __NoCanSerialize()}
        )

    errtext = (
        "Problem with filters when getting dimensions: Object of type __NoCanSerialize is not JSON serializable",
    )
    assert errtext == excinfo.value.args


def test_get_dimensions_for_datasource_single_page(
    mocker, dm, single_page_dimensions_result, datasource
):
    """Test getting dimensions. single page"""

    # The resulting dimensions should always be the same
    def assert_results_good(dimensions):
        dimensions = list(dimensions)
        assert len(dimensions) == 3

        _assert_equal_dimension_dicts(
            dimensions[0], single_page_dimensions_result["results"][0]
        )
        _assert_equal_dimension_dicts(
            dimensions[1], single_page_dimensions_result["results"][1]
        )
        _assert_equal_dimension_dicts(
            dimensions[2], single_page_dimensions_result["results"][2]
        )

    # No filters
    dm.client.get = mocker.Mock(return_value=single_page_dimensions_result)
    dm.client.get.reset_mock()
    dimensions = dm.get_dimensions_for_datasource(datasource)

    _assert_dict_and_DimensionSet_metadata_match(
        single_page_dimensions_result, dimensions
    )
    assert_results_good(dimensions)

    assert dm.client.get.call_count == 1
    assert dm.client.get.call_args[0][0] == "/rest/v1/datasource/{}/dimensions".format(
        datasource.id
    )


def test_get_dimensions_for_datasource_multi_page(
    mocker, dm, multi_page_dimensions_results, datasource
):
    """Test getting dimensions. multi-page"""

    dm.client.get = mocker.Mock(side_effect=multi_page_dimensions_results)
    dimensions = dm.get_dimensions_for_datasource(datasource)
    _assert_dict_and_DimensionSet_metadata_match(
        multi_page_dimensions_results[0], dimensions
    )
    _assert_dict_and_DimensionSet_metadata_match(
        multi_page_dimensions_results[1], dimensions
    )

    dimensions = list(dimensions)

    assert dm.client.get.call_count == 2

    assert dm.client.get.call_args_list[0][0][
        0
    ] == "/rest/v1/datasource/{}/dimensions".format(datasource.id)
    assert (
        dm.client.get.call_args_list[1][0][0]
        == multi_page_dimensions_results[0]["pagination"]["nextPageURI"]
    )

    _assert_equal_dimension_dicts(
        dimensions[0], multi_page_dimensions_results[0]["results"][0]
    )
    _assert_equal_dimension_dicts(
        dimensions[1], multi_page_dimensions_results[0]["results"][1]
    )
    _assert_equal_dimension_dicts(
        dimensions[2], multi_page_dimensions_results[1]["results"][0]
    )


# ------------------------------------------------------
# Tests for `Datasource.get_dimensions`
# ------------------------------------------------------


def test_ds_get_dimensions_bad_company(datasource):
    """
    `company` arg neither a `Company`, nor an `Iterable` of `Company`s, nor None
    """
    with pytest.raises(DataMonsterError) as excinfo:
        datasource.get_dimensions(company="string pertaining to a company")

    assert (
        "company argument must be a `Company`, or a list or tuple of `Company`s"
        in excinfo.value.args
    )


def test_ds_get_dimensions_non_company_in_list(
    datasource, company_with_int_id, other_company_with_int_id
):
    """
    `company`an `Iterable` with an element that's not a `Company`
    """
    with pytest.raises(DataMonsterError) as excinfo:
        datasource.get_dimensions(
            company=[company_with_int_id, other_company_with_int_id, (1, 2, 3)]
        )

    assert (
        "Every item in `company` argument must be a `Company`; (1, 2, 3) is not"
        in excinfo.value.args
    )


def test_ds_get_dimensions_bad_kwarg(mocker, dm, company_with_int_id):
    """
    :param company: a `Company`, an `Iterable` of `Company`s, or None
    """
    new_datasource = Datasource("id", "name", "category", "uri", dm)

    with pytest.raises(DataMonsterError) as excinfo:
        new_datasource.get_dimensions(
            company=company_with_int_id, widget=__NoCanSerialize()
        )

    errtext = (
        "Problem with filters when getting dimensions: Object of type __NoCanSerialize is not JSON serializable",
    )
    assert errtext == excinfo.value.args
