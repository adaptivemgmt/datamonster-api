import pytest

from datamonster_api import Aggregation, DataMonsterError


def test_bad_aggregations():
    """Test that bad aggregations are rejected on init"""

    # ** aggregation period is invalid
    with pytest.raises(DataMonsterError) as excinfo:
        Aggregation(period=123, company=None)

    assert "Bad Aggregation Period" in excinfo.value.args[0]

    # ** aggregation company is invalid
    with pytest.raises(DataMonsterError) as excinfo:
        Aggregation(period="month", company=123)

    assert "Aggregation company must be Company" in excinfo.value.args[0]

    # ** fiscal quarter aggregation -- no company
    with pytest.raises(DataMonsterError) as excinfo:
        Aggregation(period="fiscalQuarter", company=None)

    assert (
        "Company must be specified for a fiscalQuarter aggregation"
        in excinfo.value.args[0]
    )
