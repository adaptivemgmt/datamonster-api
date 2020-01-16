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


def test_to_time_aggregation_dictionary_1():
    """Test time aggregation dictionary -- no company"""

    # no agg type specified
    expected = {
        'cadence': 'monthly',
        'aggregationType': 'sum',
        'includePTD': False
    }

    agg = Aggregation(period="month", company=None)
    agg_dict = agg.to_time_aggregation_dictionary()
    assert agg_dict == expected

    # agg type specified
    expected = {
        'cadence': 'monthly',
        'aggregationType': 'mean',
        'includePTD': False
    }

    agg = Aggregation(period="month", company=None)
    agg_dict = agg.to_time_aggregation_dictionary('mean')
    assert agg_dict == expected


def test_to_time_aggregation_dictionary_2(company):
    """Test time aggregation dictionary -- include company"""

    # cadence is monthly
    expected = {
        'cadence': 'monthly',
        'aggregationType': 'sum',
        'includePTD': False
    }

    agg = Aggregation(period="month", company=company)
    agg_dict = agg.to_time_aggregation_dictionary()
    assert agg_dict == expected

    # cadence is fiscal quarterly
    expected = {
        'cadence': 'fiscal quarterly',
        'aggregationType': 'sum',
        'includePTD': False,
        'sectionPk': company.id
    }

    agg = Aggregation(period="fiscalQuarter", company=company)
    agg_dict = agg.to_time_aggregation_dictionary()
    assert agg_dict == expected
