from collections import namedtuple

from .company import Company
from .errors import DataMonsterError


aggregation_periods = {
    'none',
    'month',
    'quarter',
    'fiscalQuarter',
    'year',
}

Aggregation = namedtuple('Aggregation', ['period', 'company'])


def aggregation_sanity_check(aggregation):
    if aggregation.period is not None and aggregation.period not in aggregation_periods:
        raise DataMonsterError('Bad Aggregation Period: {}. Valid choices are: {}'.format(aggregation.period, aggregation_periods))

    if aggregation.company is not None and not isinstance(aggregation.company, Company):
        raise DataMonsterError('Aggregation company must be Company. Found : {}'.format(aggregation.company.__class__))
