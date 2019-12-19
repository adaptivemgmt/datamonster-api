from collections import namedtuple

from .company import Company
from .errors import DataMonsterError


aggregation_periods = {"week", "month", "quarter", "fiscalQuarter", "year"}
Aggregation = namedtuple("Aggregation", ["period", "company"])

Aggregation.__doc__ = """
A representation of an aggregation type within DataMonster

:param str period: The period of time over which to aggregate. Valid choices are:
    {}
:param company: The ``Company`` object for which fiscal quarters are calculated.
    Only relevant for the `fiscalQuarter` aggregation period.
""".format(
    aggregation_periods
)


def aggregation_sanity_check(aggregation, company=None):
    if aggregation.period is not None and aggregation.period not in aggregation_periods:
        raise DataMonsterError(
            "Bad Aggregation Period: {}. Valid choices are: {}".format(
                aggregation.period, aggregation_periods
            )
        )

    if aggregation.company is not None and not isinstance(aggregation.company, Company):
        raise DataMonsterError(
            "Aggregation company must be Company. Found : {}".format(
                aggregation.company.__class__
            )
        )

    if aggregation.period == "fiscalQuarter":
        if aggregation.company is None:
            raise DataMonsterError(
                "Company must be specified for a fiscalQuarter aggregation"
            )

        if company and aggregation.company.id != company.id:
            raise DataMonsterError(
                "Aggregating by the fiscal quarter of a different "
                "company not yet supported"
            )
