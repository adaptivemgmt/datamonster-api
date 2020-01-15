from .company import Company
from .errors import DataMonsterError


class Aggregation(object):
    """A representation of an aggregation type within DataMonster"""

    aggregation_period_map = {
        "fiscalQuarter": "fiscal quarterly",
        "quarter": "calendar quarterly",
        "month": "monthly",
        "week": "weekly",
    }

    def __init__(self, period, company):
        """Initialize an Aggregation

        :param str period: The period of time over which to aggregate. Valid choices are:
            "week", "month", "quarter", "fiscalQuarter", "year"
        :param company: The ``Company`` object for which fiscal quarters are calculated.
            Only relevant for the `fiscalQuarter` aggregation period.
        """

        self.period = period
        self.company = company
        self.aggregation_sanity_check()

    def to_time_aggregation_dictionary(self, aggregation_type='sum'):
        """Used to extract the time aggregation dictionary for API usage from the Aggregation"""
        agg_dict = {
            'cadence': self.aggregation_period_map[self.period],
            'aggregationType': aggregation_type,
            'includePTD': False
        }
        if self.period == 'fiscalQuarter':
            agg_dict['section_pk'] = self.company.id

        return agg_dict

    def aggregation_sanity_check(self, company=None):
        if self.period is not None and self.period not in self.aggregation_period_map:
            raise DataMonsterError(
                "Bad Aggregation Period: {}. Valid choices are: {}".format(
                    self.period, self.aggregation_period_map.keys()
                )
            )

        if self.company is not None and not isinstance(self.company, Company):
            raise DataMonsterError(
                "Aggregation company must be Company. Found : {}".format(
                    self.company.__class__
                )
            )

        if self.period == "fiscalQuarter":
            if self.company is None:
                raise DataMonsterError(
                    "Company must be specified for a fiscalQuarter aggregation"
                )

            if company and self.company.id != company.id:
                raise DataMonsterError(
                    "Aggregating by the fiscal quarter of a different "
                    "company not yet supported"
                )
