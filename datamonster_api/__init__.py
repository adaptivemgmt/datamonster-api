name = "datamonster_api"

from .lib.datamonster import DataMonster    # noqa
from .lib.aggregation import Aggregation    # noqa
# Added these imports, which 'export' Company, Datasource
# so users can say, e.g.
#   from datamonster_api import Company
# rather than
#   from datamonster_api.lib.company import Company
# But Client is not so exposed.
from .lib.company import Company            # noqa
from .lib.datasource import Datasource      # noqa

# And finally, as a convenience,
from .lib.errors import DataMonsterError
