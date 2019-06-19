name = "datamonster_api"

from .lib.datamonster import DataMonster    # noqa
from .lib.aggregation import Aggregation    # noqa
# Note - added these imports, which 'export' Company, Datasource, Client
# so users can say, e.g.
#   from datamonster_api import Company
# rather than
#   from datamonster_api.lib.company import Company
from .lib.company import Company            # noqa
from .lib.datasource import Datasource      # noqa
from .lib.client import Client              # noqa
