==========================
Datamonster Python Library
==========================

This library eases the use of the Datamonster REST API from Python

Quickstart
----------

Working with companies

.. code-block:: python

        from lib.datamonster import DataMonster
        dm = DataMonster(<key_id>, <secret_key>)

        print(list(dm.get_companies(query='hd')))   # Prints all companies whose name or ticker matches 'hd'

        apple = dm.get_company_by_ticker('aapl')    # Creates a company object for apple

        print(apple.quarters[:5])                   # prints first 5 quarter end dates
        print(list(apple.datasources)[:5])          # prints the first 5 datasources that cover apple


Working with data sources

.. code-block:: python

        print(list(dm.get_datasources(query='1010')))   # Prints all data sources whose name or provider matches '1010'

        print(list(                                     # Prints all data sources whose name or provider matches '1010'
            dm.get_datasources(                         # AND also cover apple
                query='1010',
                company=apple)
            )
        )

        datasource = list(dm.get_datasources(query='1010 Debit Sales Index'))[0]

        print(list(datasource.companies)[:5])          # Prints the first 5 companies covered by `1010 Debit Sales Index`


Getting data

.. code-block:: python

        import datetime
        from lib.aggregation import Aggregation

        apple = dm.get_company_by_ticker('aapl')
        datasource = next(apple.datasources)        # Gets a datasource object
        datasource.get_data(apple)                  # Gets all data for the datasource filtering on apple

        agg = Aggregation(period='fiscalQuarter', company=apple)

        datasource.get_data(                        # Gets all data for the given datasource filtered by apple, 
            apple,                                  # aggregated by apple's fiscal quarter, and starting on
            agg,                                    # January 1, 2017 (inclusive)
            start_date=datetime.date(2017, 1, 1)
        )
