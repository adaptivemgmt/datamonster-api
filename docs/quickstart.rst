.. _quickstart:

Quickstart
----------

Installing the Datamonster library:

.. code::

        pip install datamonster_api

Working with companies:

.. code::

        from datamonster_api import DataMonster
        dm = DataMonster(<key_id>, <secret_key>)

        # Prints all companies whose name or ticker matches 'hd'
        print(list(dm.get_companies(query='hd')))

        # Creates a company object for apple
        apple = dm.get_company_by_ticker('aapl')

        # prints the first 5 quarter end dates
        print(apple.quarters[:5])

        # prints the first 5 data sources that cover apple
        print(list(apple.datasources)[:5])


Working with data sources:

.. code::

        # Prints all data sources whose name or provider matches 'fake'
        print(list(dm.get_datasources(query='fake')))

        # Prints all data sources whose name or provider matches 'fake'
        # AND also cover apple
        print(list(dm.get_datasources(query='fake', company=apple)))


        # Prints first 5 companies covered by `Fake Data Source`
        datasource = list(
                dm.get_datasources(query='Fake Data Source')
                )[0]

        print(list(datasource.companies)[:5])


Getting data:

.. code::

        import datetime
        from datamonster_api import Aggregation

        # Gets a datasource object
        apple = dm.get_company_by_ticker('aapl')
        datasource = next(apple.datasources)

        # Gets all data for the datasource filtering on apple
        datasource.get_data(apple)

        agg = Aggregation(period='fiscalQuarter', company=apple)

        # Gets all data for the given data source filtered by apple,
        # aggregated by apple's fiscal quarter, and starting on
        # January 1, 2017 (inclusive)
        datasource.get_data(
            apple,
            agg,
            start_date=datetime.date(2017, 1, 1)
        )
