Examples
--------

Get Data Raw
^^^^^^^^^^^^

Initialize a ``DataMonster`` object:

..  code::

    dm = DataMonster(<key_id>, <secret_key>)

Initialize a ``Datasource`` object (we will use a fake small data source from the provider XYZ for the purposes of this example):

..  code::

    ds = dm.get_datasource_by_name(
        'XYZ Data Source'
    )

Get raw data from the data source, producing a schema and pandas dataframe:

..  code::

    schema, df = dm.get_data_raw(ds)

The schema will contain metadata for the data source, with keys and values showing the roles different columns play in the data.
In the case of the above data source:

..  code::

    >>> schema

   {
        'lower_date': ['period_start'],
        'upper_date': ['period_end'],
        'section_pk': ['section_pk'],
        'value': ['panel_sales'],
        'split': ['category']
    }

This result indicates that the ``period_start`` column represents the lower date for each data point, and so on.

Next, looking at the dataframe we see:

..  code::

    >>> df.head(2)

.. list-table::
   :header-rows: 1

   * - category
     - panel_sales
     - period_end
     - period_start
     - section_pk
   * - Not specified
     - -0.1139
     - 2017-01-01
     - 2016-10-02
     - 617
   * - Not Specified
     - -0.0523
     - 2018-07-02
     - 2018-04-02
     - 742
   * - Category1
     - -0.2233
     - 2018-07-02
     - 2018-04-02
     - 742
   * - Category1
     - -0.4132
     - 2019-03-31
     - 2019-01-01
     - 205

Note that the ``section_pk`` column, which represents which company each data point refers to, is currently in the form of
an internal DataMonster identifier and is not particularly useful for external use. To convert to a more usable form, try:

..  code::

    comps = ds.companies
    section_map = {}
    for comp in comps:
        section_map[comp.pk] = {"name": comp.name,
                                "ticker": comp.ticker}

    def map_pk_to_ticker_and_name(section_map, df):
        ticker_dict = {
            pk: v["ticker"] for pk, v in section_map.items()
        }

        name_dict = {
            pk: v["name"] for pk, v in section_map.items()
        }

        df["ticker"] = df["section_pk"].map(ticker_dict)
        df["comp_name"] = df["section_pk"].map(name_dict)

        df = df.drop(["section_pk"], axis=1)

        return df

We can now use ``map_pk_to_ticker_and_name`` to produce a more human-readable dataframe. For example:


..  code::

    >>> map_pk_to_ticker_and_name(section_map, df).head(2)

.. list-table::
   :header-rows: 1

   * - category
     - panel_sales
     - period_end
     - period_start,
     - ticker,
     - comp_name
   * - Not specified
     - -0.1139
     - 2017-01-01
     - 2016-10-02
     - PRTY
     - PARTY CITY
   * - Not Specified
     - -0.0523
     - 2018-07-02
     - 2018-04-02
     - RUTH
     - RUTH'S HOSPITALITY GROUP
   * - Category1
     - -0.2233
     - 2018-07-02
     - 2018-04-02
     - RUTH
     - RUTH'S HOSPITALITY GROUP
   * - Category1
     - -0.4132
     - 2019-03-31
     - 2019-01-01
     - HD
     - HOME DEPOT

Filtering to Specific Dimensions
""""""""""""""""""""""""""""""""

The raw data endpoint supports filtering to specific values for dimensions by applying key value pairs as a dictionary,
where the key is the dimension name and the value is a list of possibilities for that dimension. Using the example
above, we could do this in a variety of ways.

Filtering to specific companies (in this case, Party City and Home Depot):

.. code::

    >>> filters = {'section_pk': [617, 205]}
    >>> schema, df = dm.get_data_raw(ds, filters=filters)

.. list-table::
   :header-rows: 1

   * - category
     - panel_sales
     - period_end
     - period_start
     - section_pk
   * - Not specified
     - -0.1139
     - 2017-01-01
     - 2016-10-02
     - 617
   * - Category1
     - -0.4132
     - 2019-03-31
     - 2019-01-01
     - 205

Filtering to specific dimension values (in this case, ``"Category1"``):

..  code::

    >>> filters = {'category': ['Category1']}
    >>> schema, df = dm.get_data_raw(ds, filters=filters)

.. list-table::
   :header-rows: 1

   * - category
     - panel_sales
     - period_end
     - period_start
     - section_pk
   * - Category1
     - -0.2233
     - 2018-07-02
     - 2018-04-02
     - 742
   * - Category1
     - -0.4132
     - 2019-03-31
     - 2019-01-01
     - 205

Combining filters across dimensions (in this case, ``"Category1"`` for Ruth's Hospitality Group):

..  code::

    >>> filters = {'section_pk': [742], 'category': ['Category1']}
    >>> schema, df = dm.get_data_raw(ds, filters=filters)

.. list-table::
   :header-rows: 1

   * - category
     - panel_sales
     - period_end
     - period_start
     - section_pk
   * - Category1
     - -0.2233
     - 2018-07-02
     - 2018-04-02
     - 742

Aggregating Results on Different Cadences
"""""""""""""""""""""""""""""""""""""""""

The raw data endpoint can also take an optional ``Aggregation`` object to request data with a time-based aggregation applied.
For example:

.. code::

    from datamonster_api import DataMonster, Aggregation

    dm = DataMonster(<key_id>, <secret_key>)

    # Get Company for Home Depot
    hd = dm.get_company_by_ticker('hd')

    # Get our Data Source
    ds = dm.get_datasource_by_name('XYZ Data Source')

    # Filter to Home Depot data and aggregate by Home Depot's fiscal quarters
    filters = {'section_pk': [hd.pk]}
    agg = Aggregation(period='fiscalQuarter', company=hd)
    dm.get_data_raw(ds, filters=filters, aggregation=agg)

Get Dimensions for Datasource
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Assuming ``dm`` is a ``DataMonster`` object, and given this fake data source and company:

.. code::

    datasource = next(
        dm.get_datasources(query="Fake Data Source")
    )
    the_gap = dm.get_company_by_ticker("GPS")

this call to ``get_dimensions_for_datasource``:

.. code::

    dimset = dm.get_dimensions_for_datasource(
        datasource,
        filters={
            "section_pk": the_gap.pk,
            "category": "Banana Republic",
        },
    )

returns an iterable, ``dimset``, to a collection with just one dimensions dict.
Assuming ``from pprint import pprint``, the following loop:

.. code::

    for dim in dimset:
        pprint(dim)

prettyprints the single dimension dict:

.. code::

    {
        "max_date": "2019-06-21",
        "min_date": "2014-01-01",
        "row_count": 1998,
        "split_combination": {
            "category": "Banana Republic",
            "country": "US",
            "section_pk": 707,
        },
    }

How to Data Upload
^^^^^^^^^^^^^^^^^^^

Currently the API supports the ability to search for data groups one owns, check the processing status, and upload valid DataFrames. One must still upload data through the UI - specifying the schema. The API is meant to programmatically refresh the data.

..  code::

    for data_group in dm.get_data_groups():
        print(data_group)

Alternatively, one can fetch a data group by its ID and view it's status:

..  code::

    dg = dm.get_data_group_by_id(1012)
    dg.get_current_status()

To view the columns of the data group, and hence the schema, to verify the type of data we wish to reupload:

..  code::

    dg.columns

To refresh the data, call `start_data_refresh` with a valid `pandas.DataFrame` object that matches the schema of the data group.

..  code::

    df = pandas.DataFrame({
        'Start_Date': ['2019-01-01'],
        'end date': ['2019-01-02'],
        'dummy data 1': [1],
        'dummy data_2': [1],
        'Ticker': ['AAP'],
        ...
    })
    dg.start_data_refresh(df)
    dg.get_current_status()

One will notice the `status` of the data group object change.
