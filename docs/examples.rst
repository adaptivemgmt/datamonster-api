Examples
--------

Get Raw Data
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

    schema, df = dm.get_raw_data(ds)

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

Finally, we can use keyword arguments with double underscores that will be passed to the REST API
to constrain the returned data. For example:

..  code::

    dated_schema, dated_df = dm.get_raw_data(
        ds, period_start__gte="2018-01-01"
    )

Will give all rows from the data source dated on or after 01/01/2018. Similarly:

..  code::

    dated_schema, dated_df = dm.get_raw_data(
        ds, period_end__lt="2018-01-01"
    )

Will return all rows from the data source dated entirely before 01/01/2018.
Lastly, we can use a workaround to get all data where category is specified:

.. code::

    filtered_df1 = dm.get_raw_data(
        ds, category__lt = "Not Specified"
        )[1]

    filtered_df2 = dm.get_raw_data(
        ds, category__gt = "Not Specified"
        )[1]

    pd.concat([filtered_df1, filtered_df2]).head()

This trick is necessary as the REST API does not currently allow for excluding strings.

More generally, to use a filter, pass ``<column>__<filter> = <filter criterium>`` as a keyword
argument into ``get_raw_data`` (note the double underscore).
These are all the supported filters:

- exact
- iexact
- contains
- icontains
- in
- gt
- gte
- lt
- lte
- startswith
- istartswith
- endswith
- iendswith
- range
- year
- month
- day
- week_day
- isnull
- search
- regex
- iregex

Get Dimensions for Datasource
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Assuming ``dm`` is a ``DataMonster`` object, and given this fake datasource and company:

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
