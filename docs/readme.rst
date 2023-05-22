Getting Started
====================

A Python module to interact with Lariat API and perform various
operations like querying indicators, fetching datasets, etc.

Installation
------------

To install the package, run the following command:

.. code:: bash

   pip install lariat_python_sdk

Usage
-----

First, import the necessary components:

.. code:: python

   from lariat_client import configure, get_raw_datasets, get_datasets, get_indicators, get_indicator, query, Filter, FilterClause
   import datetime

Set up your API key and application key:

.. code:: python

   API_KEY = "your-api-key"
   APP_KEY = "your-app-key"

   configure(api_key=API_KEY, application_key=APP_KEY)

Get raw datasets:

.. code:: python

   raw_datasets = get_raw_datasets(dataset_ids=[1, 2, 3])

Get computed datasets:

.. code:: python

   datasets = get_datasets()

Get indicators:

.. code:: python

   indicators = get_indicators(datasets=datasets)

Get a specific indicator:

.. code:: python

   indicator = get_indicator(id=1)

Query an indicator:

.. code:: python

   from_ts = datetime.datetime(2022, 1, 1)
   to_ts = datetime.datetime(2022, 2, 1)
   group_by = ["country"]
   filter_clause = FilterClause(field="country", operator="in", values=["US,UK"])
   query_filter = Filter(clauses=[filter_clause], operator="and")

   results = query(indicator["id"], from_ts, to_ts, group_by, query_filter=query_filter)

   # Convert results to a DataFrame
   results_df = results.to_df()

   # Save results to a CSV file
   results.to_csv("results.csv")

Use the RawQuery interface to add additional query arguments:
*Note: Query arguments attached via the RawQuery interface are subject to changes in their backend interpretation. Use with caution*

.. code:: python

    import lariat_client
    import datetime

    lariat_client.configure(api_key="some_key", application_key="some_other_key")
    indicator = lariat_client.get_indicator(id=1234)
    from_ts = datetime.datetime(2023, 5, 1)
    to_ts = datetime.datetime(2023, 5, 10)

    filter_clause = lariat_client.FilterClause(field="country", operator="in", values="USA")
    query_filter = lariat_client.Filter(clauses=[filter_clause], operator="and")

    raw_query = lariat_client.RawQuery(
            indicator_id=indicator.id,
            from_ts=from_ts,
            to_ts=to_ts,
            aggregate="distinct",
            query_filter=query_filter
    )

    raw_query.add_query_argument("x_axis", "custom_x_axis")
    records = raw_query.send()
