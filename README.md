# Lariat Python SDK

A Python module to interact with Lariat API and perform various operations like querying indicators, fetching datasets, etc.

## Installation

To install the package, run the following command:

```bash
pip install lariat_python_sdk
```

## Usage

First, import the necessary components:

```python
from lariat_client import configure, get_raw_datasets, get_datasets, get_indicators, get_indicator, query, Filter, FilterClause
import datetime
```

Set up your API key and application key:

```python
API_KEY = "your-api-key"
APP_KEY = "your-app-key"

configure(api_key=API_KEY, application_key=APP_KEY)
```

Get raw datasets:

```python
raw_datasets = get_raw_datasets(dataset_ids=[1, 2, 3])
```

Get computed datasets:

```python
datasets = get_datasets()
```

Get indicators:

```python
indicators = get_indicators(datasets=datasets)
```

Get a specific indicator:

```python
indicator = get_indicator(id=1)
```

Query an indicator:

```python
from_ts = datetime.datetime(2022, 1, 1)
to_ts = datetime.datetime(2022, 2, 1)
group_by = ["country"]
filter_clause = FilterClause(field="country", operator="in", values="US,UK"])
query_filter = Filter(clauses=[filter_clause], operator="and")

results = query(indicator["id"], from_ts, to_ts, group_by, query_filter=query_filter)

# Convert results to a DataFrame
results_df = results.to_df()

# Save results to a CSV file
results.to_csv("results.csv")
```
