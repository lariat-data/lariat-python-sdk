"""
A Python module to interact with Lariat API and perform various operations
like querying indicators, fetching datasets, etc.
"""

import csv
import datetime
from dotenv import load_dotenv
import json
import logging
import os
from typing import List, Dict, Any, Union
import sys
from flatten_json import flatten
import pandas as pd
import requests

LARIAT_PUBLIC_API_ENDPOINT = "http://api.lariatdata.com/v1"
load_dotenv()
api_key = os.getenv("LARIAT_API_KEY")
application_key = os.getenv("LARIAT_APPLICATION_KEY")

logger = logging.getLogger(__name__)

s = requests.Session()
s.headers.update(
    {
        "X-Lariat-Application-Key": api_key,
        "X-Lariat-Api-Key": application_key,
    }
)


def configure(api_key: str, application_key: str):
    """
    Configures the Lariat API credentials.

    Args:
        api_key (str): The API key to use for authentication.
        application_key (str): The application key to use for authentication.
    """
    s.headers.update(
        {
            "X-Lariat-Application-Key": application_key,
            "X-Lariat-Api-Key": api_key,
        }
    )


class Field:
    """A class representing a dataset's field in Lariat.

    Attributes:
        dataset_id (int): the unique ID of a dataset.
        name (str): The name of the field.
    """

    def __init__(self, dataset_id: int, name: str):
        self.dataset_id = dataset_id
        self.name = name

    def __repr__(self):
        return json.dumps(self.__dict__)


class Indicator:
    """A class representing a Lariat indicator.

    Attributes:
        id (int): The unique ID of the indicator.
        dataset_id (int): The unique ID of the dataset associated with the indicator.
        dataset_name (str): The name of the dataset associated with the indicator.
        query (str): The SQL query used to compute the indicator.
        aggregations (List[str]): The list of aggregation functions applied to the indicator.
        name (str): The name of the indicator.
        dimensions (List[str]): The list of dimensions used to group the indicator.
        tags (List[str]): The list of tags associated with the indicator.
    """

    def __init__(
        self,
        id: int,
        dataset_id: int,
        dataset_name: str,
        query: str,
        aggregations: List[str],
        name: str,
        dimensions: List[str],
        tags: List[str],
    ):
        self.id = id
        self.dataset_id = dataset_id
        self.dataset_name = dataset_name
        self.query = query
        self.aggregations = aggregations
        self.name = name
        self.dimensions = dimensions
        self.tags = tags

    def __repr__(self):
        return json.dumps(self.__dict__)

    def get_dimension_values(
        self, dimensions: List[str] = None
    ) -> Dict[str, List[str]]:
        """
        Fetches the unique values of the specified dimensions for the indicator.

        Args:
            dimensions (List[str], optional): The list of dimensions to fetch values for.
                If not provided, all dimensions will be fetched.

        Returns:
            Dict[str, List[str]]: A dictionary with dimension names as keys and lists of unique values as values.
        """
        try:
            r = s.get(
                f"{LARIAT_PUBLIC_API_ENDPOINT}/indicators/{self.id}/dimensions",
                params={"dimensions": dimensions},
            )
            r.raise_for_status()
            return {obj["key"]: obj["values"] for obj in r.json()["filters"]}
        except requests.exceptions.HTTPError as errh:
            logging.error(f"Http Error: {errh}")
        except requests.exceptions.ConnectionError as errc:
            logging.error(f"Error Connecting: {errc}")
        except requests.exceptions.Timeout as errt:
            logging.error(f"Timeout Error: {errt}")
        except requests.exceptions.RequestException as err:
            logging.error(f"Something went wrong: {err}")
            sys.exit(1)


class RawDataset:
    """A class representing a raw dataset in Lariat.

    Attributes:
        source_id (str): The unique ID of the data source.
        data_source (str): The type of data source (e.g., 'athena').
        name (str): The name of the raw dataset.
        schema (Dict): The schema of the raw dataset.
    """

    def __init__(self, source_id: str, data_source: str, name: str, schema):
        self.source_id = source_id
        self.data_source = data_source
        self.name = name
        self.schema = schema

    def __repr__(self):
        return json.dumps(self.__dict__)


class Dataset:
    """A class representing a dataset in Lariat.

    Attributes:
        data_source (str): The type of data source (e.g., 'athena').
        source_id (str): The unique ID of the data source.
        name (str): The name of the raw dataset.
        schema (Dict): The schema of the raw dataset.
    """

    def __init__(
        self,
        data_source: str,
        source_id: str,
        name: str,
        id: int,
        query: str,
        schema: Dict,
    ):
        self.data_source = data_source
        self.source_id = source_id
        self.name = name
        self.id = id
        self.query = query
        self.schema = schema

    def __repr__(self):
        return json.dumps(self.__dict__)

    def get_schema(self) -> Dict:
        """
        Gets the schema for the dataset.

        Returns:
            json: A json representation of the dataset schema.
        """
        return self.schema

    def get_schema_fields(self) -> List[Field]:
        """
        Gets the schema's fields for the dataset.

        Returns:
            List[Field]: A list of the flattened schema fields of the dataset.
        """
        flattened_keys = [flatten(d, ".") for d in [self.schema]][0].keys()
        return [Field(dataset_id=self.id, name=field) for field in flattened_keys]

    def get_indicators(self) -> List[Indicator]:
        """
        Gets the indicators that query this dataset.

        Returns:
            List[Indicator]: A list of the indicators that query this dataset.
        """
        return get_indicators(datasets=[self])


class FilterClause:
    """A class representing a clause to filter metrics by.

    Attributes:
        field (str): The field to filter on.
        operator (str): The operator to apply to the filter values. Supported operators: [in, not_in, eq, neq]
        values (list or str): A string or list of values to filter on.
    """

    def __init__(self, field: str, operator: str, values: Union[str, List[str]]):
        self.field = field
        self.operator = operator
        self.values = values

    def __repr__(self):
        return json.dumps(self.__dict__)


class Filter:
    """A class representing a filter to use when querying metrics.

    Attributes:
        clauses (list): A list of filter clauses to apply when querying metrics.
        operator (str): The operator to apply to the filter clauses. The options for this are [any, all]. The default operator is 'all'. 'any' corresponds to an "OR"ing of the clauses, and 'all' corresponds to an "AND"ing of the clauses
    """

    def __init__(self, clauses: List[FilterClause], operator: str):
        self.clauses = clauses
        self.operator = operator

    def __repr__(self):
        return json.dumps(self.__dict__)


class MetricRecord:
    """A class representing a record returned when querying metrics.

    Attributes:
        evaluation_time (int): The epoch timestamp associated with the indicator's evaluation.
        value (str): The value of the metric at this evaluation time.
        dimensions (dict): A dictionary mapping of a metric's dimensional names to values.
    """

    def __init__(self, evaluation_time: int, value: float, dimensions: Dict[str, str]):
        self.evaluation_time = evaluation_time
        self.value = value
        self.dimensions = dimensions

    def __repr__(self):
        return json.dumps(self.__dict__)

    def to_dict(self) -> Dict[str, str]:
        """
        Gets a dictionary representation of the metric record.

        Returns:
            Dict[str, str]: A dictionary holding the evalution time, value, and
                dimensions of the metric record.
        """
        record_dict = {"evaluation_time": self.evaluation_time, "value": self.value}
        for k, v in self.dimensions.items():
            record_dict[k] = v
        return record_dict


class MetricRecordList:
    """A class representing a collection of metric records.

    A MetricRecordList is printed out as a json string by default. It can be converted to a pandas dataframe
    via the to_df() function and can be written out as a csv via the to_csv() function.

    Attributes:
        group_by_fields (list): The epoch timestamp associated with the indicator's evaluation.
        records (list): A list of metric records returned by a query.
    """

    def __init__(self, group_by_fields: List[str], records):
        self.group_by_fields = group_by_fields
        if records:
            self.records = [MetricRecord(**record) for record in records]
        else:
            self.records = []
        self.index = 0

    def __repr__(self):
        return json.dumps(self.__dict__, default=lambda x: x.__dict__)

    def __iter__(self):
        return self

    def __next__(self):
        if self.index >= len(self.records):
            raise StopIteration
        record = self.records[self.index]
        self.index += 1
        return record

    def to_df(self) -> pd.DataFrame:
        """
        Converts the MetricRecord list into a pandas dataframe.

        Returns:
            Pandas DataFrame: A Dataframe consisting of the metric records returned by an indicator query.
        """
        return pd.DataFrame.from_records([record.to_dict() for record in self.records])

    def to_csv(self, filename, header=True):
        """
        Writes the MetricRecord list to a csv.
        """
        with open(filename, mode="w", newline="") as file:
            writer = csv.writer(file)
            if header:
                output_array = ["evaluation_time", "value"]
                for field in self.group_by_fields:
                    output_array.append(field)
                writer.writerow(output_array)
            for record in self.records:
                vals = record.to_dict()
                writer.writerow([vals[field] for field in output_array])


def get_raw_datasets(dataset_ids: List[int]) -> List[RawDataset]:
    """
    Gets the raw datasets given a list of dataset ids.

    Args:
        dataset_ids (list): A list of dataset ids to filter on.

    Returns:
        List[RawDataset]: A list of raw datasets that the provided dataset_ids are created from.
    """
    try:
        r = s.get(
            f"{LARIAT_PUBLIC_API_ENDPOINT}/raw-datasets",
            params={"dataset_id": dataset_ids},
        )
        r.raise_for_status()
        return [
            RawDataset(
                source_id=obj["source_id"],
                data_source=obj["data_source"],
                name=obj["name"],
                schema=obj["schema"],
            )
            for obj in r.json()["raw_datasets"]
        ]
    except requests.exceptions.HTTPError as errh:
        logging.error(f"Http Error: {errh}")
    except requests.exceptions.ConnectionError as errc:
        logging.error(f"Error Connecting: {errc}")
    except requests.exceptions.Timeout as errt:
        logging.error(f"Timeout Error: {errt}")
    except requests.exceptions.RequestException as err:
        logging.error(f"Something went wrong: {err}")
        sys.exit(1)


def get_dataset(name: str, source_id: str) -> Union[Dataset, None]:
    """
    Gets the dataset corresponding to a given name source_id.

    Args:
        name: The name of the dataset.
        source_id: The source of the dataset.

    Returns:
        Union[Dataset,None]: A dataset corresponding to the given name and source.
    """
    try:
        r = s.get(
            f"{LARIAT_PUBLIC_API_ENDPOINT}/datasets",
            params={"source_id": source_id, "name": name},
        )
        r.raise_for_status()
        if r.json():
            return [
                Dataset(
                    data_source=obj["data_source"],
                    source_id=obj["source_id"],
                    name=obj["dataset_name"],
                    id=obj["id"],
                    query=obj["query"],
                    schema=obj["schema"],
                )
                for obj in r.json()["computed_datasets"]
            ][0]
        return None
    except requests.exceptions.HTTPError as errh:
        logging.error(f"Http Error: {errh}")
    except requests.exceptions.ConnectionError as errc:
        logging.error(f"Error Connecting: {errc}")
    except requests.exceptions.Timeout as errt:
        logging.error(f"Timeout Error: {errt}")
    except requests.exceptions.RequestException as err:
        logging.error(f"Something went wrong: {err}")
        sys.exit(1)


def get_datasets(name: str = None) -> List[Dataset]:
    """
    Gets the datasets corresponding to a given name source_id.

    Args:
        name (optional str): The name of the dataset.

    Returns:
        List[Dataset]: A list of datasets belonging to the user.
    """
    try:
        r = s.get(f"{LARIAT_PUBLIC_API_ENDPOINT}/datasets", params={"name": name})
        r.raise_for_status()
        return [
            Dataset(
                data_source=obj["data_source"],
                source_id=obj["source_id"],
                name=obj["dataset_name"],
                id=obj["id"],
                query=obj["query"],
                schema=obj["schema"],
            )
            for obj in r.json()["computed_datasets"]
        ]
    except requests.exceptions.HTTPError as errh:
        logging.error(f"Http Error: {errh}")
    except requests.exceptions.ConnectionError as errc:
        logging.error(f"Error Connecting: {errc}")
    except requests.exceptions.Timeout as errt:
        logging.error(f"Timeout Error: {errt}")
    except requests.exceptions.RequestException as err:
        logging.error(f"Something went wrong: {err}")
        sys.exit(1)


def get_indicators(
    datasets: List[Dataset] = [], tags: List[str] = [], fields: List[Field] = []
) -> List[Indicator]:
    """
    Gets the indicators corresponding to optional filters.

    Args:
        datasets (list): A list of datasets to get corresponding indicators for.
        tags (list): A list of tags to filter the indicators by.
        fields (list): A list of fields that the indicators should include.

    Returns:
        List[Indicator]: A list of corresponding indicators.
    """
    params = {}
    if datasets:
        params["dataset_id"] = [dataset.id for dataset in datasets]
    if tags:
        params["tags"] = tags
    if fields:
        params["fields"] = fields
    try:
        r = s.get(f"{LARIAT_PUBLIC_API_ENDPOINT}/indicators", params=params)
        r.raise_for_status()
        indicators = []
        for obj in r.json()["indicators"]:
            query = f'SELECT {obj["calculation"]} AS value FROM "{obj["computed_dataset_name"]}"'
            if obj["filters"]:
                query += f' WHERE {obj["filters"]}'
            if obj["group_fields"]:
                query += f' GROUP BY {obj["group_fields"]}'
            indicators.append(
                Indicator(
                    id=obj["indicator_id"],
                    dataset_id=obj["computed_dataset_id"],
                    dataset_name=obj["computed_dataset_name"],
                    query=query,
                    aggregations=obj.get("aggregations", []),
                    name=obj["name"],
                    dimensions=obj["group_fields"],
                    tags=obj.get("tags", []),
                )
            )
        return indicators
    except requests.exceptions.HTTPError as errh:
        logging.error(f"Http Error: {errh}")
    except requests.exceptions.ConnectionError as errc:
        logging.error(f"Error Connecting: {errc}")
    except requests.exceptions.Timeout as errt:
        logging.error(f"Timeout Error: {errt}")
    except requests.exceptions.RequestException as err:
        logging.error(f"Something went wrong: {err}")
        sys.exit(1)


def get_indicator(id: int) -> Indicator:
    """
    Gets the indicator corresponding to the provided id.

    Args:
        id (int): The indicator unique identifier.

    Returns:
        Indicator: The indicator corresponding to the provided id.
    """
    params = {"indicator_id": id}
    try:
        r = s.get(f"{LARIAT_PUBLIC_API_ENDPOINT}/indicator", params=params)
        r.raise_for_status()
        obj = r.json()["indicator"]
        query = f'SELECT {obj["calculation"]} AS value FROM "{obj["computed_dataset_name"]}"'
        if obj["filters"]:
            query += f' WHERE {obj["filters"]}'
        if obj["group_fields"]:
            query += f' GROUP BY {obj["group_fields"]}'
        return Indicator(
            id=obj["indicator_id"],
            dataset_id=obj["computed_dataset_id"],
            dataset_name=obj["computed_dataset_name"],
            query=query,
            aggregations=obj.get("aggregations", []),
            name=obj["name"],
            dimensions=obj["group_fields"],
            tags=obj.get("tags", []),
        )
    except requests.exceptions.HTTPError as errh:
        logging.error(f"Http Error: {errh}")
    except requests.exceptions.ConnectionError as errc:
        logging.error(f"Error Connecting: {errc}")
    except requests.exceptions.Timeout as errt:
        logging.error(f"Timeout Error: {errt}")
    except requests.exceptions.RequestException as err:
        logging.error(f"Something went wrong: {err}")
        sys.exit(1)


def query(
    indicator_id: int,
    from_ts: datetime.datetime,
    to_ts: datetime.datetime = None,
    group_by: List[str] = None,
    aggregate: str = None,
    query_filter: Filter = None,
) -> MetricRecordList:
    """
    Queries a provided indicator for its metric data,.

    Args:
        indicator (indicator_id): Indicator id to query.
        from_ts (datetime.datetime): The start time for the indicator evaluation.
        to_ts (datetime.datetime): The end time for the indicator evaluation.
        group_by (list): A list of strings to group the metrics data by.
        aggregate (str): An optional aggregation function to apply to the metric.
        query_filter (filter): A filter function to apply to the metric.

    Returns:
        MetricRecordList: An object that contains the list of records output by the query.
    """
    if to_ts is None:
        to_ts = datetime.datetime.now()
    data_filter = {"operator": "or", "filters": []}
    if group_by:
        data_filter["group_by_clauses"] = group_by
    else:
        group_by = []
    if query_filter:
        data_filter["operator"] = query_filter.operator
        data_filter["filters"] = [
            {"field": clause.field, "operator": clause.operator, "value": clause.values}
            for clause in query_filter.clauses
        ]
    data = {
        "indicator_id": indicator_id,
        "filter": data_filter,
        "time_range": {
            "from_ts": int(from_ts.timestamp() * 1000),
            "to_ts": int(to_ts.timestamp() * 1000),
        },
    }
    if aggregate:
        data["aggregation"] = aggregate
    try:
        r = s.get(url=f"{LARIAT_PUBLIC_API_ENDPOINT}/query-metrics", json=data)
        r.raise_for_status()
        records = r.json()["records"]
        return MetricRecordList(group_by, records)
    except requests.exceptions.HTTPError as errh:
        logging.error(f"Http Error: {errh}")
    except requests.exceptions.ConnectionError as errc:
        logging.error(f"Error Connecting: {errc}")
    except requests.exceptions.Timeout as errt:
        logging.error(f"Timeout Error: {errt}")
    except requests.exceptions.RequestException as err:
        logging.error(f"Something went wrong: {err}")
        sys.exit(1)
