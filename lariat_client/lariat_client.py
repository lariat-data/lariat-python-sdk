"""
A Python module to interact with Lariat API and perform various operations
like querying indicators, fetching datasets, etc.
"""

import requests
import csv
import os
import datetime
import logging
import json
from flatten_json import flatten
import pandas as pd
from typing import List, Dict, Any, Union

LARIAT_PUBLIC_API_ENDPOINT = "http://localhost:8002/public-api"

s = requests.Session()
s.headers.update({
    'X-Lariat-Application-Key': os.environ.get('LARIAT_API_KEY'),
    'X-Lariat-Api-Key': os.environ.get('LARIAT_APPLICATION_KEY'),
})

def configure(api_key: str, application_key: str):
    """
    Configures the Lariat API credentials.

    Args:
        api_key (str): The API key to use for authentication.
        application_key (str): The application key to use for authentication.
    """
    s.headers.update({
        'X-Lariat-Application-Key': application_key,
        'X-Lariat-Api-Key': api_key,
    })

logger = logging.getLogger(__name__)

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
    def __init__(self,
        id: int,
        dataset_id: int,
        dataset_name: str,
        query: str,
        aggregations: List[str],
        name: str,
        dimensions: List[str],
        tags: List[str]):
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

    def get_dimension_values(self, dimensions: List[str] = None) -> Dict[str, List[str]]:
        """
        Fetches the unique values of the specified dimensions for the indicator.

        Args:
            dimensions (List[str], optional): The list of dimensions to fetch values for.
                If not provided, all dimensions will be fetched.

        Returns:
            Dict[str, List[str]]: A dictionary with dimension names as keys and lists of
                unique values as values.
        """ 
        try:
            r = s.get(f'{LARIAT_PUBLIC_API_ENDPOINT}/indicators/{self.id}/dimensions',
                      params={'dimensions': dimensions})
            r.raise_for_status()
            return {
                obj['key']: obj['values'] for obj in r.json()['filters']
            }
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
        data_source (str): The type of data source (e.g., 'csv', 'database').
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
    def __init__(self, data_source: str, source_id: str, name: str, id: int, query: str, schema: json):
        self.data_source = data_source
        self.source_id = source_id
        self.name = name
        self.id = id
        self.query = query
        self.schema = schema

    def __repr__(self):
        return json.dumps(self.__dict__)

    def get_schema(self):
        return self.schema

    def get_schema_fields(self):
        flattened_keys = [flatten(d, '.') for d in [self.schema]][0].keys()
        return [Field(dataset_id=self.id, name=field) for field in flattened_keys]

    def get_indicators(self) -> List[Indicator]:
        return get_indicators(datasets=[self])

class Field:
    def __init__(self, dataset_id: int, name: str):
        self.dataset_id = dataset_id
        self.name = name

    def __repr__(self):
        return json.dumps(self.__dict__)

class FilterClause:
    def __init__(self, field: str, operator: str, values: Union[str, List[str]]):
        self.field = field
        self.operator = operator
        self.values = values

    def __repr__(self):
        return json.dumps(self.__dict__)


class Filter:
    def __init__(self, clauses: List[FilterClause], operator: str):
        self.clauses = clauses
        self.operator = operator

    def __repr__(self):
        return json.dumps(self.__dict__)

class MetricRecord:
    def __init__(self, evaluation_time: int, value: float, dimensions: Dict[str, str]):
        self.evaluation_time = evaluation_time
        self.value = value
        self.dimensions = dimensions

    def __repr__(self):
        return json.dumps(self.__dict__)

    def to_dict(self) -> Dict[str, str]:
        record_dict = {
            'evaluation_time': self.evaluation_time,
            'value': self.value
        }
        for k, v in self.dimensions.items():
            record_dict[k] = v
        return record_dict

class MetricRecordList:
    def __init__(self, group_by_fields: List[str], records):
        self.group_by_fields = group_by_fields
        self.records = [MetricRecord(**record) for record in records]
        self.index = 0

    def __repr__(self):
        return json.dumps(self.__dict__)

    def __iter__(self):
        return self

    def __next__(self):
        if self.index >= len(self.records):
            raise StopIteration
        record = self.records[self.index]
        self.index += 1
        return record

    def to_df(self):
        return pd.DataFrame.from_records([record.to_dict() for record in self.records])

    def to_csv(self, filename, header=True):
        with open(filename, mode='w', newline='') as file:
            writer = csv.writer(file)
            if header:
                output_array = ['evaluation_time', 'value']
                for field in self.group_by_fields:
                    output_array.append(field)
                writer.writerow(output_array)
            for record in self.records:
                vals = record.to_dict()
                writer.writerow([vals[field] for field in output_array])

        

def configure(api_key: str, application_key: str):
    s.headers.update({
        'X-Lariat-Application-Key': application_key,
        'X-Lariat-Api-Key': api_key,
    })

def get_raw_datasets(dataset_ids: List[int]) -> List[RawDataset]:
    try:
        r = s.get(f'{LARIAT_PUBLIC_API_ENDPOINT}/raw-datasets',
            params={'dataset_id': dataset_ids})
        r.raise_for_status()
        return [RawDataset(
            source_id=obj['source_id'],
            data_source=obj['data_source'],
            name=obj['name'],
            schema=obj['schema']) for obj in r.json()['raw_datasets']]
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
    try:
        r = s.get(f'{LARIAT_PUBLIC_API_ENDPOINT}/datasets',
            params={'source_id': source_id, 'name': name})
        r.raise_for_status()
        if r.json():
            return [Dataset(
                data_source=obj['data_source'],
                source_id=obj['source_id'],
                name=obj['dataset_name'],
                id=obj['id'],
                query=obj['query'],
                schema=obj['schema']) for obj in r.json()['computed_datasets']][0]
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
    try:
        r = s.get(f'{LARIAT_PUBLIC_API_ENDPOINT}/datasets', params={'name': name})
        r.raise_for_status()
        return [Dataset(
                data_source=obj['data_source'],
                source_id=obj['source_id'],
                name=obj['dataset_name'],
                id=obj['id'],
                query=obj['query'],
                schema=obj['schema']) for obj in r.json()['computed_datasets']]
    except requests.exceptions.HTTPError as errh:
        logging.error(f"Http Error: {errh}")
    except requests.exceptions.ConnectionError as errc:
        logging.error(f"Error Connecting: {errc}")
    except requests.exceptions.Timeout as errt:
        logging.error(f"Timeout Error: {errt}")
    except requests.exceptions.RequestException as err:
        logging.error(f"Something went wrong: {err}")
        sys.exit(1)

def get_indicators(datasets: List[Dataset] = [], tags: List[str] = [], fields: List[Field] = []):
    params = {}
    if datasets:
        params['dataset_id'] = [dataset.id for dataset in datasets]
    if tags:
        params['tags'] = tags
    if fields:
        params['fields'] = fields
    try:
        r = s.get(f'{LARIAT_PUBLIC_API_ENDPOINT}/indicators', params=params)
        r.raise_for_status()
        indicators = []
        for obj in r.json()['indicators']:
            query = f'SELECT {obj["calculation"]} AS value FROM {obj["computed_dataset_name"]}'
            if obj['filters']:
                query += f' WHERE {obj["filters"]}'
            if obj["group_fields"]:
                query += f' GROUP BY {obj["group_fields"]}'
            indicators.append(Indicator(
                id=obj['indicator_id'],
                dataset_id=obj['computed_dataset_id'],
                dataset_name=obj['computed_dataset_name'],
                query=query,
                aggregations=obj.get('aggregations', []),
                name=obj['name'],
                dimensions=obj['group_fields'],
                tags=obj.get('tags', [])
            ))
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
    params = {'indicator_id': id}
    try:
        r = s.get(f'{LARIAT_PUBLIC_API_ENDPOINT}/indicator', params=params)
        r.raise_for_status()
        obj = r.json()['indicator']
        query = f'SELECT {obj["calculation"]} AS value FROM {obj["computed_dataset_name"]}'
        if obj['filters']:
            query += f' WHERE {obj["filters"]}'
        if obj["group_fields"]:
            query += f' GROUP BY {obj["group_fields"]}'
        return Indicator(
                id=obj['indicator_id'],
                dataset_id=obj['computed_dataset_id'],
                dataset_name=obj['computed_dataset_name'],
                query=query,
                aggregations=obj.get('aggregations', []),
                name=obj['name'],
                dimensions=obj['group_fields'],
                tags=obj.get('tags', [])
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
    indicator: Indicator,
    from_ts: datetime.datetime,
    to_ts: datetime.datetime = datetime.datetime.now(),
    group_by: List[str] = [],
    aggregate: str = None,
    query_filter: Filter = None,
    output_format: str = 'json'
    ):
    data_filter = {
        'operator': 'or',
        'filters': []
    }
    if group_by:
        data_filter['group_by_clauses'] = group_by
    if query_filter:
        data_filter['operator'] = query_filter.operator
        data_filter['filters'] = [
            {
                "field": clause.field,
                "operator": clause.operator,
                "value": clause.value
            }
            for clause in query_filter.clauses
        ]
    data = {
        "indicator_id": indicator.id,
        "filter": data_filter,
        "time_range": {
            "from_ts": int(from_ts.timestamp() * 1000),
            "to_ts": int(to_ts.timestamp() * 1000)
        },
    }
    if aggregate:
        data['aggregation'] = aggregate

    try:
        r = s.get(f'{LARIAT_PUBLIC_API_ENDPOINT}/query-metrics', data=json.dumps(data))
        r.raise_for_status()
        records = r.json()['records']

        return MetricRecordList(group_by, r.json()['records'])
    except requests.exceptions.HTTPError as errh:
        logging.error(f"Http Error: {errh}")
    except requests.exceptions.ConnectionError as errc:
        logging.error(f"Error Connecting: {errc}")
    except requests.exceptions.Timeout as errt:
        logging.error(f"Timeout Error: {errt}")
    except requests.exceptions.RequestException as err:
        logging.error(f"Something went wrong: {err}")
        sys.exit(1)