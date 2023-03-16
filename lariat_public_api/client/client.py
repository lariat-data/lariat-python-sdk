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

logger = logging.getLogger(__name__)

s = requests.Session()
s.headers.update({
        'X-Lariat-Application-Key': os.environ.get('LARIAT_API_KEY'),
        'X-Lariat-Api-Key': os.environ.get('LARIAT_APPLICATION_KEY'),
})

class Indicator:
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

    def get_dimension_values(self, dimensions: List[str] = None) -> Dict[str, List[str]]:
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
    def __init__(self, source_id: str, data_source: str, name: str, schema):
        self.source_id = source_id
        self.data_source = data_source
        self.name = name
        self.schema = schema

class Dataset:
    def __init__(self, data_source: str, source_id: str, name: str, id: int, query: str, schema: json):
        self.data_source = data_source
        self.source_id = source_id
        self.name = name
        self.id = id
        self.query = query
        self.schema = schema

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

class FilterClause:
    def __init__(self, field: str, operator: str, values: Union[str, List[str]]):
        self.field = field
        self.operator = operator
        self.values = values


class Filter:
    def __init__(self, clauses: List[FilterClause], operator: str):
        self.clauses = clauses
        self.operator = operator

class MetricRecord:
    def __init__(self, evaluation_time: int, value: float, dimensions: Dict[str, str]):
        self.evaluation_time = evaluation_time
        self.value = value
        self.dimensions = dimensions

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
    r = s.get(f'{LARIAT_PUBLIC_API_ENDPOINT}/raw-datasets',
        params={'dataset_id': dataset_ids})
    return [RawDataset(
        source_id=obj['source_id'],
        data_source=obj['data_source'],
        name=obj['name'],
        schema=obj['schema']) for obj in r.json()['raw_datasets']]

def get_dataset(source_id: str = None, name: str = None) -> Union[Dataset, None]:
    r = s.get(f'{LARIAT_PUBLIC_API_ENDPOINT}/datasets',
        params={'source_id': source_id, 'name': name})
    if r.json():
        return [Dataset(
            data_source=obj['data_source'],
            source_id=obj['source_id'],
            name=obj['dataset_name'],
            id=obj['id'],
            query=obj['query'],
            schema=obj['schema']) for obj in r.json()['computed_datasets']][0]
    return None

def get_datasets(name: str = None) -> List[Dataset]:
    r = s.get(f'{LARIAT_PUBLIC_API_ENDPOINT}/datasets', params={'name': name})
    return [Dataset(
            data_source=obj['data_source'],
            source_id=obj['source_id'],
            name=obj['dataset_name'],
            id=obj['id'],
            query=obj['query'],
            schema=obj['schema']) for obj in r.json()['computed_datasets']]

def get_indicators(datasets: List[Dataset] = [], tags: List[str] = [], fields: List[Field] = []):
    params = {}
    if datasets:
        params['dataset_id'] = [dataset.id for dataset in datasets]
    if tags:
        params['tags'] = tags
    if fields:
        params['fields'] = fields
    r = s.get(f'{LARIAT_PUBLIC_API_ENDPOINT}/indicators', params=params)
    return [Indicator(
            id=obj['indicator_id'],
            dataset_id=obj['computed_dataset_id'],
            dataset_name=obj['computed_dataset_name'],
            query=obj['computed_dataset_query'],
            aggregations=obj.get('aggregations', []),
            name=obj['name'],
            dimensions=obj['group_fields'],
            tags=obj.get('tags', [])) for obj in r.json()['indicators']]

def get_indicator(id: int) -> Indicator:
    params = {'indicator_id': id}
    r = s.get(f'{LARIAT_PUBLIC_API_ENDPOINT}/indicator', params=params)
    obj = r.json()['indicator']
    print(r.json())
    return Indicator(
            id=obj['indicator_id'],
            dataset_id=obj['computed_dataset_id'],
            dataset_name=obj['computed_dataset_name'],
            query=obj['computed_dataset_query'],
            aggregations=obj.get('aggregations', []),
            name=obj['name'],
            dimensions=obj['group_fields'],
            tags=obj.get('tags', [])
    )

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

    print(data)


    r = s.get(f'{LARIAT_PUBLIC_API_ENDPOINT}/query-metrics', data=json.dumps(data))
    print(r.json())

    records = r.json()['records']

    return MetricRecordList(group_by, r.json()['records'])