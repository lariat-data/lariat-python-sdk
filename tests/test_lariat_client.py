import unittest
from unittest.mock import MagicMock, patch
import requests
import datetime
import json
from lariat_client import Indicator, Dataset, Field, FilterClause, Filter, configure, get_raw_datasets, get_dataset, get_datasets, get_indicators, get_indicator, query, s

class TestLariatClient(unittest.TestCase):

    @patch('lariat_client.s')
    def test_configure(self, mock_session):
        api_key = 'fake_api_key'
        app_key = 'fake_app_key'
        configure(api_key, app_key)
        mock_session().headers.update.assert_called_with({
            'X-Lariat-Application-Key': app_key,
            'X-Lariat-Api-Key': api_key
        })

    @patch('lariat_client.s.get')
    def test_get_raw_datasets(self, mock_get):
        mock_get().json.return_value = {
            'raw_datasets': [
                {
                    'source_id': '1',
                    'data_source': 'csv',
                    'name': 'Test Dataset',
                    'schema': {}
                }
            ]
        }
        raw_datasets = get_raw_datasets([1])
        self.assertEqual(len(raw_datasets), 1)
        self.assertEqual(raw_datasets[0].source_id, '1')
        self.assertEqual(raw_datasets[0].data_source, 'csv')
        self.assertEqual(raw_datasets[0].name, 'Test Dataset')

    @patch('lariat_client.s.get')
    def test_get_dataset(self, mock_get):
        mock_get().json.return_value = {
            'source_id': '1',
            'data_source': 'csv',
            'name': 'Test Dataset',
            'schema': {}
        }
        dataset = get_dataset(1)
        self.assertEqual(dataset.source_id, '1')
        self.assertEqual(dataset.data_source, 'csv')
        self.assertEqual(dataset.name, 'Test Dataset')

    @patch('lariat_client.s.get')
    def test_get_datasets(self, mock_get):
        mock_get().json.return_value = [
            {
                'source_id': '1',
                'data_source': 'csv',
                'name': 'Test Dataset',
                'schema': {}
            },
            {
                'source_id': '2',
                'data_source': 'json',
                'name': 'Test Dataset 2',
                'schema': {}
            }
        ]
        datasets = get_datasets()
        self.assertEqual(len(datasets), 2)
        self.assertEqual(datasets[0].source_id, '1')
        self.assertEqual(datasets[0].data_source, 'csv')
        self.assertEqual(datasets[0].name, 'Test Dataset')
        self.assertEqual(datasets[1].source_id, '2')
        self.assertEqual(datasets[1].data_source, 'json')
        self.assertEqual(datasets[1].name, 'Test Dataset 2')

    @patch('lariat_client.s.get')
    def test_get_indicators(self, mock_get):
        mock_get().json.return_value = [
            {
                'indicator_id': '1',
                'name': 'Indicator 1',
                'source_id': '1',
                'data_type': 'float'
            },
            {
                'indicator_id': '2',
                'name': 'Indicator 2',
                'source_id': '1',
                'data_type': 'int'
            }
        ]
        indicators = get_indicators('1')
        self.assertEqual(len(indicators), 2)
        self.assertEqual(indicators[0].indicator_id, '1')
        self.assertEqual(indicators[0].name, 'Indicator 1')
        self.assertEqual(indicators[0].source_id, '1')
        self.assertEqual(indicators[0].data_type, 'float')
        self.assertEqual(indicators[1].indicator_id, '2')
        self.assertEqual(indicators[1].name, 'Indicator 2')
        self.assertEqual(indicators[1].source_id, '1')
        self.assertEqual(indicators[1].data_type, 'int')

    @patch('lariat_client.s.get')
    def test_get_indicator(self, mock_get):
        mock_get().json.return_value = {
            'indicator_id': '1',
            'name': 'Indicator 1',
            'source_id': '1',
            'data_type': 'float'
        }
        indicator = get_indicator('1')
        self.assertEqual(indicator.indicator_id, '1')
        self.assertEqual(indicator.name, 'Indicator 1')
        self.assertEqual

        (indicator.source_id, '1')
        self.assertEqual(indicator.data_type, 'float')

    @patch('lariat_client.s.get')
    def test_get_sources(self, mock_get):
        mock_get().json.return_value = [
            {
                'source_id': '1',
                'name': 'Source 1',
            },
            {
                'source_id': '2',
                'name': 'Source 2',
            }
        ]
        sources = get_sources()
        self.assertEqual(len(sources), 2)
        self.assertEqual(sources[0].source_id, '1')
        self.assertEqual(sources[0].name, 'Source 1')
        self.assertEqual(sources[1].source_id, '2')
        self.assertEqual(sources[1].name, 'Source 2')

    @patch('lariat_client.s.get')
    def test_get_source(self, mock_get):
        mock_get().json.return_value = {
            'source_id': '1',
            'name': 'Source 1',
        }
        source = get_source('1')
        self.assertEqual(source.source_id, '1')
        self.assertEqual(source.name, 'Source 1')

if __name__ == '__main__':
    unittest.main()