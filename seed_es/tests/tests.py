from django.test import TestCase, LiveServerTestCase, Client
from django.urls import reverse
from rest_framework.test import APITestCase, APIClient
from elasticsearch_dsl import Search


from .tests_setup import setUpES
from ..views import SeedSearchView

import requests
import json

setUpES()

class SeedFacetedSearchTests(TestCase):

    def setUp(self):
        # for now we are using the live local server (django and ES)
        self.client = requests.Session()
        self.headers = {'Content-type': 'application/json'}

        # TODO make a test ES server work and change the client
        # self.client = Client()
        # resp = client.generic(method="GET", path=full_url, data=json.dumps(data),
        #                            content_type='application/json')


    def test_connection(self):
        s = Search(index='test_corn').query('match', brand='PIONEER')
        resp = s.execute()
        self.assertEqual(2, len(resp.hits))

    def test_schema_only_search(self):
        url = reverse('search_seed_schema', args=('test_corn',))
        full_url = f'http://localhost:8001{url}'
        data = {'query': None, 'filters': {} }
        resp = self.client.get(full_url, data=json.dumps(data), headers=self.headers)
        self.assertEqual(resp.status_code, 200)
        self.assertNotIn('hits', json.loads(resp.content))
        self.assertEqual(json.loads(resp.content)['aggregations']['_filter_overall_yield_obs']['doc_count'], 3)
        self.assertEqual(json.loads(resp.content)['aggregations']['_filter_year']['doc_count'], 3)
        self.assertEqual(json.loads(resp.content)['aggregations']['_filter_brand']['doc_count'], 3)
        self.assertEqual(json.loads(resp.content)['aggregations']['_filter_maturity_range']['doc_count'], 3)
        self.assertEqual(json.loads(resp.content)['aggregations']['_filter_tech_package']['doc_count'], 3)
        self.assertEqual(json.loads(resp.content)['aggregations']['_filter_tech_package']['tech_package']['buckets'],
                         [{'key': 'AM', 'doc_count': 2},
                          {'key': 'STX', 'doc_count': 1}])
        url = reverse('search_seed_schema', args=('test_soy',))
        full_url = f'http://localhost:8001{url}'
        data = {'query': None, 'filters': {} }
        resp = self.client.get(full_url, data=json.dumps(data), headers=self.headers)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(json.loads(resp.content)['aggregations']['_filter_tech_package']['tech_package']['buckets'],
                         [{'key': 'E3', 'doc_count': 1},
                          {'key': 'RXF', 'doc_count': 1}])


    def test_simple_seed_search_corn(self):
        url = reverse('search_seed_facet', args=('test_corn',))
        full_url = f'http://localhost:8001{url}'
        data = {'query': 'Pioneer', 'filters': {'tech_package': 'AM'} }
        resp = self.client.get(full_url, data=json.dumps(data), headers=self.headers)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(len(json.loads(resp.content)['hits']['hits']), 1)

        data = {'query': None, 'filters': {'tech_package': 'AM'} }
        resp = self.client.get(full_url, data=json.dumps(data), headers=self.headers)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(len(json.loads(resp.content)['hits']['hits']), 2)

        data = {'query': None, 'filters': {'tech_package': ['AM', 'STX']} }
        resp = self.client.get(full_url, data=json.dumps(data), headers=self.headers)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(len(json.loads(resp.content)['hits']['hits']), 3)

        data = {'query': None,
                'location': [-88.5, 39]}
        resp = self.client.get(full_url, data=json.dumps(data), headers=self.headers)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(len(json.loads(resp.content)['hits']['hits']), 2)

    def test_simple_seed_search_soy(self):
        url = reverse('search_seed_facet', args=('test_soy',))
        full_url = f'http://localhost:8001{url}'

        data = {'query': None, 'filters': {'tech_package': ['E3', 'RXF']} }
        resp = self.client.get(full_url, data=json.dumps(data), headers=self.headers)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(len(json.loads(resp.content)['hits']['hits']), 2)

        data = {'query': None, 'filters': {'maturity_range': 'Group 1'} }
        resp = self.client.get(full_url, data=json.dumps(data), headers=self.headers)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(len(json.loads(resp.content)['hits']['hits']), 1)

        data = {'query': None, 'filters': {'maturity_range': 'Group 0'} }
        resp = self.client.get(full_url, data=json.dumps(data), headers=self.headers)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(len(json.loads(resp.content)['hits']['hits']), 1)

        data = {'query': None, 'filters': {'overall_yield_obs': [20.0]} }
        resp = self.client.get(full_url, data=json.dumps(data), headers=self.headers)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(len(json.loads(resp.content)['hits']['hits']), 1)
