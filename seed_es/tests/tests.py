from django.test import TestCase, LiveServerTestCase, Client
from django.urls import reverse
from django.contrib.auth.models import User
from rest_framework.test import APITestCase, APIClient
from elasticsearch_dsl import Search

from .tests_setup import setUpES

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
        s = Search(index='test_corn').query('match', brand='Pioneer')
        resp = s.execute()
        self.assertEqual(2, len(resp.hits))

    def test_schema_only_search(self):
        url = reverse('search_schema', args=('test_corn',))
        full_url = f'http://localhost:8001{url}'
        data = {'query': None, 'filters': {} }
        resp = self.client.get(full_url, data=json.dumps(data), headers=self.headers)
        self.assertEqual(resp.status_code, 200)
        #print(json.loads(resp.content))
        self.assertNotIn('hits', json.loads(resp.content))
        self.assertEqual(json.loads(resp.content)['aggregations']['_filter_yield_obs']['doc_count'], 3)
        self.assertEqual(json.loads(resp.content)['aggregations']['_filter_year']['doc_count'], 3)
        self.assertEqual(json.loads(resp.content)['aggregations']['_filter_brand']['doc_count'], 3)
        self.assertEqual(json.loads(resp.content)['aggregations']['_filter_maturity_range']['doc_count'], 3)
        self.assertEqual(json.loads(resp.content)['aggregations']['_filter_tech_package']['doc_count'], 3)
        self.assertEqual(json.loads(resp.content)['aggregations']['_filter_tech_package']['tech_package']['buckets'],
                         [{'key': 'AM', 'doc_count': 2},
                          {'key': 'STX', 'doc_count': 1}])
        url = reverse('search_schema', args=('test_soy',))
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

        data = {'query': None, 'filters': {'states': ['Iowa']} }
        resp = self.client.get(full_url, data=json.dumps(data), headers=self.headers)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(len(json.loads(resp.content)['hits']['hits']), 1)

        "Elasticsearch 7.10 does not support geo_shape fields in this query.  Need to refactor the documents"
        # data = {'query': None,
        #         'location': [-88.5, 39]}
        # resp = self.client.get(full_url, data=json.dumps(data), headers=self.headers)
        # self.assertEqual(resp.status_code, 200)
        # self.assertEqual(len(json.loads(resp.content)['hits']['hits']), 2)

        data = {'query': 'n12', 'filters': {} }
        resp = self.client.get(full_url, data=json.dumps(data), headers=self.headers)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(len(json.loads(resp.content)['hits']['hits']), 1)

        data = {'maturity_range': [100, 104]}
        resp = self.client.get(full_url, data=json.dumps(data), headers=self.headers)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(len(json.loads(resp.content)['hits']['hits']), 1)

    def test_simple_seed_search_soy(self):
        url = reverse('search_seed_facet', args=('test_soy',))
        full_url = f'http://localhost:8001{url}'

        data = {'query': '11X', 'filters': {} }
        resp = self.client.get(full_url, data=json.dumps(data), headers=self.headers)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(len(json.loads(resp.content)['hits']['hits']), 1)

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

        data = {'query': None, 'filters': {'yield_obs': [20.0]} }
        resp = self.client.get(full_url, data=json.dumps(data), headers=self.headers)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(len(json.loads(resp.content)['hits']['hits']), 1)

        data = {'maturity_range': [0, 0.09]}
        resp = self.client.get(full_url, data=json.dumps(data), headers=self.headers)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(len(json.loads(resp.content)['hits']['hits']), 1)

        data = {'maturity_range': [1, 2]}
        resp = self.client.get(full_url, data=json.dumps(data), headers=self.headers)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(len(json.loads(resp.content)['hits']['hits']), 1)

    def test_compex_query(self):
        url = reverse('search_seed_facet', args=('test_corn',))
        full_url = f'http://localhost:8001{url}'

        data = {'query': 'Pio 119'}
        resp = self.client.get(full_url, data=json.dumps(data), headers=self.headers)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(len(json.loads(resp.content)['hits']['hits']), 2)

        data = {'query': 'AM'}
        resp = self.client.get(full_url, data=json.dumps(data), headers=self.headers)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(len(json.loads(resp.content)['hits']['hits']), 2)

        data = {'query': '114 day'}
        resp = self.client.get(full_url, data=json.dumps(data), headers=self.headers)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(len(json.loads(resp.content)['hits']['hits']), 1)



class TrialFacetedSearchTests(TestCase):

    def setUp(self):
        # for now we are using the live local server (django and ES)
        self.client = requests.Session()
        self.headers = {'Content-type': 'application/json'}

        # TODO make a test ES server work and change the client
        # self.client = Client()
        # resp = client.generic(method="GET", path=full_url, data=json.dumps(data),
        #                            content_type='application/json')


    def test_connection(self):
        s = Search(index='test_corn').query('match', brand='Pioneer')
        resp = s.execute()
        self.assertEqual(2, len(resp.hits))

    def test_simple_corn_seed_trials(self):
        url = reverse('search_trial_facet', args=('test_corn_trials', 1))
        full_url = f"http://localhost:8001{url}"
        resp = self.client.get(full_url, headers=self.headers)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(len(json.loads(resp.content)['hits']['hits']), 6)
        self.assertNotIn('value_yield', json.loads(resp.content)['hits']['hits'][0]['_source'])
        self.assertIn('display_yield', json.loads(resp.content)['hits']['hits'][0]['_source'])

        data = {'location': 'POINT (-96.010279 42.987214)'}
        resp = self.client.get(full_url, data=json.dumps(data), headers=self.headers)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(len(json.loads(resp.content)['hits']['hits']), 1)


class ReportFacetedSearchTests(TestCase):

    def setUp(self):
        # for now we are using the live local server (django and ES)
        self.client = requests.Session()
        self.headers = {'Content-type': 'application/json'}
        url = reverse('search_report_facet', args=('test_corn_reports',))
        self.full_url = f'http://localhost:8001{url}'

    def test_reports_facets(self):
        data = {'filters': {'soil_texture': ['Clay'], 'state': ['MO']}}
        resp = self.client.get(self.full_url, data=json.dumps(data), headers=self.headers)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(len(json.loads(resp.content)['hits']['hits']), 2)
        # The facets keep the selected available options, but reduce the other facets to only available options
        self.assertEqual(len(json.loads(resp.content)['facets']['soil_texture']), 5)
        self.assertEqual(json.loads(resp.content)['facets']['soil_texture'][0][2], True)
        self.assertEqual(json.loads(resp.content)['facets']['state'][0][1], 2)


    def test_reports_near_location(self):
        data = {'query': None,
                'location': 'POINT (-96.010279 42.987214)'}
        resp = self.client.get(self.full_url, data=json.dumps(data), headers=self.headers)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(len(json.loads(resp.content)['hits']['hits']), 1)

    def test_reports_by_maturity(self):
        data = {'maturity_range': [100, 104]}
        resp = self.client.get(self.full_url, data=json.dumps(data), headers=self.headers)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(len(json.loads(resp.content)['hits']['hits']), 5)

        data = {'maturity_range': [88, 91]}
        resp = self.client.get(self.full_url, data=json.dumps(data), headers=self.headers)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(len(json.loads(resp.content)['hits']['hits']), 2)

    def test_reports_by_state(self):
        data = {'filters': {'state': ['IA', 'MO']}}
        resp = self.client.get(self.full_url, data=json.dumps(data), headers=self.headers)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(len(json.loads(resp.content)['hits']['hits']), 7)
        self.assertEqual(len(json.loads(resp.content)['facets']['state']), 2)
        self.assertEqual(json.loads(resp.content)['facets']['state'][0][1], 6)

    def test_reports_by_soil(self):
        data = {'filters': {'soil_texture': ['Clay'], 'state': ['MO', 'IA']}}
        resp = self.client.get(self.full_url, data=json.dumps(data), headers=self.headers)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(len(json.loads(resp.content)['hits']['hits']), 3)
        self.assertEqual(json.loads(resp.content)['facets']['state'][0][1], 2)

    def test_reports_by_last_update(self):
        data = {'filters': {'published': ["new"]}}
        resp = self.client.get(self.full_url, data=json.dumps(data), headers=self.headers)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(len(json.loads(resp.content)['hits']['hits']), 1)

        data = {'filters': {'published': ["week"]}}
        resp = self.client.get(self.full_url, data=json.dumps(data), headers=self.headers)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(len(json.loads(resp.content)['hits']['hits']), 4)

        data = {'filters': {'published': ["2 weeks"]}}
        resp = self.client.get(self.full_url, data=json.dumps(data), headers=self.headers)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(len(json.loads(resp.content)['hits']['hits']), 5)

        data = {'filters': {'published': ["month"]}}
        resp = self.client.get(self.full_url, data=json.dumps(data), headers=self.headers)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(len(json.loads(resp.content)['hits']['hits']), 6)

        data = {'filters': {'published': ["any"]}}
        resp = self.client.get(self.full_url, data=json.dumps(data), headers=self.headers)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(len(json.loads(resp.content)['hits']['hits']), 7)
        # test soybeans

    def test_reports_facets(self):

        data = {'filters': {'state': 'IA'}}
        resp = self.client.get(self.full_url, data=json.dumps(data), headers=self.headers)
        self.assertEqual(resp.status_code, 200)
        content = json.loads(resp.content)
        self.assertEqual(len(content['hits']['hits']), 6)

