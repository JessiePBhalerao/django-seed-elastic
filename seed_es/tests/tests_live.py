from django.test import TestCase, LiveServerTestCase, Client, override_settings
from unittest import skip
from django.urls import reverse
from django.contrib.auth.models import User
from rest_framework.test import APITestCase, APIClient
from elasticsearch_dsl import Search

from .tests_setup import setUpES
import requests
import json

# setUpES()

"""
THESE tests require a complete run of the ES index build with the full datasert through 2021 --> EXCLUDE 2022!
"""

@skip("Live server tests -- only run for integration testing")
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
        s = Search(index='corn').query('match', brand='PIONEER')
        resp = s.execute()
        self.assertEqual(10, len(resp.hits))

    def test_schema_only_search(self):
        url = reverse('search_schema', args=('corn',))
        full_url = f'http://localhost:8001{url}'
        data = {'query': None, 'filters': {} }
        resp = self.client.get(full_url, data=json.dumps(data), headers=self.headers)
        self.assertEqual(resp.status_code, 200)
        #print(json.loads(resp.content))
        self.assertNotIn('hits', json.loads(resp.content))
        self.assertEqual(json.loads(resp.content)['aggregations']['_filter_yield_obs']['doc_count'], 1599)
        self.assertEqual(json.loads(resp.content)['aggregations']['_filter_year']['doc_count'], 1599)
        self.assertEqual(json.loads(resp.content)['aggregations']['_filter_brand']['doc_count'], 1599)
        self.assertEqual(json.loads(resp.content)['aggregations']['_filter_maturity_range']['doc_count'], 1599)
        self.assertEqual(json.loads(resp.content)['aggregations']['_filter_tech_package']['doc_count'], 1599)
        self.assertEqual(len(json.loads(resp.content)['aggregations']['_filter_tech_package']['tech_package']['buckets']), 10)
        url = reverse('search_schema', args=('soy',))
        full_url = f'http://localhost:8001{url}'
        data = {'query': None, 'filters': {} }
        resp = self.client.get(full_url, data=json.dumps(data), headers=self.headers)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(len(json.loads(resp.content)['aggregations']['_filter_tech_package']['tech_package']['buckets']), 10)

    @override_settings(ES_USE_PAGINATION=False)
    def test_simple_seed_search_corn(self):
        url = reverse('search_seed_facet', args=('corn',))

        full_url = f'http://localhost:8001{url}'
        data = {'query': 'pioneer', 'filters': {'tech_package': 'AM'} }
        resp = self.client.get(full_url, data=json.dumps(data), headers=self.headers)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(len(json.loads(resp.content)['hits']['hits']), 10)

        data = {'query': None, 'filters': {'tech_package': 'AM'} }
        resp = self.client.get(full_url, data=json.dumps(data), headers=self.headers)
        self.assertEqual(resp.status_code, 200)

        self.assertEqual(len(json.loads(resp.content)['hits']['hits']), 10)

        data = {'query': None, 'filters': {'tech_package': ['AM', 'STX']} }
        resp = self.client.get(full_url, data=json.dumps(data), headers=self.headers)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(len(json.loads(resp.content)['hits']['hits']), 10)

        "Unfortunately this query isn't supported by ES 7.10.2 --> starts in 7.11"
        # data = {'query': None,
        #         'location': [-88.5, 39]}
        # resp = self.client.get(full_url, data=json.dumps(data), headers=self.headers)
        # self.assertEqual(resp.status_code, 200)
        # self.assertEqual(len(json.loads(resp.content)['hits']['hits']), 10)
        #
        # data = {'query': None,
        #         'location': [-88.5, 39],
        #         'distance': '50km'}
        # resp = self.client.get(full_url, data=json.dumps(data), headers=self.headers)
        # self.assertEqual(resp.status_code, 200)
        # self.assertEqual(len(json.loads(resp.content)['hits']['hits']), 10)

        data = {'query': '1197', 'filters': {} }
        resp = self.client.get(full_url, data=json.dumps(data), headers=self.headers)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(len(json.loads(resp.content)['hits']['hits']), 1)

        data = {'maturity_range': [100, 104]}
        resp = self.client.get(full_url, data=json.dumps(data), headers=self.headers)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(len(json.loads(resp.content)['hits']['hits']), 10)


    def test_simple_seed_search_soy(self):
        url = reverse('search_seed_facet', args=('soy',))
        full_url = f'http://localhost:8001{url}'

        data = {'query': '11X', 'filters': {} }
        resp = self.client.get(full_url, data=json.dumps(data), headers=self.headers)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(len(json.loads(resp.content)['hits']['hits']), 6)

        data = {'query': None, 'filters': {'tech_package': ['E3', 'RXF']} }
        resp = self.client.get(full_url, data=json.dumps(data), headers=self.headers)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(len(json.loads(resp.content)['hits']['hits']), 10)

        data = {'query': None, 'filters': {'maturity_range': 'Group 1'} }
        resp = self.client.get(full_url, data=json.dumps(data), headers=self.headers)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(len(json.loads(resp.content)['hits']['hits']), 10)

        data = {'query': None, 'filters': {'maturity_range': 'Group 0'} }
        resp = self.client.get(full_url, data=json.dumps(data), headers=self.headers)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(len(json.loads(resp.content)['hits']['hits']), 10)

        data = {'query': None, 'filters': {'yield_obs': [70.0]} }
        resp = self.client.get(full_url, data=json.dumps(data), headers=self.headers)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(len(json.loads(resp.content)['hits']['hits']), 3)

    def test_seed_search_by_maturity_soy(self):
        url = reverse('search_seed_facet', args=('soy',))
        full_url = f'http://localhost:8001{url}'
        data = {'maturity_range': [0, 0.09]}
        resp = self.client.get(full_url, data=json.dumps(data), headers=self.headers)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(len(json.loads(resp.content)['hits']['hits']), 10)

        data = {'maturity_range': [1, 2]}
        resp = self.client.get(full_url, data=json.dumps(data), headers=self.headers)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(len(json.loads(resp.content)['hits']['hits']), 10)


@skip("Live server tests -- only run for integration testing")
class TrialFacetedSearchTests(TestCase):

    def setUp(self):
        # for now we are using the live local server (django and ES)
        self.client = requests.Session()
        self.headers = {'Content-type': 'application/json'}
        self.su = User.objects.create_superuser('jessie', email='jessie@gmail.com', password='SUPR1$E')
        originator = Organization.objects.create(fullname='Default Seed Company',
                                                 created_by=self.su, modified_by=self.su)
        self.corn = Corn.objects.create(
            brand='PIONEER',
            name='BEST100X',
            id=1,
            seed_status='A',
            maturity=100,
            originator=originator,
        )
        # TODO make a test ES server work and change the client
        # self.client = Client()
        # resp = client.generic(method="GET", path=full_url, data=json.dumps(data),
        #                            content_type='application/json')


    def test_connection(self):
        s = Search(index='corn').query('match', brand='PIONEER')
        resp = s.execute()
        self.assertEqual(10, len(resp.hits))

    def test_simple_corn_seed_trials(self):
        url = reverse('search_trial_facet', args=('corntrials', 3244))
        full_url = f"http://localhost:8001{url}"
        resp = self.client.get(full_url, headers=self.headers)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(len(json.loads(resp.content)['hits']['hits']), 5)
        self.assertNotIn('value_yield', json.loads(resp.content)['hits']['hits'][0]['_source'])
        self.assertIn('display_yield', json.loads(resp.content)['hits']['hits'][0]['_source'])

        data = {'seed_id': 3244, 'location': 'POINT (-98.1942616 46.6535903)'}
        resp = self.client.get(full_url, data=json.dumps(data), headers=self.headers)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(len(json.loads(resp.content)['hits']['hits']), 1)


@skip("Live server tests -- only run for integration testing")
class ReportFacetedSearchTests(TestCase):

    def setUp(self):
        # for now we are using the live local server (django and ES)
        self.client = requests.Session()
        self.headers = {'Content-type': 'application/json'}
        url = reverse('search_report_facet', args=('cornreports',))
        self.full_url = f'http://localhost:8001{url}'

    def test_reports_near_location(self):
        data = {'query': None,
                'location': 'POINT (-96.010279 42.987214)',
                'distance': '100km'}
        resp = self.client.get(self.full_url, data=json.dumps(data), headers=self.headers)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(len(json.loads(resp.content)['hits']['hits']), 23)

    def test_reports_by_maturity(self):
        data = {'maturity_range': [100, 104]}
        resp = self.client.get(self.full_url, data=json.dumps(data), headers=self.headers)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(len(json.loads(resp.content)['hits']['hits']), 315)

        data = {'maturity_range': [88, 91]}
        resp = self.client.get(self.full_url, data=json.dumps(data), headers=self.headers)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(len(json.loads(resp.content)['hits']['hits']), 71)

        url = reverse('search_report_facet', args=('soyreports',))
        full_url = f'http://localhost:8001{url}'
        data = {'maturity_range': [1.0, 1.2]}
        resp = self.client.get(full_url, data=json.dumps(data), headers=self.headers)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(len(json.loads(resp.content)['hits']['hits']), 48)

    def test_reports_by_state(self):
        data = {'filters': {'state': ['Iowa', 'Missouri']}}
        resp = self.client.get(self.full_url, data=json.dumps(data), headers=self.headers)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(len(json.loads(resp.content)['hits']['hits']), 148)

    def test_reports_by_soil(self):
        data = {'filters': {'soil_texture': ['silty clay loam']}}
        resp = self.client.get(self.full_url, data=json.dumps(data), headers=self.headers)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(len(json.loads(resp.content)['hits']['hits']), 138)


    # def test_reports_by_last_update(self):
    #     data = {'filters': {'published': ["new"]}}
    #     resp = self.client.get(self.full_url, data=json.dumps(data), headers=self.headers)
    #     self.assertEqual(resp.status_code, 200)
    #     self.assertEqual(len(json.loads(resp.content)['hits']['hits']), 1)
    #
    #     data = {'filters': {'published': ["week"]}}
    #     resp = self.client.get(self.full_url, data=json.dumps(data), headers=self.headers)
    #     self.assertEqual(resp.status_code, 200)
    #     self.assertEqual(len(json.loads(resp.content)['hits']['hits']), 3)
    #
    #     data = {'filters': {'published': ["2 weeks"]}}
    #     resp = self.client.get(self.full_url, data=json.dumps(data), headers=self.headers)
    #     self.assertEqual(resp.status_code, 200)
    #     self.assertEqual(len(json.loads(resp.content)['hits']['hits']), 4)
    #
    #     data = {'filters': {'published': ["month"]}}
    #     resp = self.client.get(self.full_url, data=json.dumps(data), headers=self.headers)
    #     self.assertEqual(resp.status_code, 200)
    #     self.assertEqual(len(json.loads(resp.content)['hits']['hits']), 5)
    #
    #     data = {'filters': {'published': ["any"]}}
    #     resp = self.client.get(self.full_url, data=json.dumps(data), headers=self.headers)
    #     self.assertEqual(resp.status_code, 200)
    #     self.assertEqual(len(json.loads(resp.content)['hits']['hits']), 6)
        # test soybeans