from django.test import TestCase
from django.conf import settings

from elasticsearch_dsl import Index, Document, Text, Keyword, GeoShape
from elasticsearch_dsl import connections
connections.create_connection(hosts=['localhost'])

import time

# indices = ['test_corn', 'test_soy']
# ELASTICSEARCH_TEST_HOST = "http://elasticsearch_test:9200"
#
#
# def setup_elasticsearch():
#     es = Elasticsearch(ELASTICSEARCH_TEST_HOST)
#     for index_name in indices:
#         body = {
#             "settings": {
#                 "number_of_shards": 1,
#                 "number_of_replicas": 1,
#                 "index.store.type": "mmapfs",
#             },
#             # "mappings": schema,
#         }
#         es.indices.create(index=index_name, body=body)
#     return es
#
# def teardown_elasticsearch():
#     es = Elasticsearch(ELASTICSEARCH_TEST_HOST)
#     for index_name in indices:
#         es.indices.delete(index=index_name)
#


class TestSeedDocument(Document):
    brand = Keyword()
    name = Text()
    tech_package = Keyword()
    proper_brand = Keyword()
    location = GeoShape()


class CornDoc(TestSeedDocument):
    pass

class SoyDoc(TestSeedDocument):
    pass



def setUpES():
    # load ES docs
    test_index = 'test_corn'
    try:
        i = Index(test_index)
        i.delete()
    except: pass
    i = Index(test_index)
    i.create()
    test_index = 'test_soy'
    try:
        i = Index(test_index)
        i.delete()
    except: pass
    i = Index(test_index)
    i.create()

    CornDoc.init(index='test_corn')
    corn = CornDoc(
        brand='PIONEER',
        name = 'P1197AM',
        tech_package = 'AM',
        proper_brand='Pioneer',
        location="BBOX (-94.8724706, -88.4856042, 40.3469749, 37.7306054)",
    )
    corn.save(index='test_corn')
    corn2 = CornDoc(
        brand='PIONEER',
        name='P9999',
        tech_package='STX',
        proper_brand='Pioneer',
        location="BBOX (-94.8724706, -88.4856042, 40.3469749, 37.7306054)",
        overall_yield_obs = 25,
        years=[2020, 2019, 2018],
    )
    corn2.save(index='test_corn')
    corn3 = CornDoc(
        brand='NUTECH',
        name='N1234',
        tech_package='AM',
        proper_brand='NuTech',
        location = "BBOX (-94.8724706, -90.4856042, 42.3469749, 41.7306054)",
        overall_yield_obs=9,
        years=[2019, 2018],
    )
    corn3.save(index='test_corn')

    SoyDoc.init(index='test_soy')
    bean = SoyDoc(
        brand='PIONEER',
        name = 'P11X01',
        tech_package = 'RXF',
        maturity = 1.1,
        proper_brand='Pioneer',
        location="BBOX (-94.8724706, -88.4856042, 40.3469749, 37.7306054)",
        overall_yield_obs=9,
        years=[2020],
    )
    bean.save(index='test_soy')
    bean2 = SoyDoc(
        brand='PIONEER',
        name = 'P009X01',
        tech_package = 'E3',
        maturity = 0.09,
        proper_brand='Pioneer',
        location="BBOX (-94.8724706, -88.4856042, 40.3469749, 37.7306054)",
        overall_yield_obs=24,
        years=[2020],
    )
    bean2.save(index='test_soy')


    # have to wait for the documents to index properly
    time.sleep(2)
    print('\nES test index built\n')

