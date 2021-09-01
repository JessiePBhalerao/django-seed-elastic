from django.test import TestCase
from django.conf import settings

from elasticsearch_dsl import Index, Document, Text, Keyword, GeoShape, Integer, Float, GeoPoint
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


class TestTrialDocument(Document):
    product_id = Integer()
    value_yield = Float()
    display_yield = Text()
    value_yield_adv = Float()
    display_yield_adv = Text()
    display_gi_rank = Text()

    test_name = Text()
    site_name = Text()
    state = Text()
    # location lookups
    year = Integer()
    test_region = Text()
    location = GeoPoint()


class CornTrialDoc(TestTrialDocument):
    pass


class SoyTrialDoc(TestTrialDocument):
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
        id = 1,
        brand='PIONEER',
        name = 'P1197AM',
        tech_package = 'AM',
        proper_brand='Pioneer',
        location="BBOX (-94.8724706, -88.4856042, 40.3469749, 37.7306054)",
    )
    corn.save(index='test_corn')
    corn2 = CornDoc(
        id=2,
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
        id=3,
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
        id=1,
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
        id=2,
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

    # Trials setup
    alton = 'POINT (-96.010279 42.987214)' #[-96.010279, 42.987214]
    emmetsburg = 'POINT (-94.6830357 43.1127427)' #[-94.6830357, 43.1127427]
    osage = 'POINT (-92.8190838 43.2841382)'
    paullina = 'POINT (-95.6880657 42.9791479)'
    plymouth = 'POINT (-93.1215924 43.2446852)'
    ventura = 'POINT (-93.4779849 43.1291257)'

    test_index = 'test_corn_trials'
    try:
        i = Index(test_index)
        i.delete()
    except:
        pass
    i = Index(test_index)
    i.create()

    # product 1
    CornTrialDoc.init(index=test_index)
    ctrial = CornTrialDoc(
        product_id=1,
        value_yield=198.2,
        display_yield='--',
        value_yield_adv=-9.8,
        display_yield_adv='--',
        display_gi_rank='<30',

        test_name='B2020IANOu',
        site_name='Alton',
        state='IA',
        # location lookups
        year=2020,
        test_region='Iowa North [IANO]',
        location=alton,
    )
    ctrial.save(index=test_index)
    ctrial = CornTrialDoc(
        product_id=1,
        value_yield=225.4,
        display_yield='225.4',
        value_yield_adv=5.7,
        display_yield_adv='5.7',
        display_gi_rank='18',

        test_name='B2020IANOu',
        site_name='Emmetsburg',
        state='IA',
        # location lookups
        year=2020,
        test_region='Iowa North [IANO]',
        location=emmetsburg,
    )
    ctrial.save(index=test_index)
    ctrial = CornTrialDoc(
        product_id=1,
        value_yield=186.0,
        display_yield='--',
        value_yield_adv=-12.8,
        display_yield_adv='--',
        display_gi_rank='<30',

        test_name='B2020IANOu',
        site_name='Osage',
        state='IA',
        # location lookups
        year=2020,
        test_region='Iowa North [IANO]',
        location=osage,
    )
    ctrial.save(index=test_index)
    ctrial = CornTrialDoc(
        product_id=1,
        value_yield=201.0,
        display_yield='201.0',
        value_yield_adv=-8.9,
        display_yield_adv='-8.9',
        display_gi_rank='30',

        test_name='B2020IANOu',
        site_name='Paullina',
        state='IA',
        # location lookups
        year=2020,
        test_region='Iowa North [IANO]',
        location=paullina,
    )
    ctrial.save(index=test_index)
    ctrial = CornTrialDoc(
        product_id=1,
        value_yield=234.2,
        display_yield='234.2',
        value_yield_adv=10.6,
        display_yield_adv='1.6',
        display_gi_rank='24',

        test_name='B2020IANOu',
        site_name='Plymouth',
        state='IA',
        # location lookups
        year=2020,
        test_region='Iowa North [IANO]',
        location=plymouth,
    )
    ctrial.save(index=test_index)
    ctrial = CornTrialDoc(
        product_id=1,
        value_yield=225.4,
        display_yield='225.4',
        value_yield_adv=2.0,
        display_yield_adv='2.7',
        display_gi_rank='21',

        test_name='B2020IANOu',
        site_name='Ventura',
        state='IA',
        # location lookups
        year=2020,
        test_region='Iowa North [IANO]',
        location=ventura,
    )
    ctrial.save(index=test_index)

    # product 2
    ctrial = CornTrialDoc(
        product_id=2,
        value_yield=218.2,
        display_yield='218.2',
        value_yield_adv=11.4,
        display_yield_adv='11.4',
        display_gi_rank='4',

        test_name='B2020IANOu',
        site_name='Alton',
        state='IA',
        # location lookups
        year=2020,
        test_region='Iowa North [IANO]',
        location=alton,
    )
    ctrial.save(index=test_index)
    ctrial = CornTrialDoc(
        product_id=2,
        value_yield=236.4,
        display_yield='236.4',
        value_yield_adv=15.7,
        display_yield_adv='15.7',
        display_gi_rank='3',

        test_name='B2020IANOu',
        site_name='Emmetsburg',
        state='IA',
        # location lookups
        year=2020,
        test_region='Iowa North [IANO]',
        location=emmetsburg,
    )
    ctrial.save(index=test_index)
    ctrial = CornTrialDoc(
        product_id=2,
        value_yield=206.0,
        display_yield='206.0',
        value_yield_adv=-1.4,
        display_yield_adv='-1.4',
        display_gi_rank='28',

        test_name='B2020IANOu',
        site_name='Osage',
        state='IA',
        # location lookups
        year=2020,
        test_region='Iowa North [IANO]',
        location=osage,
    )
    ctrial.save(index=test_index)
    ctrial = CornTrialDoc(
        product_id=2,
        value_yield=241.0,
        display_yield='241.0',
        value_yield_adv=28.9,
        display_yield_adv='28.9',
        display_gi_rank='2',

        test_name='B2020IANOu',
        site_name='Paullina',
        state='IA',
        # location lookups
        year=2020,
        test_region='Iowa North [IANO]',
        location=paullina,
    )
    ctrial.save(index=test_index)
    ctrial = CornTrialDoc(
        product_id=2,
        value_yield=241.1,
        display_yield='241.1',
        value_yield_adv=0.6,
        display_yield_adv='0.6',
        display_gi_rank='12',

        test_name='B2020IANOu',
        site_name='Plymouth',
        state='IA',
        # location lookups
        year=2020,
        test_region='Iowa North [IANO]',
        location=plymouth,
    )
    ctrial.save(index=test_index)
    ctrial = CornTrialDoc(
        product_id=2,
        value_yield=241.8,
        display_yield='241.8',
        value_yield_adv=2.7,
        display_yield_adv='2.7',
        display_gi_rank='21',

        test_name='B2020IANOu',
        site_name='Ventura',
        state='IA',
        # location lookups
        year=2020,
        test_region='Iowa North [IANO]',
        location=ventura,
    )
    ctrial.save(index=test_index)

    # product 2 different region
    ctrial = CornTrialDoc(
        product_id=2,
        value_yield=212.8,
        display_yield='212.8',
        value_yield_adv=12.0,
        display_yield_adv='12.0',
        display_gi_rank='21',

        test_name='B2020SDSEa',
        site_name='Salem',
        state='SD',
        # location lookups
        year=2020,
        test_region='South Dakota Southeast [SDSE]',
        location='POINT (-97.388953 43.7241455)'
    )
    ctrial.save(index=test_index)
    ctrial = CornTrialDoc(
        product_id=2,
        value_yield=179.8,
        display_yield='179.8',
        value_yield_adv=-12.0,
        display_yield_adv='--',
        display_gi_rank='>30',

        test_name='B2020SDSEa',
        site_name='Irene',
        state='SD',
        # location lookups
        year=2020,
        test_region='South Dakota Southeast [SDSE]',
        location='POINT (-97.1570149 43.0834535)'
    )
    ctrial.save(index=test_index)

    # have to wait for the documents to index properly
    time.sleep(2)
    print('\nES test index built\n')

