from django.utils.timezone import now, timedelta
from elasticsearch_dsl import Index, Document, Text, Keyword, GeoShape, Integer, Float, GeoPoint, Date
from django.conf import settings

import time

from elasticsearch_dsl import connections
# connections.create_connection(hosts=['localhost'])
connections.create_connection(hosts=settings.ELASTICSEARCH_DSL['test']['hosts'])

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
    name = Text()
    tech_package = Keyword()
    brand = Keyword()
    maturity = Float()
    location = GeoShape()
    yield_obs = Integer()
    states = Keyword()


class CornDoc(TestSeedDocument):
    pass


class SoyDoc(TestSeedDocument):
    pass


class TestTrialDocument(Document):
    seed_id = Integer()
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


class TestReportDocument(Document):
    site_name = Text()
    state = Keyword()
    # location lookups
    year = Integer()
    test_region = Text()
    location = GeoPoint()
    soil_texture = Keyword()
    last_update = Date()

class CornReportDoc(TestReportDocument):
    pass


class SoyReportDoc(TestReportDocument):
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
        full_name='PIONEER P1197AM',
        name = 'P1197AM',
        tech_package = 'AM',
        maturity = 101.0,
        brand='Pioneer',
        top30_pct = 70.0,
        location="BBOX (-94.8724706, -88.4856042, 40.3469749, 37.7306054)",
        states=['Iowa', 'Illinois', 'Missouri'],
    )
    corn.save(index='test_corn')
    corn2 = CornDoc(
        id=2,
        full_name='PIONEER P9999',
        name='P9999',
        tech_package='STX',
        maturity = 99.0,
        brand='Pioneer',
        location="BBOX (-94.8724706, -88.4856042, 40.3469749, 37.7306054)",
        yield_obs = 25,
        top30_pct = 70.0,
        years=[2020, 2019, 2018],
        states=['Minnesota', 'North Dakota'],
    )
    corn2.save(index='test_corn')
    corn3 = CornDoc(
        id=3,
        full_name='NUTECH N1234',
        name='N1234',
        tech_package='AM',
        maturity=114.0,
        brand='NuTech',
        location = "BBOX (-94.8724706, -90.4856042, 42.3469749, 41.7306054)",
        yield_obs=9,
        top30_pct = 70.0,
        years=[2019, 2018],
        states=['Ohio', 'Pennsylvania', 'Maryland'],
    )
    corn3.save(index='test_corn')

    SoyDoc.init(index='test_soy')
    bean = SoyDoc(
        id=1,
        full_name='PIONEER P11X01',
        name = 'P11X01',
        tech_package = 'RXF',
        maturity = 1.1,
        brand='Pioneer',
        location="BBOX (-94.8724706, -88.4856042, 40.3469749, 37.7306054)",
        yield_obs=9,
        top30_pct = 70.0,
        years=[2020],
    )
    bean.save(index='test_soy')
    bean2 = SoyDoc(
        id=2,
        full_name='PIONEER P009X01',
        name = 'P009X01',
        tech_package = 'E3',
        maturity = 0.09,
        brand='Pioneer',
        location="BBOX (-94.8724706, -88.4856042, 40.3469749, 37.7306054)",
        yield_obs=24,
        top30_pct = 70.0,
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
        seed_id=1,
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
        seed_id=1,
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
        seed_id=1,
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
        seed_id=1,
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
        seed_id=1,
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
        seed_id=1,
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
        seed_id=2,
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
        seed_id=2,
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
        seed_id=2,
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
        seed_id=2,
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
        seed_id=2,
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
        seed_id=2,
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
        seed_id=2,
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
        seed_id=2,
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

    " Set up reports index"
    test_index = 'test_corn_reports'
    try:
        i = Index(test_index)
        i.delete()
    except:
        pass
    i = Index(test_index)
    i.create()

    CornReportDoc.init(index=test_index)
    doc = CornReportDoc(
        site_name='Alton',
        state='IA',
        # location lookups
        year=2020,
        test_region='Iowa North [IANO]',
        location=alton,
        pub_days_ago=0,
        soil_texture='Silty Clay Loam',
        maturity=[100.0, 101.0, 102.0, 103.0, 104.0, 105.0],
    )
    doc.save(index=test_index)
    doc = CornReportDoc(
        site_name='Emmetsburg',
        state='IA',
        # location lookups
        year=2020,
        test_region='Iowa North [IANO]',
        location=emmetsburg,
        pub_days_ago=3,
        soil_texture='Clay Loam',
        maturity=[90.0, 91.0, 92.0, 93.0, 94.0, 95.0]
    )
    doc.save(index=test_index)
    doc = CornReportDoc(
        site_name='Osage',
        state='IA',
        # location lookups
        year=2020,
        test_region='Iowa North [IANO]',
        location=osage,
        soil_texture='Sandy Clay Loam',
        pub_days_ago=7,
        maturity=[90.0, 91.0, 92.0, 93.0, 94.0, 95.0]
    )
    doc.save(index=test_index)
    doc = CornReportDoc(
        site_name='Paullina',
        state='IA',
        # location lookups
        year=2020,
        test_region='Iowa North [IANO]',
        location=paullina,
        soil_texture='Clay',
        pub_days_ago=14,
        maturity=[100.0, 101.0, 102.0, 103.0, 104.0, 105.0]
    )
    doc.save(index=test_index)
    doc = CornReportDoc(
        site_name='Plymouth',
        state='IA',
        # location lookups
        year=2020,
        test_region='Iowa North [IANO]',
        location=plymouth,
        pub_days_ago=21,
        soil_texture='Sand',
        maturity=[100.0, 101.0, 102.0, 103.0, 104.0, 105.0]
    )
    doc.save(index=test_index)
    doc = CornReportDoc(
        site_name='Ventura',
        state='IA',
        # location lookups
        year=2020,
        test_region='Iowa North [IANO]',
        location=ventura,
        pub_days_ago=32,
        soil_texture='Clay',
        maturity=[100.0, 101.0, 102.0, 103.0, 104.0, 105.0]
    )
    doc.save(index=test_index)
    doc = CornReportDoc(
        site_name='New Franklin',
        state='MO',
        # location lookups
        year=2019,
        test_region='Missouri North [MONO]',
        location=ventura,
        pub_days_ago=3,
        soil_texture='Clay',
        maturity=[100.0, 101.0, 102.0, 103.0, 104.0, 105.0]
    )
    doc.save(index=test_index)

    # have to wait for the documents to index properly
    time.sleep(2)
    print('\nES test index built\n')

