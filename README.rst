django-seed-elastic
===================

Seed_es is a Django app to conduct ElasticSearch searches for FIRST seed documents and trials. Endpoints to find relevant seed documents and filter and summarize trials will be included.  Endpoints for querying FIRST harvest and summary reports will supported at some point.

Detailed documentation is in the "docs" directory.

Quick start
-----------

1. Add "seed_es" to your INSTALLED_APPS setting like this::

    INSTALLED_APPS = [
        ...
        'django-seed-elastic.seed_es',
    ]

2. Include the seed_es URLconf in your project urls.py like this::

    path('search/', include('django-seed-elastic.seed_es.urls')),

3. Add the following to your project settings file::

    ELASTICSEARCH_DSL={
        'default': {
            'hosts': 'localhost:9200'
        },
        'test': {
            'hosts': 'localhost:9200'
        }
    }
    ES_MAX_RESULTS_COUNT = 500
    ES_PAGINATION_DEFAULT_LENGTH = 10
    ES_USE_PAGINATION = os.environ.get('ES_USE_PAGINATION', "true").lower() == "true"

4. Example API calls:  (look in tests)

