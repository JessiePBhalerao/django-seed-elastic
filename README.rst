django-seed-elastic
===================

Seed_es is a Django app to conduct ElasticSearch searches for FIRST seed documents and trials. Endpoints to find relevant seed documents and filter and summarize trials will be included.  Endpoints for querying FIRST harvest and summary reports will supported at some point.

Detailed documentation is in the "docs" directory.

Quick start
-----------

1. Add "polls" to your INSTALLED_APPS setting like this::

    INSTALLED_APPS = [
        ...
        'seed_es',
    ]

2. Include the seed_es URLconf in your project urls.py like this::

    path('search/', include('seed_es.urls')),

3. Example API calls:

