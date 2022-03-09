from elasticsearch_dsl import FacetedSearch, TermsFacet, RangeFacet, HistogramFacet

from elasticsearch import Elasticsearch
from elasticsearch_dsl.connections import connections
from elasticsearch_dsl import Search, Q
from copy import deepcopy

import logging

logger = logging.getLogger(__name__)

DEFAULT_LOCATION_QUERY_DISTANCE = "20km"


class SeedFacetedSearch(FacetedSearch):
    doc_types = ['_doc']
    index = None
    location = None
    distance = DEFAULT_LOCATION_QUERY_DISTANCE

    # fields that should be searched
    fields = ['brand',
              # 'tech_package',
              "name",
              # 'maturity',
              # 'overall_yield_obs',
              # 'guide_score', 'customer_score', 'price_score',
              # 'years', 'overall_yield_adv'
              ]

    facets = {
        # use bucket aggregations to define facets
        'year': TermsFacet(field='years'),
        'tech_package': TermsFacet(field='tech_package'),
        'overall_yield_obs': HistogramFacet(field='overall_yield_obs', interval=10),
        'brand': TermsFacet(field='proper_brand'),
    }

    def __init__(self, query=None, filters={}, sort=(), index=None,
                 location=None, distance=None):
        if index:
            self.index = index
        if location:
            self.location = location
        if distance:
            self.distance = distance
        super().__init__(query, filters, sort)


    def query(self, search, query):
        """
        Add query part to ``search``.
        :param search :     Search class
        :param query :      text search term
        """
        ns = deepcopy(search)
        if self.location:
            # ns = Search(index='corn')
            # qry = ns.query('geo_distance', distance="20km", location=[-88, 40]).sort('-overall_yield_adv', '_score').query("match", tech_package="VT2P")
            # resp = qry.execute() --> hits in the corn index that are within 20km of the location.  can chain query after
            ns = ns.query("geo_distance", distance=self.distance, location=self.location)

        if query:
            if self.fields:
                q_objects = None
                for i, field in enumerate(self.fields):
                    if i == 0:
                        q_objects = Q("wildcard", **{field: '*' + query + '*'})
                    else:
                        q_objects |= Q("wildcard", **{field: '*' + query + '*'})
                return ns.query(q_objects)
            else:
                # return ns.query("multi_match", query=query)
                return ns.query("wildcard", query=query)
        return ns


class CornFacetedSearch(SeedFacetedSearch):

    def __init__(self, *args, **kwargs):
        self.facets.update({'maturity_range':
                                RangeFacet(field="maturity", ranges=[("< 85", (0, 84)),
                                                                     ("85-94", (85, 94)),
                                                                     ("95-104", (95, 104)),
                                                                     ("105-114", (105, 114)),
                                                                     ("115-124", (115, 124)),
                                                                     (">124", (125, None)),
                                                                     ]
                                           )
                            })
        super().__init__(*args, **kwargs)


class SoyFacetedSearch(SeedFacetedSearch):

    def __init__(self, *args, **kwargs):
        self.facets.update({'maturity_range':
                                RangeFacet(field="maturity", ranges=[("Group 0", (0, 0.99)),
                                                                     ("Group 1", (1, 1.99)),
                                                                     ("Group 2", (2, 2.99)),
                                                                     ("Group 3", (3, 3.99)),
                                                                     ("Group 4", (4, 4.99)),
                                                                     ("Group 5", (5, 5.99)),
                                                                     ("Group 6", (6, 6.99)),
                                                                     ("Group 7", (7, 7.99)),
                                                                     ]
                                           )
                            })
        super().__init__(*args, **kwargs)


def aggregate_trials(search):
    return search.aggs.metric('avg_yield', 'avg', field='value_yield')


class TrialFacetedSearch(FacetedSearch):
    "Search focused on returning a seed (by id) trials with some filter and aggregate added as we go"

    def __init__(self, query=None, filters={}, sort=(), index=None, seed_id=None,
                 location=[], distance=DEFAULT_LOCATION_QUERY_DISTANCE):
        self.index = index
        self.seed_id = seed_id
        self.location = location
        self.distance = distance
        super().__init__(query, filters, sort)

    def limit_source(self, search):
        # remove value_* fields from the documents returned, we only want display ready values for the front end
        s = search.source(excludes=['value_*'])
        return s

    def query(self, search, query):
        """
        Add query part to ``search``.
        :param search :     Search class
        :param query :      text search term
        """
        ns = search
        # override to always query the seed_id
        if self.location:
            print('\nLocation query:  ', self.distance, self.location)
            ns = search.query("geo_distance", distance=self.distance, location=self.location)
        ns = ns.query("term", product_id=self.seed_id)
        # remove value_* fields from the documents returned, we only want display ready values for the front end
        ns = ns.source(excludes=['value_*'])
        # aggregations occur in place - no copy returned  https://elasticsearch-dsl.readthedocs.io/en/latest/search_dsl.html#the-search-object
        aggregate_trials(ns)
        return ns


class CornTrialFacetedSearch(TrialFacetedSearch):
    pass


class SoyTrialFacetedSearch(TrialFacetedSearch):
    pass

