from elasticsearch_dsl import FacetedSearch, TermsFacet, RangeFacet, HistogramFacet, DateHistogramFacet

from elasticsearch import Elasticsearch
from elasticsearch_dsl.connections import connections
from elasticsearch_dsl import Search, Q
from copy import deepcopy


import logging

logger = logging.getLogger(__name__)

DEFAULT_LOCATION_QUERY_DISTANCE = "20km"
MATURITY_RANGE_DEFAULT = (0, 130)


class FIRSTFacetedSearch(FacetedSearch):
    """
     Base search class for FIRST searches for seeds, seed trials, and reports.  A faceted Search
     provides a categorization of the facets and counts of each facet in a schema call.
     This is helpful to derive the options for the Front End with an API.
    """
    doc_types = ['_doc']
    index = None
    location = None
    distance = DEFAULT_LOCATION_QUERY_DISTANCE
    maturity_range = None
    crop_code = None

    def __init__(self, query=None, filters={}, sort=(), index=None,
                 location=None, distance=DEFAULT_LOCATION_QUERY_DISTANCE,
                 maturity_range=None):
        if index:
            self.index = index
        if location:
            self.location = location
        if distance:
            self.distance = distance
        if maturity_range:
            self.maturity_range = maturity_range
        super().__init__(query, filters, sort)


    def query(self, search, query):
        """
        Add query part to ``search``.
        :param search :     Search class
        :param query :      text search term
        """
        ns = deepcopy(search)
        if self.maturity_range:
            return ns.filter('terms', maturity=self.maturity_range)

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


class SeedFacetedSearch(FIRSTFacetedSearch):
    # fields that should be searched
    fields = ['brand',
              # 'tech_package',
              "name",
              ]

    facets = {
        # use bucket aggregations to define facets
        'year': TermsFacet(field='years'),
        'tech_package': TermsFacet(field='tech_package'),
        'overall_yield_obs': HistogramFacet(field='overall_yield_obs', interval=10),
        'brand': TermsFacet(field='proper_brand', size=30),
        'maturity': TermsFacet(field='maturity', size=60)
    }

    # def search(self):
    #     # override search to update the results to 50
    #     s = super().search()
    #     return s


class CornFacetedSearch(SeedFacetedSearch):
    crop_code = 'B'
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
    crop_code = 'S'

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





class TrialFacetedSearch(FacetedSearch):
    "Search focused on returning a seed (by id) trials with some filter and aggregate added as we go"

    def __init__(self, query=None, filters={}, sort=(), index=None, seed_id=None,
                 location=[], distance=DEFAULT_LOCATION_QUERY_DISTANCE):
        self.index = index
        self.seed_id = seed_id
        self.location = location
        self.distance = distance
        super().__init__(query, filters, sort)

    @staticmethod
    def aggregate_trials(search):
        return search.aggs.metric('avg_yield', 'avg', field='value_yield')

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

        # aggregations occur in place - no copy returned  https://elasticsearch-dsl.readthedocs.io/en/latest/search_dsl.html#the-search-object
        self.aggregate_trials(ns)
        return ns

    def search(self):
        s = super().search()
        # override to always search on seed_id
        s.query("term", product_id=self.seed_id)
        # remove value_* fields from the documents returned, we only want display ready values for the front end
        s.source(excludes=['value_*'])
        return s


class CornTrialFacetedSearch(TrialFacetedSearch):
    pass


class SoyTrialFacetedSearch(TrialFacetedSearch):
    pass


class ReportFacetedSearch(FIRSTFacetedSearch):
    """Search focused on returning Harvest Reports of interest, principally by location
    Other required filters: year, state, maturity range, publish date/harvest date
    Future filters: soils, tillage, previous crop, population, irrigation, FIRST manager, CONV/Non-GMO
    """
    # fields that should be searched
    fields = ['sitename',
              "field_manager",
              "plot_host"
              ]

    facets = {
        # use bucket aggregations to define facets
        'year': TermsFacet(field='year'),
        'state': TermsFacet(field='state', size=50),
        'soil_texture': TermsFacet(field='soil_texture', size=20),
        'published': RangeFacet(field='pub_days_ago', ranges=[("new", (0, 2)),
                                                             ("week", (0, 8)),
                                                             ("2 weeks", (0, 15)),
                                                             ("month", (0, 32)),
                                                             ("any", (0, None)),
                                                            ])
    }


class CornReportSearch(ReportFacetedSearch):
    crop_code = 'B'


class SoyReportSearch(ReportFacetedSearch):
    crop_code = 'S'


class SilageReportSearch(ReportFacetedSearch):
    crop_code = 'L'