from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import authentication, permissions

from elasticsearch import Elasticsearch
from elasticsearch_dsl.connections import connections
from elasticsearch_dsl import Search
from elasticsearch_dsl import FacetedSearch, TermsFacet, RangeFacet, HistogramFacet

import logging

logger = logging.getLogger(__name__)

DEFAULT_LOCATION_QUERY_DISTANCE = "20km"


class SeedFacetedSearch(FacetedSearch):
    doc_types = ['_doc']
    # fields that should be searched
    fields = ['proper_brand',
              'tech_package',
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
        ns = search
        if self.location:
            # ns = Search(index='corn')
            # qry = ns.query('geo_distance', distance="20km", location=[-88, 40]).sort('-overall_yield_adv', '_score').query("match", tech_package="VT2P")
            # resp = qry.execute() --> hits in the corn index that are within 20km of the location.  can chain query after
            ns = search.query("geo_distance", distance=self.distance, location=self.location)
        if query:
            if self.fields:
                return ns.query("multi_match", fields=self.fields, query=query)
            else:
                return ns.query("multi_match", query=query)
        return ns


class CornFacetedSearch(SeedFacetedSearch):
    index = 'corn'
    location = None
    distance = DEFAULT_LOCATION_QUERY_DISTANCE


class SoyFacetedSearch(SeedFacetedSearch):
    index = 'soy'
    location = None
    distance = DEFAULT_LOCATION_QUERY_DISTANCE

    def __init__(self, *args, **kwargs):
        self.facets.update({'maturity_range':
                                RangeFacet(field="maturity", ranges=[("Group 0", (0, 0.99)),
                                                                     ("Group 1", (1, 1.99)),
                                                                     ]
                                           )
                            })
        super().__init__(*args, **kwargs)


class SearchSchemaView(APIView):

    def get(self, request, index, format=None):
        fctSearch = None
        payload = request.data
        if 'corn' in index:
            fctSearch = CornFacetedSearch
        if 'soy' in index:
            fctSearch = SoyFacetedSearch
        if not fctSearch:
            Response(status=404)

        search = fctSearch(query=None, filters={}, index=index, location=[], distance=None)
        response = search.execute()
        print('\nHit Summary\n', response.hits)
        for hit in response:
            print(hit.meta.score, hit)
        print('\nFacet Summary\n')
        for i, (tag, count, selected) in enumerate(response.facets.tech_package):
            print(i, tag, ' (SELECTED):' if selected else ':', count)

        # for (month, count, selected) in response.facets.publishing_frequency:
        #     print(month.strftime('%B %Y'), ' (SELECTED):' if selected else ':', count)
        d = response.to_dict()
        d.pop('_faceted_search')
        print(d)
        facets = response.facets
        print('\nFacets: ', facets.to_dict())
        d.pop('hits')
        return Response(d)



class SeedSearchView(APIView):
    """Search for seeds by location + faceted filters"""
    # permission_classes = [permissions.IsAuthenticated]

    def get(self, request, index, format=None):
        "request payload contains JSON of keys and values that we want searched"
        fctSearch = None
        payload = request.data
        query = payload.get('query')
        filters = payload.get('filters', {})
        location = payload.get('location', [])
        distance = payload.get('distance', DEFAULT_LOCATION_QUERY_DISTANCE)
        #print(f"'\nIndex:  {index}\nPayload:   {payload}\nQuery: {query}\nFilters: {filters}")
        if 'corn' in index:
            fctSearch = CornFacetedSearch
        if 'soy' in index:
            fctSearch = SoyFacetedSearch
        if not fctSearch:
            Response(status=404)

        search = fctSearch(query=query, filters=filters, index=index, location=location, distance=distance)
        response = search.execute()
        #print('\nHit Summary\n', response.hits)
        for hit in response:
            print(hit.meta.score, hit)
        #print('\nFacet Summary\n')
        for i, (tag, count, selected) in enumerate(response.facets.tech_package):
            print(i, tag, ' (SELECTED):' if selected else ':', count)

        # for (month, count, selected) in response.facets.publishing_frequency:
        #     print(month.strftime('%B %Y'), ' (SELECTED):' if selected else ':', count)
        d = response.to_dict()
        d.pop('_faceted_search')
        #print(d)
        facets = response.facets
        #print('\n', facets.to_dict())
        return Response(d)

