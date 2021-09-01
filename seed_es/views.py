from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import authentication, permissions

from .search import *


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


class SimpleTrialsView(APIView):
    """Retrieve all trials data for a particular seed -- with facets for filtering at some point"""

    def get(self, request, index, format=None):
        fctSearch = None
        payload = request.data
        seed_id = payload.get('seed_id')
        filters = payload.get('filters', {})
        location = payload.get('location', [])
        fctSearch = CornTrialFacetedSearch
        if 'soy' in index:
            fctSearch = SoyTrialFacetedSearch
        if not fctSearch:
            Response(status=404)

        search = fctSearch(seed_id=seed_id, filters=filters, index=index, location=location)
        response = search.execute()
        d = response.to_dict()
        d.pop('_faceted_search')
        return Response(d)