from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework.pagination import PageNumberPagination
from rest_framework import authentication, permissions
from django.views.generic.list import MultipleObjectMixin
from django.conf import settings

from efg.utility.general_utils import seq
from .search import *


from django.core.paginator import Paginator, Page, EmptyPage, PageNotAnInteger


class DSEPaginator(Paginator):
    """
    Override Django's built-in Paginator class to take in a count/total number of items;
    Elasticsearch provides the total as a part of the query results, so we can minimize hits.
    """
    def __init__(self, *args, **kwargs):
        super(DSEPaginator, self).__init__(*args, **kwargs)
        self.count = self.object_list.hits.total.value

    def page(self, number):
        # this is overridden to prevent any slicing of the object_list - Elasticsearch has
        # returned the sliced data already.
        number = self.validate_number(number)
        return Page(self.object_list, number, self)


class SearchSchemaView(APIView):
    "View the schema of the Faceted Searches to get the available fields and facets (categories)"

    def get(self, request, index, format=None):
        fctSearch = None
        payload = request.data
        if 'report' in index and 'corn' in index:
            fctSearch = CornReportSearch
        elif 'report' in index and 'soy' in index:
            fctSearch = SoyReportSearch
        elif 'corn' in index:
            fctSearch = CornFacetedSearch
        elif 'soy' in index:
            fctSearch = SoyFacetedSearch
        if not fctSearch:
            Response(status=404)

        search = fctSearch(query=None, filters={}, index=index, location=[], distance=None)
        response = search.execute()

        # print('\nHit Summary\n', response.hits)
        # for hit in response:
        #     print(hit.meta.score, hit)
        # print('\nFacet Summary\n')
        # for i, (tag, count, selected) in enumerate(response.facets.brand):
        #     print(i, tag, ' (SELECTED):' if selected else ':', count)

        d = response.to_dict()
        # print(d)
        d.pop('_faceted_search')
        # facets = response.facets
        # print('\nFacets: ', facets.to_dict())
        d.pop('hits')
        return Response(d)



class SeedSearchView(APIView):
    """Search for seeds by location + faceted filters"""
    # permission_classes = [permissions.IsAuthenticated]

    def sort_priority(self, sort_value):
        if sort_value == 'count':
            search = ('-top30_cnt', '-yield_obs', 'brand')
        elif sort_value == 'alpha':
            search = ('brand', 'name')
        else:
            search = ('-top30_pct', '-yield_obs', 'brand')
        return search

    def get(self, request, index, format=None):
        "request payload contains JSON of keys and values that we want searched"
        fctSearch = None
        payload = request.data
        query = payload.get('query')
        filters = payload.get('filters', {})
        location = payload.get('location', [])
        distance = payload.get('distance', DEFAULT_LOCATION_QUERY_DISTANCE)
        maturity_range = payload.get('maturity_range', None)
        crop_code = None

        #sort param handling
        sort_value = payload.get('sort_value', 'top30pct')
        sort_value = request.GET.get('sort_value', sort_value)

        # print(f"'\nIndex:  {index}\nPayload:   {payload}\nQuery: {query}\nFilters: {filters}\nMaturityRange: {maturity_range}")
        if 'corn' in index:
            fctSearch = CornFacetedSearch
            crop_code = 'B'
        if 'soy' in index:
            fctSearch = SoyFacetedSearch
            crop_code = 'S'
        if not fctSearch:
            Response(status=404)
        # in this view, the maturity range can be set as a filter directly because the SeedFacetedSearch has it as a facet
        if maturity_range:
            if crop_code == 'S':
                mats = seq(float(maturity_range[0]), float(maturity_range[1]), step=0.01, digit=2)
            else:
                mats = seq(float(maturity_range[0]), float(maturity_range[1]), step=1, digit=0)
            filters.update({'maturity': mats})

        sort_by = self.sort_priority(sort_value)
        search = fctSearch(query=query, filters=filters, index=index, location=location, distance=distance,
                           sort=sort_by)

        if settings.ES_USE_PAGINATION:
            # pagination
            page = int(request.GET.get('page', '1'))
            per_page = int(request.GET.get('per_page', settings.ES_PAGINATION_DEFAULT_LENGTH))
            start = (page - 1) * settings.ES_PAGINATION_DEFAULT_LENGTH
            end = start + settings.ES_PAGINATION_DEFAULT_LENGTH

            search = search[start:end]
            response = search.execute()
            paginator = DSEPaginator(response, per_page or settings.ES_PAGINATION_DEFAULT_LENGTH)
            try:
                response = paginator.page(page)
            except PageNotAnInteger:
                response = paginator.page(1)
            except EmptyPage as e:
                response = paginator.page(paginator.num_pages)
            d = response.object_list.to_dict()
            d.update({'num_pages': paginator.num_pages,
                      'next_page': response.next_page_number() if (page > 1 and page < paginator.num_pages) else None,
                      'page': page,
                      'previous_page': response.previous_page_number() if page > 1 else None,
                      'per_page': per_page or settings.ES_PAGINATION_DEFAULT_LENGTH,
                      })
        else:
            response = search.execute()
            d = response.to_dict()

        d.pop('_faceted_search')
        d.pop('aggregations')
        return Response(d)


class TrialsSearchView(APIView):
    """Retrieve all trials data for a particular seed -- with facets for filtering at some point
    NOTE - not paginated, all trials returned up to the ES_MAX_RESULTS_COUNT setting
    """

    def search(self, request, index, seed_id):
        fctSearch = None
        payload = request.data
        seed_id = payload.get('seed_id') or seed_id
        filters = payload.get('filters', {})
        location = payload.get('location', [])
        fctSearch = CornTrialFacetedSearch
        if 'soy' in index:
            fctSearch = SoyTrialFacetedSearch
        if not fctSearch:
            Response(status=404)

        search = fctSearch(seed_id=seed_id, filters=filters, index=index, location=location)
        response = search.execute()
        return response

    def get(self, request, index, seed_id, format=None):
        fctSearch = None
        payload = request.data
        seed_id = payload.get('seed_id') or seed_id
        filters = payload.get('filters', {})
        location = payload.get('location', [])
        fctSearch = CornTrialFacetedSearch
        if 'soy' in index:
            fctSearch = SoyTrialFacetedSearch
        if not fctSearch:
            Response(status=404)

        search = fctSearch(seed_id=seed_id,
                           filters=filters, index=index, location=location)[0:settings.ES_MAX_RESULTS_COUNT]
        response = search.execute()

        d = response.to_dict()
        d.pop('_faceted_search')
        return Response(d)


class ReportSearchView(APIView):
    """Search for Harvest Reports by location + faceted filters
    NOTE - not paginated, returns all results up to the ES_MAX_RESULTS_COUNT setting
    """
    # permission_classes = [permissions.IsAuthenticated]

    def get(self, request, index, format=None):
        "request payload contains JSON of keys and values that we want searched"
        fctSearch = None
        crop_code = None
        payload = request.data
        query = payload.get('query')
        filters = payload.get('filters', {})
        location = payload.get('location', [])
        distance = payload.get('distance', DEFAULT_LOCATION_QUERY_DISTANCE)
        maturity_range = payload.get('maturity_range', None)
        #print(f"'\nIndex:  {index}\nPayload:   {payload}\nQuery: {query}\nFilters: {filters}")
        if 'corn' in index:
            fctSearch = CornReportSearch
            crop_code = 'B'
        if 'soy' in index:
            fctSearch = SoyReportSearch
            crop_code = 'S'
        if 'silage' in index:
            fctSearch = SilageReportSearch
            crop_code = 'L'
        if not fctSearch:
            Response(status=404)
        if maturity_range:
            if crop_code == 'S':
                maturity_range = seq(float(maturity_range[0]), float(maturity_range[1]), step=0.01, digit=2)
            else:
                maturity_range = seq(float(maturity_range[0]), float(maturity_range[1]), step=1, digit=0)
        search = fctSearch(query=query, filters=filters, index=index, location=location, distance=distance,
                           maturity_range=maturity_range)[0:settings.ES_MAX_RESULTS_COUNT]

        response = search.execute()
        d = response.to_dict()
        d.pop('_faceted_search')
        return Response(d)