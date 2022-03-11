from django.urls import path

from . import views

urlpatterns = (
    path('/v1/<str:index>/reports/', views.ReportSearchView.as_view(), name='search_report_facet'),
    path('/v1/<str:index>/seeds/', views.SeedSearchView.as_view(), name='search_seed_facet'),
    path('/v1/<str:index>/trials/', views.SimpleTrialsView.as_view(), name='search_trial_facet'),
    path('/v1/<str:index>/schema/', views.SearchSchemaView.as_view(), name='search_seed_schema'),
)