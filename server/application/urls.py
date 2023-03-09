from controller import *

urlpatterns = [
    {"ApiRouter": station_status_app, "prefix": "/station/status", "tags": ["海洋站状态"]},
]
