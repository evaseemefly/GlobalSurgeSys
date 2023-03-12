# from controller import *
from controller.station_status import app as station_status_app
from controller.station_surge import app as station_surge_app

urlpatterns = [
    {"ApiRouter": station_status_app, "prefix": "/station/status", "tags": ["海洋站状态"]},
    {"ApiRouter": station_surge_app, "prefix": "/station/surge", "tags": ['海洋站潮位实况']},
]
