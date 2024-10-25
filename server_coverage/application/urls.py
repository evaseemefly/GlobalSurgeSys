# from controller import *
from controllers.coverage import app as coverage_app

urlpatterns = [
    {"ApiRouter": coverage_app, "prefix": "/coverage", "tags": ["栅格图层服务"]},
]
