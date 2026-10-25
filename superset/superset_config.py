import os

SECRET_KEY = os.environ["SUPERSET_SECRET_KEY"]

# Local single-user setup: allow the bootstrap script to call the REST API.
WTF_CSRF_ENABLED = False
FEATURE_FLAGS = {"ENABLE_TEMPLATE_PROCESSING": True}
