import logging

import cdsapi

logging.basicConfig(level=logging.DEBUG, format="%(name)s %(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

BASE_URL = "https://cds-beta.climate.copernicus.eu/api"


class ERA5:
    def __init__(self, key: str):
        self.client = cdsapi.Client(
            key=key,
        )
