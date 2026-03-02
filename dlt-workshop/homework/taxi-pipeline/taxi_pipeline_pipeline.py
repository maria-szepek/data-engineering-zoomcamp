"""Template for building a `dlt` pipeline to ingest data from a REST API."""

import dlt
from dlt.sources.rest_api import rest_api_resources
from dlt.sources.rest_api.typing import RESTAPIConfig

BASE_URL = "https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api"

@dlt.source
def taxi_pipeline_rest_api_source():
    """Define dlt resources from REST API endpoints."""
    # construct declarative configuration for the taxi API
    # the API returns JSON lists of trips, 1 000 records per page and
    # uses a simple `page` query parameter that increments until an
    # empty list is returned. there is no authentication required.
    config: RESTAPIConfig = {
        "client": {
            # base URL for the REST API
            "base_url": BASE_URL,
        },
        # defaults applied to all resources/endpoints
        "resource_defaults": {
            "endpoint": {
                # include a page size query just to be explicit
                "params": {"page_size": 1000},
            }
        },
        "resources": [
            {
                # one resource covering the taxi trips
                "name": "taxi_trips",
                "endpoint": {
                    # the base URL already returns the trip list
                    "path": "",
                    # configure pagination by page number
                    "paginator": {
                        "type": "page_number",
                        "page_param": "page",
                        "base_page": 1,
                        "stop_after_empty_page": True,
                        # API doesn't return a total count field
                        "total_path": None,
                    },
                },
            },
        ],
    }

    yield from rest_api_resources(config)


pipeline = dlt.pipeline(
    pipeline_name='taxi_pipeline',
    destination='duckdb',
    # `refresh="drop_sources"` ensures the data and the state is cleaned
    # on each `pipeline.run()`; remove the argument once you have a
    # working pipeline.
    refresh="drop_sources",
    # show basic progress of resources extracted, normalized files and load-jobs on stdout
    progress="log",
)


if __name__ == "__main__":
    load_info = pipeline.run(taxi_pipeline_rest_api_source())
    print(load_info)  # noqa: T201
