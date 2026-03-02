"""Template for building a `dlt` pipeline to ingest data from a REST API."""

import dlt
from dlt.sources.rest_api import rest_api_resources
from dlt.sources.rest_api.typing import RESTAPIConfig


@dlt.source
def open_library_rest_api_source():
    """Define dlt resources for the Open Library REST API."""
    config: RESTAPIConfig = {
        "client": {
            # Base URL for the Open Library API
            "base_url": "https://openlibrary.org/",
        },
        "resource_defaults": {
            "endpoint": {
                # Default query parameters applied to all endpoints
                "params": {
                    "limit": 100,
                },
            },
        },
        "resources": [
            {
                "name": "harry_potter_search",
                "endpoint": {
                    # Open Library search API
                    "path": "search.json",
                    "params": {
                        # Query for Harry Potter books
                        "q": "harry potter",
                    },
                    # The search results are returned under the "docs" key
                    "data_selector": "docs",
                    # Treat the response as a single page (no pagination configured)
                    "paginator": {
                        "type": "single_page",
                    },
                },
            },
        ],
    }

    yield from rest_api_resources(config)


pipeline = dlt.pipeline(
    pipeline_name='open_library_pipeline',
    destination='duckdb',
    # `refresh="drop_sources"` ensures the data and the state is cleaned
    # on each `pipeline.run()`; remove the argument once you have a
    # working pipeline.
    refresh="drop_sources",
    # show basic progress of resources extracted, normalized files and load-jobs on stdout
    progress="log",
)


if __name__ == "__main__":
    load_info = pipeline.run(open_library_rest_api_source())
    print(load_info)  # noqa: T201
