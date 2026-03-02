# https://openlibrary.org/developers/api
# https://openlibrary.org/dev/docs/api/search # good link

# author name for example comes as list, not good for relational database for example

# example 
def openlibrary_source(query: str = "harry potter"):

    return rest_api_source({
        "client": {
            "base_url": "https://openlibrary.org",
            # authentication would happen here if needed
        },
        "resource_defaults": {
            "primary_key": "key",
            "write_disposition": "replace",
        },
        "resources": [
            {
                "name": "books",
                "endpoint": {
                    "path": "search.json",
                    "params": {
                        "q": query,
                        "limit": 100,
                    },
                    "data_selector": "docs",
                    "paginator": {
                        "type": "offset",
                        "limit": 100,
                        "offset_param": "offset",
                        "limit_param": "limit",
                        "total_path": "numFound",
                    },
                },
            },
        ],
    })

# standard: pipeline.run = pipeline.extract + pipeline.normalize + pipeline.load

# The dlt workspace workflow 

# 1. using llm scaffolds, get the instructions tailored for a specific source to minimize debugging

# 2. quality ensurance using dlt dashboard, dlt mcp server, or dlt cli

# 3. create reports and transformations 
