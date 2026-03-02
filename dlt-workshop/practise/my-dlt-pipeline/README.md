# how did i set this up:

# i configured my dlt mcp server 
# i installed the pyenv 3.11.9 because it said in the requirements to use python 3.11+
# i set the local python version as 3.11.9 with pyenv local 3.11.9 in the dlt-workshop directory for all the dlt activities which created the .python-version file 
# i created this folder here my-dlt-pipeline 
# i initialized it as uv managed python project with uv init 
# it is supposed to be a open library scuffolding so i did:
# i installed uv add "dlt[workspace]" see https://dlthub.com/context/source/open-library 
# i executed uv run dlt init dlthub:open_library duckdb see https://dlthub.com/context/source/open-library
# then i use the suggested promt to generate everything and the i test: uv run open_library_pipeline.py
# when its done i need to test: i run uv run dlt pipeline open_library_pipeline show to setup a dashboard 

# exploring the data with marimo and ibis https://dlthub.com/docs/general-usage/dataset-access/marimo

# promt example: using the above reference, create bar chart showing number of books per author, create line chart showing books over time