# lb-preserve-scripts

This repository collects scripts to put together a Neo4j database which unites LHCb production metadata, data bookkeeping path and software dependencies.

- `get-prod` queries Dirac to download the productions. It creates JSON dictionaries with all the important information from the production metadata and from the steps.
- `set-db` has an API to the database. It just loads software dependencies from a file and creates nodes
- `load-db` loads productions and creates nodes
