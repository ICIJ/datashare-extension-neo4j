# Datashare Extension for Neo4j

## Dependencies
### Main
Users are expected to have the following dependencies on their file system:  
- [Python](https://www.python.org/downloads/) `>3.8,<4.0`
- [poetry](https://python-poetry.org/) (see `./neo4j install_peotry`)
- JDK `>= 11`

### Test
- Docker

## Commands

The `neo4j` script allows to run commands for this repository.
Some commands may require to specify a project using the `-p` flag. 

### Setting up
All projects:
```bash
./neo4j setup
```

only the Python app:
```bash
./neo4j setup -p neo4j_app
```

### Building

All projects:
```bash
./neo4j build
```

only the Python app:
```bash
./neo4j build -p neo4j_app
```

### Start elasticsearch
On `9200`:
```bash
./neo4j run_test_elasticsearch
```
or
```bash
./neo4j run_test_elasticsearch --elasticsearch-port 9999
```
