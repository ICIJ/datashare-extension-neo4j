# Datashare Extension for Neo4j

## Dependencies

### Main

Users are expected to have the following dependencies on their file system:

- [Python](https://www.python.org/downloads/) `>3.8,<4.0`
- [poetry](https://python-poetry.org/) (see `./neo4j install_peotry`)
- JDK `>= 11`

### Dev, test and build

- Docker
- [maven](https://maven.apache.org/) `>3.8,<4.0`

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

### Display the Python app documentation

After building the app:

```bash
./neo4j run -p neo4j_app
```

and then navigate to [http://localhost/8080/docs]`http://localhost/8080/docs`

### Start/stop test services

#### All services

```bash
./neo4j start_all_test_services
./neo4j stop_all_test_services
```

#### Elasticsearch

On `9200`:

```bash
./neo4j start_test_elasticsearch
./neo4j stop_test_elasticsearch
```

or

```bash
./neo4j start_test_elasticsearch --elasticsearch-port 9999
```

#### neo4j

```bash
./neo4j start_test_neo4j
./neo4j stop_test_neo4j
```