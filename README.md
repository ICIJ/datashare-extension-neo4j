# Datashare Extension for Neo4j

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
./neo4j run -p neo4j_app
```

### Running
