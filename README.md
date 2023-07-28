# Datashare Extension for Neo4j

## Dependencies

### Main

Users are expected to have the following dependencies on their file system:

- [Python](https://www.python.org/downloads/) `>3.9,<4.0`
- [poetry](https://python-poetry.org/) (see `./neo4j install_peotry`)
- JDK `>= 11`

### Dev, test and build

- Docker
- [maven](https://maven.apache.org/) `>3.8,<4.0`
- [yarn](https://yarnpkg.com/) `>1.22,<2.0`
- [npm](https://www.npmjs.com/) `>1.22,<2.0`
- [git](https://git-scm.com/)

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

### Format and code styles
To run code formatting use:
```bash
./neo4j format
```
it will run Python code base formatting.

The Java code style guides are located in [checkstyle.xml](qa/java/checkstyle.xml).
You can then use the [Checkstyle Intellij plugin](https://plugins.jetbrains.com/plugin/1065-checkstyle-idea) to
highlight code style warnings and errors.

Additionally once the plugin has been installed you can import the [checkstyle.xml](qa/java/checkstyle.xml) as a 
Java Code Style template ` Settings|Editor|Code Style|Manage...|Import..`.
This will allow you to automatically format you code according to the style guide when running the `Reformat Code`
action in Intellij.

Be aware that reformatting code will only solve formatting style issues and other issues might be left.

`Checkstyle` is also available for other IDEs and you should be able to integrate it in your preferred IDE.


### Display the Python app documentation

After building the app:

```bash
./neo4j run -p neo4j_app
```

this will use the default app configuration. However is to provide the path to a 
[Datashare](https://github.com/ICIJ/datashare) `properties`. The location of this file can be found in Datashare's 
settings. 

and then navigate to [http://localhost/8080/docs](http://localhost/8080/docs)

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

### Run test
#### Local test
To run all tests locally run:
```bash
./neo4j test
```

To run a specify test  run:
```bash
./neo4j test -p neo4j_app
```
available tests are `neo4j_app`, `neo4j_app_format`, `neo4j_extension` and `neo4j_extension_format` 

#### Docker tests
**Docker tests require you to launch tests services first using `./neo4j start_all_test_services`**, then you can use
any of the above test commands replacing `test` by `docker_test`, for instance:
```bash
./neo4j docker_test -p neo4j_app
```
