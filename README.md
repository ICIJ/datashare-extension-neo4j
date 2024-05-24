# Datashare Extension for Neo4j [![CircleCI](https://dl.circleci.com/status-badge/img/gh/ICIJ/datashare-extension-neo4j/tree/main.svg?style=svg)](https://dl.circleci.com/status-badge/redirect/gh/ICIJ/datashare-extension-neo4j/tree/main)

Create [Neo4j](https://neo4j.com/docs/getting-started/get-started-with-neo4j/graph-database/) graphs from your [Datashare](https://datashare.icij.org/) projects.

## Usage

Learn how to setup the plugin and use it reading [Datashare's documentation](https://icij.gitbook.io/datashare/usage/explore-the-neo4j-graph). 

## Development

This repository is a monorepo hosting:
- the Java backend extension of Datashare (`src`), this extension is mainly a wrapper around the Python Neo4j application
- the Python Neo4j backend application handling (`neo4j-app`)
- the Datashare Graph widget plugin frontend (`plugins/neo4j-graph-widget`)

### Dependencies

#### Core
Developers need the following dependencies on their operating system:

- [Python](https://www.python.org/downloads/) `>3.10.8,<4.0`
- [poetry](https://python-poetry.org/) (see `./neo4j install_peotry`)
- JDK `>= 11`
- [maven](https://maven.apache.org/) `>3.8,<4.0`

#### Frontend
- [yarn](https://yarnpkg.com/) `>1.22,<2.0`
- [npm](https://www.npmjs.com/) `>1.22,<2.0`

#### Test and build
- Docker
- [git](https://git-scm.com/)

### Dev commands

The `neo4j` script allows to run commands for this repository.
Some commands may require to specify a project using the `-p` flag.

#### Setting up

All projects:

```bash
./neo4j setup
```

only the Python app:

```bash
./neo4j setup -p neo4j_app
```

#### Building

All projects:

```bash
./neo4j build
```

only the Python app:

```bash
./neo4j build -p neo4j_app
```

#### Format and code styles
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



#### Testing

##### Start and stop test services

```bash
./neo4j start_all_test_services
./neo4j stop_all_test_services
```

To start/stop Elasticsearch only, run:

```bash
./neo4j start_test_elasticsearch
./neo4j stop_test_elasticsearch
```

to use a different port run:

```bash
./neo4j start_test_elasticsearch --elasticsearch-port 9999
```

To start/srop neo4j only, run:

```bash
./neo4j start_test_neo4j
./neo4j stop_test_neo4j
```


##### Run tests locally
To run all tests locally run:
```bash
./neo4j test
```

To run a specify test  run:
```bash
./neo4j test -p neo4j_app
```
available tests are `neo4j_app`, `neo4j_app_format`, `neo4j_extension` and `neo4j_extension_format` 

##### Run tests inside Docker (like in the CI)
You can use any of the above test commands replacing `test` by `docker_test` to run them inside a docker container.
For instance:
```bash
./neo4j docker_test -p neo4j_app
```
