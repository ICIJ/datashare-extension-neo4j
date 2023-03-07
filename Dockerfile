# Global build args with placeholders in case they are not specified from outside (value don't matter much)
ARG ELASTICSEARCH_VERSION=7.17.9
ARG NEO4J_VERSION=4.4.17
ARG NEO4J_IMAGE=neo4j
ARG DOCKER_PLATFORM="linux/amd64"

# Base image
FROM phusion/baseimage:jammy-1.0.1 as base
USER $DOCKER_UID:$DOCKER_GID
ENV HOME="/home/dev"
RUN add-apt-repository --yes ppa:deadsnakes/ppa
RUN --mount=type=cache,target=/var/cache/apt  \
    apt-get -y update && \
    apt-get -y install \
        libssl-dev \
        maven \
        python3.8 \
        python3.8-dev \
	    openjdk-11-jdk \
        python3.8-distutils \
        python3.8-venv \
        wget

# Python
RUN wget https://bootstrap.pypa.io/get-pip.py \
    && python3 get-pip.py \
    && python3 -m pip install --upgrade pip \
    && python3 -m pip install virtualenv
RUN curl -sSL https://install.python-poetry.org | POETRY_HOME=$HOME/.local/share/pypoetry POETRY_VERSION=1.3.1 python3.8 -
ENV PATH="$HOME/.local/share/pypoetry/bin:$PATH"

ENV LANGUAGE="en"
ENV LANG="en_US.UTF-8"
ENV TERM xterm-256color
WORKDIR $HOME

ADD neo4j $HOME/

# Python base (we don't copy java assets not to destroy the cache in case of java changes)
FROM base as base-python
ADD qa/python $HOME/qa/python
ADD version $HOME
ADD neo4j-app $HOME/neo4j-app

# Java base (we don't copy python assets not to destroy the cache in case of python changes)
FROM base as base-java
# Lets make maven point to the cached dependency dirs
RUN mkdir $HOME/.m2 \
    &&sed -i -- "s@</settings>@<localRepository>$HOME/.m2</localRepository></settings>@g" \
        $(mvn --version | grep "Maven home" |sed "s@Maven home: *@@g")/conf/settings.xml
ADD qa/java $HOME/qa/java
ADD pom.xml $HOME
ADD version $HOME
ADD src $HOME/src

FROM base as base-script
ADD bins/.gitignore $HOME/bins/.gitignore
ADD neo4j-app/pyproject.toml $HOME/neo4j-app/pyproject.toml
ADD src/main/resources/manifest.txt $HOME/src/main/resources/manifest.txt
ADD pom.xml $HOME
ADD version $HOME
ADD scripts $HOME/scripts

# We use placeholders here
FROM ${NEO4J_IMAGE}:${NEO4J_VERSION} as neo4j
ADD scripts/neo4j_healthcheck /var/lib/neo4j
RUN sed -i -- 's/dbms.directories.import=import/#dbms.directories.import=import/g' conf/neo4j.conf

FROM --platform=$DOCKER_PLATFORM docker.elastic.co/elasticsearch/elasticsearch:${ELASTICSEARCH_VERSION} AS elasticsearch
