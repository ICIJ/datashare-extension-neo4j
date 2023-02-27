# Base image
FROM --platform=$BUILDPLATFORM phusion/baseimage:jammy-1.0.1 as base

RUN add-apt-repository --yes ppa:deadsnakes/ppa

# TODO: handle user here...

# TODO: reduce this to the minimum...
RUN apt-get -y update && \
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
RUN curl -sSL https://install.python-poetry.org | POETRY_HOME=/opt/poetry POETRY_VERSION=1.3.1 python3 -
ENV PATH="/opt/poetry/bin:$PATH"

ENV HOME="/home/dev"
RUN mkdir $HOME
ENV LANGUAGE="en"
ENV LANG="en_US.UTF-8"
ENV TERM xterm-256color
WORKDIR $HOME

ADD neo4j $HOME/

# Python base (we don't copy java assets not to destroy the cache in case of java changes)
FROM base as base-python
# TODO: this does not seem to be the most efficient way of adding recursively
ADD neo4j-app $HOME/neo4j-app
ADD qa/python $HOME/qa/python

RUN ./neo4j setup -p neo4j_app

# Java base (we don't copy python assets not to destroy the cache in case of python changes)
FROM base as base-java
ADD pom.xml $HOME
ADD qa/java $HOME/qa/java
ADD src $HOME/src
