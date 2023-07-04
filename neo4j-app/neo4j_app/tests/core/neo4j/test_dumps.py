import xml.etree.ElementTree as e_t
from io import StringIO

import neo4j
import pytest
import pytest_asyncio

from neo4j_app.core.neo4j.dumps import dump_graph
from neo4j_app.core.objects import DumpFormat
from neo4j_app.tests.conftest import xml_elements_equal


@pytest_asyncio.fixture(scope="module")
async def _populate_neo4j(
    neo4j_test_driver_module: neo4j.AsyncDriver,
) -> neo4j.AsyncDriver:
    query = """
CREATE (TheMatrix:Movie {title:'The Matrix'})
CREATE (Keanu:Person {name:'Keanu Reeves', born:1964})
CREATE (Keanu)-[:ACTED_IN {roles:['Neo']}]->(TheMatrix);
"""
    async with neo4j_test_driver_module.session() as sess:
        await sess.run(query)
    yield neo4j_test_driver_module


@pytest.mark.asyncio
async def test_dump_full_graph_to_graphml(_populate_neo4j: neo4j.AsyncDriver):
    # pylint: disable=invalid-name
    # Given
    driver = _populate_neo4j
    dump_format = DumpFormat.GRAPHML

    # When
    output = StringIO()
    async for line in dump_graph(
        dump_format=dump_format,
        neo4j_driver=driver,
        neo4j_db=neo4j.DEFAULT_DATABASE,
        query=None,
    ):
        output.write(line)

    # Then
    expected_keys = """<?xml version="1.0" encoding="UTF-8"?>
<graphml xmlns="http://graphml.graphdrawing.org/xmlns" \
xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" \
xsi:schemaLocation="http://graphml.graphdrawing.org/xmlns http\
://graphml.graphdrawing.org/xmlns/1.0/graphml.xsd">
<key id="born" for="node" attr.name="born"/>
<key id="name" for="node" attr.name="name"/>
<key id="title" for="node" attr.name="title"/>
<key id="TYPE" for="node" attr.name="TYPE"/>
<key id="labels" for="node" attr.name="labels"/>
<key id="roles" for="edge" attr.name="roles"/>
<key id="label" for="edge" attr.name="label"/>
<key id="TYPE" for="edge" attr.name="TYPE"/>
</graphml>
"""
    ns = {"": "http://graphml.graphdrawing.org/xmlns"}

    root = e_t.fromstring(output.getvalue())
    keys = root.findall("key", namespaces=ns)
    expected_root = e_t.fromstring(expected_keys)
    expected_keys = expected_root.findall("key", namespaces=ns)
    assert len(keys) == len(expected_keys)
    assert all(xml_elements_equal(a, e) for a, e in zip(keys, expected_keys))
    nodes = root.findall(".graph/node", namespaces=ns)
    assert len(nodes) == 2
    edges = root.findall(".graph/edge", namespaces=ns)
    assert len(edges) == 1


@pytest.mark.asyncio
async def test_should_dump_subgraph_to_graphml(_populate_neo4j: neo4j.AsyncDriver):
    # pylint: disable=invalid-name
    # Given
    driver = _populate_neo4j
    dump_format = DumpFormat.GRAPHML
    query = """MATCH (person:Person { name:'Keanu Reeves' })
RETURN person;
"""

    # When
    output = StringIO()
    async for line in dump_graph(
        dump_format=dump_format,
        neo4j_driver=driver,
        neo4j_db=neo4j.DEFAULT_DATABASE,
        query=query,
    ):
        output.write(line)

    # Then
    expected_keys = """<?xml version="1.0" encoding="UTF-8"?>
<graphml xmlns="http://graphml.graphdrawing.org/xmlns" \
xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" \
xsi:schemaLocation="http://graphml.graphdrawing.org/xmlns http\
://graphml.graphdrawing.org/xmlns/1.0/graphml.xsd">
<key id="born" for="node" attr.name="born"/>
<key id="name" for="node" attr.name="name"/>
<key id="TYPE" for="node" attr.name="TYPE"/>
<key id="labels" for="node" attr.name="labels"/>
<key id="TYPE" for="edge" attr.name="TYPE"/>
</graphml>
    """
    ns = {"": "http://graphml.graphdrawing.org/xmlns"}

    root = e_t.fromstring(output.getvalue())
    keys = root.findall("key", namespaces=ns)
    expected_root = e_t.fromstring(expected_keys)
    expected_keys = expected_root.findall("key", namespaces=ns)
    assert len(keys) == len(expected_keys)
    assert all(xml_elements_equal(a, e) for a, e in zip(keys, expected_keys))
    nodes = root.findall(".graph/node", namespaces=ns)
    assert len(nodes) == 1
    names = root.findall(".graph//data[@key='name']", namespaces=ns)
    assert len(names) == 1
    assert names[0].text == "Keanu Reeves"


@pytest.mark.asyncio
async def test_should_dump_full_graph_to_cypher(_populate_neo4j: neo4j.AsyncDriver):
    # pylint: disable=invalid-name
    # Given
    driver = _populate_neo4j
    dump_format = DumpFormat.CYPHER

    # When
    output = StringIO()
    async for line in dump_graph(
        dump_format=dump_format, neo4j_driver=driver, neo4j_db=neo4j.DEFAULT_DATABASE
    ):
        output.write(line)

    # Then
    cypher_statements = output.getvalue()
    assert 'properties:{title:"The Matrix"}}' in cypher_statements
    assert 'properties:{born:1964, name:"Keanu Reeves"}}' in cypher_statements


@pytest.mark.asyncio
async def test_should_dump_subgraph_to_cypher(_populate_neo4j: neo4j.AsyncDriver):
    # pylint: disable=invalid-name
    # Given
    driver = _populate_neo4j
    dump_format = DumpFormat.CYPHER
    query = """MATCH (person:Person { name:'Keanu Reeves' })
RETURN person;
"""

    # When
    output = StringIO()
    async for line in dump_graph(
        dump_format=dump_format,
        neo4j_driver=driver,
        neo4j_db=neo4j.DEFAULT_DATABASE,
        query=query,
    ):
        output.write(line)

    # Then
    cypher_statements = output.getvalue()
    assert 'properties:{title:"The Matrix"}}' not in cypher_statements
    assert 'properties:{born:1964, name:"Keanu Reeves"}}' in cypher_statements


@pytest.mark.asyncio
async def test_should_raise_for_invalid_dump_format(
    neo4j_test_driver_session: neo4j.AsyncDriver,
):
    # Given
    driver = neo4j_test_driver_session
    dump_format = "UNKNOWN_FORMAT"

    # When/Then
    with pytest.raises(
        ValueError, match='dump not supported for "UNKNOWN_FORMAT" format'
    ):
        async for _ in dump_graph(
            dump_format=dump_format,
            neo4j_driver=driver,
            neo4j_db=neo4j.DEFAULT_DATABASE,
        ):
            pass
