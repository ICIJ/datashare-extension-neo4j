import xml.etree.ElementTree as e_t
from io import StringIO
from typing import Dict

import neo4j
import pytest
import pytest_asyncio

from neo4j_app.core.neo4j.graphs import count_graph_nodes, dump_graph
from neo4j_app.core.objects import DumpFormat, GraphNodesCount
from neo4j_app.tests.conftest import TEST_PROJECT, xml_elements_equal


async def _create_docs(driver: neo4j.AsyncDriver, n: int):
    doc_ids = [f"doc-{i}" for i in range(n)]
    query = """UNWIND $docIds as docId
CREATE (:Document {id: docId})
"""
    await driver.execute_query(query, docIds=doc_ids)


async def _create_ents(driver: neo4j.AsyncDriver, n_ents: Dict[str, int]):
    ents = []
    for category, n in n_ents.items():
        labels = ["NamedEntity", category]
        for i in range(n):
            props = {"mentionNorm": f"ent-{i}"}
            mention_ids = [f"ent-{i}" for i in range(1, i + 2)]
            ents.append({"props": props, "labels": labels, "mentionIds": mention_ids})
    query = """UNWIND $ents as ent
CALL apoc.create.node(ent.labels, ent.props) YIELD node as ne
MATCH (doc:Document {id: 'doc-0'})
MERGE (ne)-[rel:APPEARS_IN {mentionIds: ent.mentionIds}]->(doc)
"""
    await driver.execute_query(query, ents=ents)


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
        project=neo4j.DEFAULT_DATABASE,
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
        project=neo4j.DEFAULT_DATABASE,
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
    dump_format = DumpFormat.CYPHER_SHELL

    # When
    output = StringIO()
    async for line in dump_graph(
        dump_format=dump_format, neo4j_driver=driver, project=neo4j.DEFAULT_DATABASE
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
    dump_format = DumpFormat.CYPHER_SHELL
    query = """MATCH (person:Person { name:'Keanu Reeves' })
RETURN person;
"""

    # When
    output = StringIO()
    async for line in dump_graph(
        dump_format=dump_format,
        neo4j_driver=driver,
        project=neo4j.DEFAULT_DATABASE,
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
            project=neo4j.DEFAULT_DATABASE,
        ):
            pass


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "n_docs,n_ents,expected_count",
    [
        (0, dict(), GraphNodesCount()),
        (1, dict(), GraphNodesCount(documents=1)),
        (
            1,
            {"CAT_0": 1, "CAT_1": 2},
            GraphNodesCount(documents=1, named_entities={"CAT_0": 1, "CAT_1": 1 + 2}),
        ),
    ],
)
async def test_count_graph_nodes(
    neo4j_test_driver: neo4j.AsyncDriver,
    n_docs: int,
    n_ents: Dict[str, int],
    expected_count: GraphNodesCount,
):
    # Given
    driver = neo4j_test_driver
    if n_docs:
        await _create_docs(driver, n_docs)
    if n_ents:
        await _create_ents(driver, n_ents)

    # When
    count = await count_graph_nodes(driver, project=TEST_PROJECT)

    # Then
    assert count == expected_count
