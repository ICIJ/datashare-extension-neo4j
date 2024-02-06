import xml.etree.ElementTree as e_t
from io import StringIO
from typing import Dict, Optional

import neo4j
import pytest
import pytest_asyncio

from neo4j_app.core.neo4j.graphs import (
    _make_default_query,  # pylint: disable=protected-access
    dump_graph,
    project_statistics,
    refresh_project_statistics,
)
from neo4j_app.core.objects import DumpFormat, GraphCounts, ProjectStatistics
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
            props = {"props": props, "labels": labels, "mentionCount": len(mention_ids)}
            ents.append(props)
    query = """UNWIND $ents as ent
CALL apoc.create.node(ent.labels, ent.props) YIELD node as ne
MATCH (doc:Document {id: 'doc-0'})
MERGE (ne)-[rel:APPEARS_IN {mentionCount: ent.mentionCount}]->(doc)
"""
    await driver.execute_query(query, ents=ents)


@pytest_asyncio.fixture(scope="module")
async def _populate_neo4j(
    neo4j_test_driver_module: neo4j.AsyncDriver,
) -> neo4j.AsyncDriver:
    query = """
CREATE (doc:Document {id:'doc-id'})
CREATE (ne:NamedEntity:Person {mentionNorm:'Keanu Reeves'})
CREATE (doc)<-[:APPEARS_IN]-(ne);
"""
    async with neo4j_test_driver_module.session() as sess:
        await sess.run(query)
    yield neo4j_test_driver_module


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
        export_batch_size=10,
    ):
        output.write(line)

    # Then
    expected_keys = """<?xml version="1.0" encoding="UTF-8"?>
<graphml xmlns="http://graphml.graphdrawing.org/xmlns"\
 xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"\
  xsi:schemaLocation="http://graphml.graphdrawing.org/xmlns\
   http://graphml.graphdrawing.org/xmlns/1.0/graphml.xsd">
<key id="mentionNorm" for="node" attr.name="mentionNorm"/>
<key id="id" for="node" attr.name="id"/>
<key id="TYPE" for="node" attr.name="TYPE"/>
<key id="labels" for="node" attr.name="labels"/>
<key id="label" for="edge" attr.name="label"/>
<key id="TYPE" for="edge" attr.name="TYPE"/>
</graphml>
"""
    ns = {"": "http://graphml.graphdrawing.org/xmlns"}

    xml_string = output.getvalue()
    root = e_t.fromstring(xml_string)
    keys = root.findall("key", namespaces=ns)
    expected_root = e_t.fromstring(expected_keys)
    expected_keys = expected_root.findall("key", namespaces=ns)
    assert len(keys) == len(expected_keys)
    assert all(xml_elements_equal(a, e) for a, e in zip(keys, expected_keys))
    nodes = root.findall(".graph/node", namespaces=ns)
    assert len(nodes) == 2
    edges = root.findall(".graph/edge", namespaces=ns)
    assert len(edges) == 1


async def test_should_dump_subgraph_to_graphml(_populate_neo4j: neo4j.AsyncDriver):
    # pylint: disable=invalid-name
    # Given
    driver = _populate_neo4j
    dump_format = DumpFormat.GRAPHML
    query = """MATCH (person:NamedEntity { mentionNorm:'Keanu Reeves' })
RETURN person;
"""

    # When
    output = StringIO()
    async for line in dump_graph(
        dump_format=dump_format,
        neo4j_driver=driver,
        project=neo4j.DEFAULT_DATABASE,
        query=query,
        export_batch_size=10,
    ):
        output.write(line)

    # Then
    expected_keys = """<?xml version="1.0" encoding="UTF-8"?>
<graphml xmlns="http://graphml.graphdrawing.org/xmlns" \
xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" \
xsi:schemaLocation="http://graphml.graphdrawing.org/xmlns \
http://graphml.graphdrawing.org/xmlns/1.0/graphml.xsd">
<key id="mentionNorm" for="node" attr.name="mentionNorm"/>
<key id="TYPE" for="node" attr.name="TYPE"/>
<key id="labels" for="node" attr.name="labels"/>
<key id="TYPE" for="edge" attr.name="TYPE"/>
</graphml>"""
    ns = {"": "http://graphml.graphdrawing.org/xmlns"}

    xml_string = output.getvalue()
    root = e_t.fromstring(xml_string)
    keys = root.findall("key", namespaces=ns)
    expected_root = e_t.fromstring(expected_keys)
    expected_keys = expected_root.findall("key", namespaces=ns)
    assert len(keys) == len(expected_keys)
    assert all(xml_elements_equal(a, e) for a, e in zip(keys, expected_keys))
    nodes = root.findall(".graph/node", namespaces=ns)
    assert len(nodes) == 1
    names = root.findall(".graph//data[@key='mentionNorm']", namespaces=ns)
    assert len(names) == 1
    assert names[0].text == "Keanu Reeves"


async def test_should_dump_full_graph_to_cypher(_populate_neo4j: neo4j.AsyncDriver):
    # pylint: disable=invalid-name
    # Given
    driver = _populate_neo4j
    dump_format = DumpFormat.CYPHER_SHELL

    # When
    output = StringIO()
    async for line in dump_graph(
        dump_format=dump_format,
        neo4j_driver=driver,
        project=neo4j.DEFAULT_DATABASE,
        export_batch_size=10,
    ):
        output.write(line)

    # Then
    cypher_statements = output.getvalue()
    assert "SET n:Document" in cypher_statements
    assert 'properties:{id:"doc-id"}}' in cypher_statements
    assert "SET n:NamedEntity:Person" in cypher_statements
    assert 'properties:{mentionNorm:"Keanu Reeves"}}' in cypher_statements


async def test_should_dump_subgraph_to_cypher(_populate_neo4j: neo4j.AsyncDriver):
    # pylint: disable=invalid-name
    # Given
    driver = _populate_neo4j
    dump_format = DumpFormat.CYPHER_SHELL
    query = """MATCH (person:NamedEntity { mentionNorm:'Keanu Reeves' })
RETURN person;
"""

    # When
    output = StringIO()
    async for line in dump_graph(
        dump_format=dump_format,
        neo4j_driver=driver,
        project=neo4j.DEFAULT_DATABASE,
        query=query,
        export_batch_size=10,
    ):
        output.write(line)

    # Then
    cypher_statements = output.getvalue()
    assert "SET n:Document" not in cypher_statements
    assert "SET n:NamedEntity:Person" in cypher_statements
    assert 'properties:{mentionNorm:"Keanu Reeves"}}' in cypher_statements


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
            parallel=False,
            export_batch_size=10,
        ):
            pass


@pytest.mark.parametrize(
    "n_docs,n_ents,expected_stats",
    [
        (0, dict(), ProjectStatistics()),
        (1, dict(), ProjectStatistics(counts=GraphCounts(documents=1))),
        (
            1,
            {"CAT_0": 1, "CAT_1": 2},
            ProjectStatistics(
                counts=GraphCounts(
                    documents=1, named_entities={"CAT_0": 1, "CAT_1": 1 + 2}
                )
            ),
        ),
    ],
)
async def test_project_statistics(
    neo4j_app_driver: neo4j.AsyncDriver,
    n_docs: int,
    n_ents: Dict[str, int],
    expected_stats: GraphCounts,
):
    # Given
    driver = neo4j_app_driver
    if n_docs:
        await _create_docs(driver, n_docs)
    if n_ents:
        await _create_ents(driver, n_ents)
    if n_docs + bool(n_ents):
        await refresh_project_statistics(driver, project=TEST_PROJECT)

    # When
    stats = await project_statistics(driver, project=TEST_PROJECT)

    # Then
    assert stats == expected_stats


@pytest.mark.parametrize(
    "default_docs_limit,expected_query",
    [
        (
            None,
            """MATCH (doc:Document)
WITH doc
ORDER BY doc.path ASC
OPTIONAL MATCH (doc)-[rel:APPEARS_IN|SENT|RECEIVED]-(ne:NamedEntity)
RETURN apoc.coll.toSet(collect(doc) + collect(ne) + collect(rel)) AS values""",
        ),
        (
            666,
            """MATCH (doc:Document)
WITH doc
ORDER BY doc.path ASC
LIMIT 666
OPTIONAL MATCH (doc)-[rel:APPEARS_IN|SENT|RECEIVED]-(ne:NamedEntity)
RETURN apoc.coll.toSet(collect(doc) + collect(ne) + collect(rel)) AS values""",
        ),
    ],
)
def test_make_default_query(default_docs_limit: Optional[int], expected_query: str):
    # When
    query = _make_default_query(default_docs_limit)

    # Then
    assert query == expected_query


async def test_project_statistics_should_raise_for_duplicate_stats(
    neo4j_test_driver: neo4j.AsyncDriver,
):
    # Given
    driver = neo4j_test_driver
    # When/Then
    query = "CREATE (:_ProjectStatistics { id: 'some-id' })"
    await driver.execute_query(query)
    query = "CREATE (:_ProjectStatistics { id: 'some-other-id' })"
    await driver.execute_query(query)
    expected = "Inconsistent state, found several project statistics"
    with pytest.raises(ValueError, match=expected):
        await project_statistics(driver, TEST_PROJECT)
