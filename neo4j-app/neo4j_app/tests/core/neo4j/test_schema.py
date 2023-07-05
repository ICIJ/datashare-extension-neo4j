import neo4j
import pytest

from neo4j_app.constants import MIGRATION_NODE
from neo4j_app.core.neo4j.schema import graph_schema
from neo4j_app.core.objects import GraphSchema, Neo4jProperty, NodeSchema


@pytest.mark.asyncio
async def test_graph_schema_should_aggregate_mandatory_attribute(
    neo4j_test_driver: neo4j.AsyncDriver,
):
    # Given
    driver = neo4j_test_driver
    query = """// propA is not mandatory for NodeA
CREATE (:NodeA {propA: ""})
CREATE (:NodeA {propA: null})
// propA is not mandatory for :NodeA:NodeB
CREATE (:NodeA:NodeB {propA: ""})
"""
    await driver.execute_query(query)

    # When
    schema = await graph_schema(driver, neo4j.DEFAULT_DATABASE)

    # Then
    nodes = [
        NodeSchema(
            label="NodeA",
            properties=[Neo4jProperty(name="propA", types=["String"], mandatory=False)],
        ),
        NodeSchema(
            label="NodeB",
            properties=[Neo4jProperty(name="propA", types=["String"], mandatory=True)],
        ),
    ]
    expected_schema = GraphSchema(nodes=nodes)
    assert schema == expected_schema


@pytest.mark.asyncio
async def test_graph_schema_should_aggregate_types_for_multitype_nodes(
    neo4j_test_driver: neo4j.AsyncDriver,
):
    # Given
    driver = neo4j_test_driver
    query = """CREATE (:NodeA {propA: ""})
CREATE (:NodeA {propA: 2})
"""
    await driver.execute_query(query)

    # When
    schema = await graph_schema(driver, neo4j.DEFAULT_DATABASE)

    # Then
    nodes = [
        NodeSchema(
            label="NodeA",
            properties=[
                Neo4jProperty(name="propA", types=["String", "Long"], mandatory=True)
            ],
        )
    ]
    expected_schema = GraphSchema(nodes=nodes)
    assert schema == expected_schema


@pytest.mark.asyncio
async def test_graph_schema_should_not_contain_utility_nodes(
    neo4j_test_driver: neo4j.AsyncDriver,
):
    # Given
    driver = neo4j_test_driver
    query = f"""CREATE (:NodeA {{propA: ""}})
CREATE (:{MIGRATION_NODE} {{propA: 2}})
"""
    await driver.execute_query(query)

    # When
    schema = await graph_schema(driver, neo4j.DEFAULT_DATABASE)

    # Then
    nodes = [
        NodeSchema(
            label="NodeA",
            properties=[Neo4jProperty(name="propA", types=["String"], mandatory=True)],
        )
    ]
    expected_schema = GraphSchema(nodes=nodes)
    assert schema == expected_schema
