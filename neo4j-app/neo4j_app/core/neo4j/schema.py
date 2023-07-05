import re
from collections import defaultdict
from typing import List

import neo4j

from neo4j_app.constants import MIGRATION_NODE
from neo4j_app.core.objects import (
    GraphSchema,
    Neo4jProperty,
    NodeSchema,
    RelationshipSchema,
)

_RELTYPE_REGEX = None
_RELTYPE_REGEX_PATTERN = r"^:`(\w+)`$"
_FILTERED_NODES = {MIGRATION_NODE}


async def graph_schema(neo4j_driver: neo4j.AsyncDriver, neo4j_db: str) -> GraphSchema:
    async with neo4j_driver.session(database=neo4j_db) as sess:
        node_records = await sess.execute_read(_node_properties_tx)
        rel_records = await sess.execute_read(_relationships_properties_tx)
    nodes = sorted(records_to_node_schemas(node_records), key=lambda node: node.label)
    relationships = sorted(
        records_to_relationships_schemas(rel_records), key=lambda rel: rel.type
    )
    schema = GraphSchema(nodes=nodes, relationships=relationships)
    return schema


def records_to_node_schemas(node_records: neo4j.Record) -> List[NodeSchema]:
    nodes = defaultdict(dict)
    for rec in node_records:
        for label in rec["nodeLabels"]:
            if label in _FILTERED_NODES:
                continue
            properties = nodes[label]
            prop_name = rec["propertyName"]
            if prop_name not in properties:
                properties[prop_name] = (set(rec["propertyTypes"]), rec["mandatory"])
            else:
                properties[prop_name][0].update(rec["propertyTypes"])
                properties[prop_name] = (
                    properties[prop_name][0],
                    properties[prop_name][1] and rec["mandatory"],
                )
    schemas = []
    for label, props in nodes.items():
        properties = [
            Neo4jProperty(name=prop_name, types=prop_types, mandatory=mandatory)
            for prop_name, (prop_types, mandatory) in props.items()
        ]
        schemas.append(NodeSchema(label=label, properties=properties))
    return schemas


def records_to_relationships_schemas(
    relationships_records: neo4j.Record,
) -> List[RelationshipSchema]:
    rels = defaultdict(list)
    for rec in relationships_records:
        rel_type = _parse_relationship_type(rec["relType"])
        properties = rels[rel_type]
        mandatory = rec["mandatory"]
        prop = Neo4jProperty(
            name=rel_type, types=rec["propertyTypes"], mandatory=mandatory
        )
        properties.append(prop)
    schemas = [
        RelationshipSchema(type=rel_type, properties=properties)
        for rel_type, properties in rels.items()
    ]
    return schemas


async def _node_properties_tx(tx: neo4j.AsyncTransaction) -> List[neo4j.Record]:
    # TODO: support multiple forbidden nodes
    query = """CALL db.schema.nodeTypeProperties()
YIELD nodeLabels, propertyName, propertyTypes, mandatory
RETURN nodeLabels, propertyName, propertyTypes, mandatory 
"""
    res = await tx.run(query)
    records = [record async for record in res]
    return records


async def _relationships_properties_tx(
    tx: neo4j.AsyncTransaction,
) -> List[neo4j.Record]:
    # TODO: support forbidden relationships
    query = """CALL db.schema.relTypeProperties()
YIELD relType, propertyName, propertyTypes, mandatory
return relType, propertyName, propertyTypes, mandatory
"""
    res = await tx.run(query)
    records = [record async for record in res]
    return records


def _get_reltype_regex():
    global _RELTYPE_REGEX
    if _RELTYPE_REGEX is None:
        _RELTYPE_REGEX = re.compile(_RELTYPE_REGEX_PATTERN)
    return _RELTYPE_REGEX


def _parse_relationship_type(relationship_type: str) -> str:
    regex = _get_reltype_regex()
    match = regex.match(relationship_type)
    if match is None:
        msg = f"Couldn't find string matching {regex.pattern} in {relationship_type}"
        raise ValueError(msg)
    return match.group(0)
