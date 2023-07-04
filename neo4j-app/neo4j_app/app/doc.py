ADMIN_TAG = "Admin"
DOCUMENT_TAG = "Documents"
NE_TAG = "Named entities"
OTHER_TAG = "Other"
GRAPH_TAG = "Graphs"


DOC_IMPORT_SUM = "Import documents from elasticsearch to neo4j"
DOC_IMPORT_DESC = """Documents are searched for in `elasticsearch` using \
the optionally provided query, they are then upserted into the `neo4j` database. 

They query must be an content of the `query` field of an elasticsearch query. When \
provided it will combined into `bool` query in order to restrict the provided query to \
 apply to documents. 

If you provide:
```json
{
    "match": {
        "path": "somePath"
    }
}
```
then the query which will actually be performed will be:
```json
{
    "query": {
        "bool": {
            "must": [
                {
                    "term": {
                        "<docFieldType>": "Document"
                    }
                }
                {
                    "match": {
                        "path": "somePath"
                    }
                }           
            ]
        }
    }
}
```

The `<docFieldType>` defaults to `type` and is supposed to be forwarded from the Java \
app to the Python one through configuration. 
"""

DOC_NEO4J_CSV = (
    "Export data from elasticsearch into CSVs which can then be used to"
    " call the `neo4j-admin import` CLI"
)
DOC_NEO4J_CSV_DESC = """Documents are searched for in `elasticsearch` using the \
optionally provided query they are then dumped into a CSV. 

This CSV can then be imported to neo4k using the \
[neo4j-admin-import](https://neo4j.com/docs/operations-manual/current/tutorial/neo4j-admin-import/) \
CLI.

They query must be an content of the `query` field of an elasticsearch query. When \
provided it will combined into `bool` query in order to restrict the provided query to \
 apply to documents. 

If you provide:
```json
{
    "match": {
        "path": "somePath"
    }
}
```
then the query which will actually be performed will be:
```json
{
    "query": {
        "bool": {
            "must": [
                {
                    "term": {
                        "<docFieldType>": "Document"
                    }
                }
                {
                    "match": {
                        "path": "somePath"
                    }
                }           
            ]
        }
    }
}
```

The `<docFieldType>` defaults to `type` and is supposed to be forwarded from the Java \
app to the Python one through configuration.

When the export is complete a archive containing the CSVs and a import script is 
available at the returned path. 

After decompressing the archive the import can be done as described above:
```
./bulk-import.sh --dry-run
./bulk-import.sh
``` 
"""

DOC_GRAPH_DUMP = (
    "Dumps a full graph or subgraph to the desired format, the exported"
    " graph can then be re-imported and explored using the adapted graph visualization"
    " tool"
)
DOC_GRAPH_DUMP_DESC = """Dumps a full graph or subgraph (when a query is provided) to\
 one of the supported format:
- [GraphML](http://graphml.graphdrawing.org/), which enables to visualize the graph in:
    - [Gephi](https://gephi.org/)
- [Cypher shell](https://neo4j.com/docs/operations-manual/current/tools/cypher-shell/) \
, which enables to visualize the graph in:
    - [Neo4j Bloom](https://neo4j.com/product/bloom/)
    - [Linkurious](https://linkurious.com/)

To import the dumped graph:
- in Gephi, instructions can be found \
[here](https://gephi.org/users/supported-graph-formats/graphml-format/)
- into a `neo4j` database for visualization with Bloom or Linkurious, use the \
[`neo4j-shell` CLI](https://neo4j.com/docs/apoc/5/export/cypher/#export-cypher-cypher-shell) 
"""

NE_IMPORT_SUM = "Import named entities from elasticsearch to neo4j"
NE_IMPORT_DESC = """Named entities are searched for in `elasticsearch` \
using the optionally provided query, they are then upserted into the `neo4j` database.

**Only named entities which document has been priorly imported into neo4j are imported**

They query must be an content of the `query` field of an elasticsearch query. When \
provided it will combined into `bool` query in order to restrict the provided query to \
 apply to named entities. 

If you provide:
```json
{
    "ids": {
        "values": ["someNamedEntityId"]
    }
}
```
the service will first look into neo4j for existing ID `["docId1",...,"docIdN"]` \
and then the query which will actually be performed will be:
```json
{
    "query": {
        "bool": {
            "must": [
                {
                    "term": {
                        "type": "NamedEntity"
                    }
                },
                {
                    "has_parent": {
                        "parent_type": "Document",
                        "query": {
                            "ids": {
                                "values": ["docId1",...,"docIdN"]
                            }
                        }
                    }
                }
            ]
        }
    }
}
```

The `<docFieldType>` defaults to `type` and is supposed to be forwarded from the Java \
app to the Python one through configuration. 
"""
