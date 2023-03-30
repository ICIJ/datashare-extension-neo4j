DOCUMENT_TAG = "Documents"
NE_TAG = "Named entities"
OTHER_TAG = "Other"


DOC_IMPORT_SUM = "Import documents from elasticsearch to neo4j"
DOC_IMPORT_DESC = """Documents are searched for in `elasticsearch` potentially using \
the provided query they are then upserted into the `neo4j` database. 

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


NE_IMPORT_SUM = "Import named entities from elasticsearch to neo4j"
NE_IMPORT_DESC = """Named entities are searched for in `elasticsearch` potentially \
using the provided query they are then upserted into the `neo4j` database.

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
