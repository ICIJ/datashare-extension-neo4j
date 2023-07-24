package org.icij.datashare;

import static java.lang.Math.min;
import static org.icij.datashare.Neo4jUtils.DOC_NODE;
import static org.icij.datashare.Neo4jUtils.DOC_PATH;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;
import java.util.HashMap;
import java.util.List;
import org.neo4j.cypherdsl.core.Cypher;
import org.neo4j.cypherdsl.core.Node;
import org.neo4j.cypherdsl.core.Relationship;
import org.neo4j.cypherdsl.core.SortItem;
import org.neo4j.cypherdsl.core.Statement;

//CHECKSTYLE.OFF: MemberName
//CHECKSTYLE.OFF: ParameterName
public class Objects {

    protected static class DocumentSortItem {
        protected final String property;
        protected final SortDirection direction;

        @JsonCreator
        DocumentSortItem(@JsonProperty("property") String property,
                         @JsonProperty("query") SortDirection direction) {
            this.property = property;
            this.direction = direction;
        }
    }

    protected enum DumpFormat {
        CYPHER_SHELL, GRAPHML;

        @JsonValue
        public String getFormatId() {
            switch (this) {
                case CYPHER_SHELL:
                    return "cypher-shell";
                case GRAPHML:
                    return "graphml";
                default:
                    throw new IllegalArgumentException("unhandled format ID" + this);
            }
        }
    }

    protected static class DumpQuery extends Neo4jUtils.Query {
        @JsonCreator
        protected DumpQuery(
            @JsonProperty("matches") List<Neo4jUtils.Match> matches,
            @JsonProperty("where") Neo4jUtils.Where where,
            @JsonProperty("orderBy") List<Neo4jUtils.OrderBy> orderBy,
            @JsonProperty("limit") Long limit
        ) {
            super(matches, where, orderBy, limit);
        }

        protected static Statement defaultQueryStatement(long defaultLimit) {
            Node doc = Cypher.node(DOC_NODE).named("doc");
            Node other = Cypher.anyNode().named("other");
            Relationship rel = doc.relationshipBetween(other).named("rel");
            return Cypher.match(doc)
                .optionalMatch(rel)
                .returning(doc, other, rel)
                .orderBy(doc.property(DOC_PATH).ascending())
                .limit(defaultLimit)
                .build();
        }
    }


    protected static class DumpRequest {
        protected final DumpFormat format;
        protected final DumpQuery query;

        @JsonCreator
        DumpRequest(@JsonProperty("format") DumpFormat format,
                    @JsonProperty("query") DumpQuery query) {
            this.format = format;
            this.query = query;
        }

    }


    static class IncrementalImportResponse {
        protected final long imported;
        protected final long nodesCreated;
        protected final long relationshipsCreated;


        @JsonCreator
        IncrementalImportResponse(@JsonProperty("imported") long imported,
                                  @JsonProperty("nodesCreated") long nodesCreated,
                                  @JsonProperty("relationshipsCreated")
                                  long relationshipsCreated) {
            this.imported = imported;
            this.nodesCreated = nodesCreated;
            this.relationshipsCreated = relationshipsCreated;
        }
    }

    static class IncrementalImportRequest {
        protected HashMap<String, Object> query;

        @JsonCreator
        IncrementalImportRequest(@JsonProperty("query") HashMap<String, Object> query) {
            this.query = query;
        }
    }

    //CHECKSTYLE.OFF: AbbreviationAsWordInName
    static class Neo4jAppNeo4jCSVRequest {

        protected HashMap<String, Object> query;

        @JsonCreator
        Neo4jAppNeo4jCSVRequest(@JsonProperty("query") HashMap<String, Object> query) {
            this.query = query;
        }
    }

    protected static class Neo4jAppDumpRequest {
        protected final DumpFormat format;
        protected final String query;

        @JsonCreator
        Neo4jAppDumpRequest(@JsonProperty("format") DumpFormat format,
                            @JsonProperty("query") String query) {
            this.format = format;
            this.query = query;
        }
    }

    static class Neo4jCSVResponse {
        protected String path;
        protected Neo4jCSVMetadata metadata;

        @JsonCreator
        Neo4jCSVResponse(@JsonProperty("path") String path,
                         @JsonProperty("metadata") Neo4jCSVMetadata metadata) {
            this.path = path;
            this.metadata = metadata;
        }
    }

    static class Neo4jCSVMetadata {
        protected List<NodeCSVs> nodes;
        protected List<RelationshipCSVs> relationships;

        @JsonCreator
        Neo4jCSVMetadata(@JsonProperty("nodes") List<NodeCSVs> nodes,
                         @JsonProperty("relationships") List<RelationshipCSVs> relationships) {
            this.nodes = nodes;
            this.relationships = relationships;
        }
    }

    static class NodeCSVs {
        protected List<String> labels;
        protected String headerPath;
        protected List<String> nodePaths;
        protected long nNodes;

        @JsonCreator
        NodeCSVs(@JsonProperty("labels") List<String> labels,
                 @JsonProperty("headerPath") String headerPath,
                 @JsonProperty("nodePaths") List<String> nodePaths,
                 @JsonProperty("nNodes") long nNodes) {
            this.labels = labels;
            this.headerPath = headerPath;
            this.nodePaths = nodePaths;
            this.nNodes = nNodes;
        }
    }

    static class RelationshipCSVs {
        protected List<String> types;
        protected String headerPath;
        protected List<String> relationshipPaths;
        protected long nRelationships;

        @JsonCreator
        RelationshipCSVs(@JsonProperty("types") List<String> types,
                         @JsonProperty("headerPath") String headerPath,
                         @JsonProperty("relationshipPaths") List<String> relationshipPaths,
                         @JsonProperty("nNodes") long nRelationships) {
            this.types = types;
            this.headerPath = headerPath;
            this.relationshipPaths = relationshipPaths;
            this.nRelationships = nRelationships;
        }
    }
    //CHECKSTYLE.ON: AbbreviationAsWordInName


    protected enum  SortDirection {
        ASC, DESC;

        public SortItem.Direction toDsl() {
            switch (this) {
                case ASC:
                    return SortItem.Direction.ASC;
                case DESC:
                    return SortItem.Direction.DESC;
                default:
                    throw new IllegalArgumentException("unhandled direction" + this);
            }
        }
    }

    protected static class SortedDumpQuery {
        protected final List<DocumentSortItem> sort;
        protected final Long limit;

        @JsonCreator
        protected SortedDumpQuery(
            @JsonProperty("sort") List<DocumentSortItem> sort,
            @JsonProperty("limit") Long limit
        ) {
            this.sort = java.util.Objects.requireNonNull(sort, "missing sort");
            this.limit = limit;
        }

        protected Statement defaultQueryStatement(long defaultLimit) {
            Node doc = Cypher.node(DOC_NODE).named("doc");
            Node other = Cypher.anyNode().named("other");
            Relationship rel = doc.relationshipBetween(other).named("rel");
            SortItem[] orderBy = this.sort.stream().map(item -> {
                if (item.direction == Objects.SortDirection.ASC) {
                    return doc.property(item.property).ascending();
                } else {
                    return doc.property(item.property).descending();
                }
            }).toArray(SortItem[]::new);
            long limit = defaultLimit;
            if (this.limit != null) {
                limit = min(this.limit, defaultLimit);
            }
            return Cypher.match(rel)
                .returning(doc, other, rel)
                .orderBy(orderBy)
                .limit(limit)
                .build();
        }
    }


    protected static class SortedDumpRequest {
        protected final DumpFormat format;
        protected final SortedDumpQuery query;
        protected final long limit;

        @JsonCreator
        protected SortedDumpRequest(
            @JsonProperty("format") DumpFormat format,
            @JsonProperty("query") SortedDumpQuery query
        ) {
            this.format = java.util.Objects.requireNonNull(format, "missing dump format");
            this.query = java.util.Objects.requireNonNull(query, "missing query");
        }
    }


    static class StartNeo4jAppRequest {
        protected boolean forceMigration;

        @JsonCreator
        StartNeo4jAppRequest(@JsonProperty("forceMigration") boolean forceMigration) {
            this.forceMigration = forceMigration;
        }
    }
}
//CHECKSTYLE.ON: ParameterName
//CHECKSTYLE.ON: MemberName