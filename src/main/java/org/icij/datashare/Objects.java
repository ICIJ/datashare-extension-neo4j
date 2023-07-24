package org.icij.datashare;

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

    public enum DumpFormat {
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



    public static class DumpRequest {
        public final DumpFormat format;
        public final DumpQuery query;

        @JsonCreator
        DumpRequest(@JsonProperty("format") DumpFormat format,
                    @JsonProperty("query") DumpQuery query) {
            this.format = format;
            this.query = query;
        }

    }


    static class IncrementalImportResponse {
        public final long imported;
        public final long nodesCreated;
        public final long relationshipsCreated;


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
        public HashMap<String, Object> query;

        @JsonCreator
        IncrementalImportRequest(@JsonProperty("query") HashMap<String, Object> query) {
            this.query = query;
        }
    }

    //CHECKSTYLE.OFF: AbbreviationAsWordInName
    static class Neo4jAppNeo4jCSVRequest {

        public HashMap<String, Object> query;

        @JsonCreator
        Neo4jAppNeo4jCSVRequest(@JsonProperty("query") HashMap<String, Object> query) {
            this.query = query;
        }
    }

    public static class Neo4jAppDumpRequest {
        public final DumpFormat format;
        public final String query;

        @JsonCreator
        Neo4jAppDumpRequest(@JsonProperty("format") DumpFormat format,
                            @JsonProperty("query") String query) {
            this.format = format;
            this.query = query;
        }
    }

    static class Neo4jCSVResponse {
        public String path;
        public Neo4jCSVMetadata metadata;

        @JsonCreator
        Neo4jCSVResponse(@JsonProperty("path") String path,
                         @JsonProperty("metadata") Neo4jCSVMetadata metadata) {
            this.path = path;
            this.metadata = metadata;
        }
    }

    static class Neo4jCSVMetadata {
        public List<NodeCSVs> nodes;
        public List<RelationshipCSVs> relationships;

        @JsonCreator
        Neo4jCSVMetadata(@JsonProperty("nodes") List<NodeCSVs> nodes,
                         @JsonProperty("relationships") List<RelationshipCSVs> relationships) {
            this.nodes = nodes;
            this.relationships = relationships;
        }
    }

    static class NodeCSVs {
        public List<String> labels;
        public String headerPath;
        public List<String> nodePaths;
        public long nNodes;

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
        public List<String> types;
        public String headerPath;
        public List<String> relationshipPaths;
        public long nRelationships;

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


    public enum SortDirection {
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

    static class StartNeo4jAppRequest {
        public boolean forceMigration;

        @JsonCreator
        StartNeo4jAppRequest(@JsonProperty("forceMigration") boolean forceMigration) {
            this.forceMigration = forceMigration;
        }
    }
}
//CHECKSTYLE.ON: ParameterName
//CHECKSTYLE.ON: MemberName