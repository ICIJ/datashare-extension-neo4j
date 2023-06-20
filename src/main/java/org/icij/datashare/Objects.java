package org.icij.datashare;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.HashMap;
import java.util.List;

//CHECKSTYLE.OFF: MemberName
//CHECKSTYLE.OFF: ParameterName
public class Objects {
    static class IncrementalImportResponse {
        public final long imported;
        public final long nodesCreated;
        public final long relationshipsCreated;


        @JsonCreator
        IncrementalImportResponse(@JsonProperty("imported") long imported,
                                  @JsonProperty("nodesCreated") long nodesCreated,
                                  @JsonProperty("relationshipsCreated") long relationshipsCreated) {
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