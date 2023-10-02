package org.icij.datashare;

import static java.lang.Math.min;
import static org.icij.datashare.Neo4jUtils.DOC_NODE;
import static org.icij.datashare.Neo4jUtils.DOC_PATH;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;
import java.io.Serializable;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.icij.datashare.text.NamedEntity;
import org.neo4j.cypherdsl.core.Cypher;
import org.neo4j.cypherdsl.core.Node;
import org.neo4j.cypherdsl.core.Relationship;
import org.neo4j.cypherdsl.core.SortItem;
import org.neo4j.cypherdsl.core.Statement;
import org.neo4j.cypherdsl.core.SymbolicName;

//CHECKSTYLE.OFF: MemberName
//CHECKSTYLE.OFF: ParameterName
public class Objects {

    protected static class DocumentSortItem {
        protected final String property;
        protected final SortDirection direction;

        @JsonCreator
        DocumentSortItem(@JsonProperty("property") String property,
                         @JsonProperty("direction") SortDirection direction) {
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
        private DumpQuery(
            List<Neo4jUtils.Match> matches,
            Neo4jUtils.Where where,
            List<Neo4jUtils.OrderBy> orderBy,
            Long limit
        ) {
            super(matches, where, orderBy, limit);
        }

        @JsonCreator
        protected static DumpQuery createDumpQuery(
            @JsonProperty("matches") List<Neo4jUtils.Match> matches,
            @JsonProperty("where") Neo4jUtils.Where where,
            @JsonProperty("orderBy") List<Neo4jUtils.OrderBy> orderBy,
            @JsonProperty("limit") Long limit
        ) {
            if (matches == null || matches.isEmpty()) {
                matches = defaultMatchClause();
            }
            return new DumpQuery(matches, where, orderBy, limit);
        }

        protected static Statement defaultQueryStatement(long defaultLimit) {
            SymbolicName doc = Cypher.name("doc");
            return buildMatch(defaultMatchClause())
                .returning(doc, Cypher.name("other"), Cypher.name("rel"))
                .orderBy(doc.property(DOC_PATH).ascending())
                .limit(defaultLimit)
                .build();
        }

        private static List<Neo4jUtils.Match> defaultMatchClause() {
            Neo4jUtils.PatternNode doc = new Neo4jUtils.PatternNode(
                "doc", List.of(DOC_NODE), null);
            Neo4jUtils.PatternNode other =
                new Neo4jUtils.PatternNode(
                    "other", null, null);
            Neo4jUtils.PathPattern match = new Neo4jUtils.PathPattern(
                List.of(doc), null, false
            );
            Neo4jUtils.PatternRelationship rel = new Neo4jUtils.PatternRelationship(
                "rel", Neo4jUtils.PatternRelationship.Direction.BETWEEN, null);
            Neo4jUtils.PathPattern optionalMatch = new Neo4jUtils.PathPattern(
                List.of(doc, other), List.of(rel), true
            );
            return List.of(match, optionalMatch);
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

    protected static class GraphNodesCount {
        protected final long documents;
        protected final Map<NamedEntity.Category, Long> namedEntities;

        @JsonCreator
        GraphNodesCount(
            @JsonProperty("documents") Long documents,
            @JsonProperty("namedEntities") Map<NamedEntity.Category, Long> namedEntities
        ) {
            this.documents = Optional.ofNullable(documents).orElse(0L);
            this.namedEntities = Optional.ofNullable(namedEntities).orElse(Map.of());
        }
    }


    protected static class IncrementalImportResponse {
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

    protected static class IncrementalImportRequest {
        protected HashMap<String, Object> query;

        @JsonCreator
        IncrementalImportRequest(@JsonProperty("query") HashMap<String, Object> query) {
            this.query = query;
        }
    }

    //CHECKSTYLE.OFF: AbbreviationAsWordInName
    protected static class Neo4jAppNeo4jCSVRequest {

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

    protected static class Neo4jCSVResponse {
        protected String path;
        protected Neo4jCSVMetadata metadata;

        @JsonCreator
        Neo4jCSVResponse(@JsonProperty("path") String path,
                         @JsonProperty("metadata") Neo4jCSVMetadata metadata) {
            this.path = path;
            this.metadata = metadata;
        }
    }

    protected static class Neo4jCSVMetadata {
        protected List<NodeCSVs> nodes;
        protected List<RelationshipCSVs> relationships;

        @JsonCreator
        Neo4jCSVMetadata(@JsonProperty("nodes") List<NodeCSVs> nodes,
                         @JsonProperty("relationships") List<RelationshipCSVs> relationships) {
            this.nodes = nodes;
            this.relationships = relationships;
        }
    }

    protected static class NodeCSVs {
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

    protected static class RelationshipCSVs {
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


    protected enum SortDirection {
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


    protected enum TaskType {
        FULL_IMPORT;

        @JsonCreator
        private static TaskType fromStringValue(String value) {
            return TaskType.valueOf(value.toUpperCase());
        }

        @JsonValue
        private String getType() {
            return this.toString().toLowerCase();
        }
    }

    protected enum TaskStatus {
        CREATED, QUEUED, RUNNING, RETRY, ERROR, DONE, CANCELLED;
    }

    protected static class Task {
        protected final TaskType type;
        protected final String id;
        protected final TaskStatus status;
        protected final Map<String, Object> inputs;
        protected final Float progress;
        protected final Integer retries;
        protected final Date createdAt;
        protected final Date completedAt;

        @JsonCreator
        Task(@JsonProperty("id") String id,
             @JsonProperty("type") TaskType type,
             @JsonProperty("status") TaskStatus status,
             @JsonProperty("inputs") Map<String, Object> inputs,
             @JsonProperty("progress") Float progress,
             @JsonProperty("retries") Integer retries,
             @JsonProperty("createdAt") Date createdAt,
             @JsonProperty("completedAt") Date completedAt) {
            this.type = java.util.Objects.requireNonNull(type);
            this.id = id;
            this.status = status;
            this.inputs = Optional.ofNullable(inputs).orElse(Map.of());
            this.progress = progress;
            this.retries = retries;
            this.createdAt = createdAt;
            this.completedAt = completedAt;
        }
    }

    protected static class TaskJob<T extends Serializable> {
        protected final TaskType taskType;
        protected final String taskId;
        protected final T inputs;
        protected final Date createdAt;


        @JsonCreator
        TaskJob(@JsonProperty("taskType") TaskType taskType, @JsonProperty("taskId") String taskId,
                @JsonProperty("inputs") T inputs, @JsonProperty("createdAt") Date createdAt) {
            this.taskType = java.util.Objects.requireNonNull(taskType);
            this.taskId = taskId;
            this.inputs = inputs;
            this.createdAt = createdAt;
        }
    }

    protected static class TaskError {
        protected final String id;
        protected final String title;
        protected final String detail;
        protected final Date occurredAt;


        @JsonCreator
        TaskError(@JsonProperty("id") String id, @JsonProperty("title") String title,
                  @JsonProperty("detail") String detail,
                  @JsonProperty("occurredAt") Date occurredAt) {
            this.id = java.util.Objects.requireNonNull(id);
            this.title = java.util.Objects.requireNonNull(title);
            this.detail = detail;
            this.occurredAt = java.util.Objects.requireNonNull(occurredAt);
        }
    }

}
//CHECKSTYLE.ON: ParameterName
//CHECKSTYLE.ON: MemberName