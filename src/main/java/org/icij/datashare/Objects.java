package org.icij.datashare;

import static java.util.UUID.randomUUID;
import static org.icij.datashare.Neo4jUtils.APPEARS_IN_REL;
import static org.icij.datashare.Neo4jUtils.DOC_NODE;
import static org.icij.datashare.Neo4jUtils.DOC_PATH;
import static org.icij.datashare.Neo4jUtils.NE_NODE;
import static org.icij.datashare.Neo4jUtils.RECEIVED_REL;
import static org.icij.datashare.Neo4jUtils.SENT_REL;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.icij.datashare.text.NamedEntity;
import org.neo4j.cypherdsl.core.Cypher;
import org.neo4j.cypherdsl.core.ExposesMatch;
import org.neo4j.cypherdsl.core.Expression;
import org.neo4j.cypherdsl.core.Functions;
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

    protected static class DumpQuery {
        List<Neo4jUtils.Query> queries;

        @JsonCreator
        protected DumpQuery(@JsonProperty("queries") List<Neo4jUtils.Query> queries) {
            if (queries == null || queries.isEmpty()) {
                queries = List.of(defaultMatchQuery());
            }
            this.queries = queries;
        }

        protected Statement asValidated(Long defaultLimit) {
            if (this.queries.size() != 1) {
                throw new IllegalArgumentException(
                    "expected a single query matching documents to be specified"
                );
            }

            List<Neo4jUtils.Query> validated = new ArrayList<>(this.queries);
            validated.add(optionalNamedEntityMatch());
            ListIterator<Neo4jUtils.Query> it = validated.listIterator();
            int numQueries = validated.size();

            Statement statement = null;
            ExposesMatch ongoing = null;
            while (it.hasNext()) {
                int i = it.nextIndex();
                Neo4jUtils.Query query = it.next();
                if (ongoing == null) {
                    ongoing = query.startStatement(defaultLimit);
                } else if (i == numQueries - 1) {
                    statement = query.finishQuery(ongoing, null, exportedValues());
                } else {
                    ongoing = query.continueStatement(ongoing, null);
                }
            }
            return statement;
        }

        protected static Neo4jUtils.Query defaultMatchQuery() {
            Neo4jUtils.PatternNode doc = new Neo4jUtils.PatternNode(
                "doc", List.of(DOC_NODE), null);
            Neo4jUtils.PathPattern firstMatch = new Neo4jUtils.PathPattern(
                List.of(doc), null, false
            );
            Neo4jUtils.SortByProperty orderBy = new Neo4jUtils.SortByProperty(
                new Neo4jUtils.VariableProperty("doc", DOC_PATH), SortDirection.ASC
            );
            return new Neo4jUtils.Query(
                List.of(firstMatch), null, List.of(orderBy), null);

        }

        protected static Neo4jUtils.Query optionalNamedEntityMatch() {
            Neo4jUtils.PatternNode doc = new Neo4jUtils.PatternNode(
                "doc", null, null);
            Neo4jUtils.PatternNode ne = new Neo4jUtils.PatternNode(
                "ne", List.of(NE_NODE), null);
            Neo4jUtils.PatternRelationship rel = new Neo4jUtils.PatternRelationship(
                "rel",
                Neo4jUtils.PatternRelationship.Direction.BETWEEN,
                List.of(APPEARS_IN_REL, SENT_REL, RECEIVED_REL)
            );
            Neo4jUtils.PathPattern secondMatch = new Neo4jUtils.PathPattern(
                List.of(doc, ne), List.of(rel), true
            );
            return new Neo4jUtils.Query(List.of(secondMatch), null, null, null);
        }

        private static List<Expression> exportedValues() {
            Expression values = Cypher.call("apoc.coll.toSet")
                .withArgs(
                    Functions.collect(Cypher.name("doc"))
                        .add(Functions.collect(Cypher.name("ne")))
                        .add(Functions.collect(Cypher.name("rel")))
                )
                .asFunction()
                .as("values");
            return List.of(values);
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

        String dumpExtension() {
            switch (format) {
                case CYPHER_SHELL:
                    return ".dump";
                case GRAPHML:
                    return ".graphml";
                default:
                    throw new IllegalStateException("Unexpected value: " + format);
            }
        }
    }

    protected static class GraphCount {
        protected final long documents;
        protected final Map<NamedEntity.Category, Long> namedEntities;

        @JsonCreator
        GraphCount(
            @JsonProperty("documents") Long documents,
            @JsonProperty("namedEntities") Map<NamedEntity.Category, Long> namedEntities
        ) {
            this.documents = Optional.ofNullable(documents).orElse(0L);
            this.namedEntities = Optional.ofNullable(namedEntities).orElse(Map.of());
        }
    }

    protected static class FullImportResponse {
        protected final IncrementalImportResponse documents;
        protected final IncrementalImportResponse namedEntities;

        @JsonCreator
        FullImportResponse(
            @JsonProperty("documents") IncrementalImportResponse documents,
            @JsonProperty("namedEntities") IncrementalImportResponse namedEntities
        ) {
            this.documents = documents;
            this.namedEntities = namedEntities;
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

        String generateTaskId() {
            return this.name().toLowerCase() + "-" + randomUUID();
        }

    }

    protected enum TaskStatus {
        CREATED, QUEUED, RUNNING, RETRY, ERROR, DONE, CANCELLED;

        static final Set<TaskStatus> READY_STATES = Set.of(DONE, ERROR, CANCELLED);
    }

    public static class Task {
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

    protected static class TaskSearch {
        protected final TaskType type;
        protected final List<TaskStatus> status;

        @JsonCreator
        TaskSearch(@JsonProperty("type") TaskType type,
                   @JsonProperty("status") List<TaskStatus> status) {
            this.type = type;
            this.status = status;
        }
    }

}
//CHECKSTYLE.ON: ParameterName
//CHECKSTYLE.ON: MemberName