package org.icij.datashare;

import static org.icij.datashare.HttpUtils.fromException;
import static org.icij.datashare.LoggingUtils.lazy;
import static org.icij.datashare.Objects.IncrementalImportRequest;
import static org.icij.datashare.Objects.IncrementalImportResponse;
import static org.icij.datashare.Objects.Neo4jAppDumpRequest;
import static org.icij.datashare.Objects.Neo4jAppNeo4jCSVRequest;
import static org.icij.datashare.Objects.Neo4jCSVResponse;
import static org.icij.datashare.Objects.Task;
import static org.icij.datashare.Objects.TaskError;
import static org.icij.datashare.Objects.TaskJob;
import static org.icij.datashare.Objects.TaskType;
import static org.icij.datashare.json.JsonObjectMapper.MAPPER;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.util.HashMap;
import kong.unirest.Config;
import kong.unirest.GenericType;
import kong.unirest.HttpRequestSummary;
import kong.unirest.HttpResponse;
import kong.unirest.Interceptor;
import kong.unirest.ObjectMapper;
import kong.unirest.Unirest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Neo4jClient {
    private static final Logger logger = LoggerFactory.getLogger(Neo4jClient.class);
    private static final int HTTP_TIMEOUT = 1000 * 30 * 60;
    protected final int port;
    private final String host = "127.0.0.1";

    private final java.net.http.HttpClient httpClient;

    protected static class AppNotReady extends Exception {
        protected AppNotReady(String message) {
            super(message);
        }

        protected AppNotReady(Throwable cause) {
            super(cause);
        }

    }

    public Neo4jClient(int port) {
        this.port = port;
        this.httpClient = java.net.http.HttpClient.newHttpClient();
        Unirest.config()
            .setObjectMapper(makeObjectMapper())
            .socketTimeout(HTTP_TIMEOUT)
            .interceptor(new Neo4jClient.ErrorInterceptor())
        ;
    }

    public boolean initProject(String project) {
        String url = buildNeo4jUrl("/projects/init?project=" + project);
        logger.debug("Initializing project: {}", project);
        HttpResponse<?> res = Unirest.post(url)
            .header("Content-Type", "application/json")
            .asEmpty();
        int status = res.getStatus();
        switch (status) {
            case 200:
                return false;
            case 201:
                return true;
            default:
                throw new IllegalStateException("Unexpected init status: " + status);
        }
    }

    public HashMap<String, Object> config() {
        String url = buildNeo4jUrl("/config");
        logger.debug("Fetching Python app config");
        return Unirest.get(url).asObject(new GenericType<HashMap<String, Object>>() {
        }).getBody();
    }

    protected String fullImport(String project) {
        String url = buildNeo4jUrl("/tasks?project=" + project);
        TaskJob<?> body = new TaskJob<>(TaskType.FULL_IMPORT, null, null, null);
        logger.debug("Starting full import for project: {}", project);
        return Unirest.post(url)
            .body(body)
            .header("Content-Type", "application/json")
            .asString()
            .getBody();
    }

    protected IncrementalImportResponse importDocuments(
        String project, IncrementalImportRequest body
    ) {
        String url = buildNeo4jUrl("/documents?project=" + project);
        logger.debug("Importing documents to neo4j with request: {}",
            lazy(() -> MAPPER.writeValueAsString(body)));
        return Unirest.post(url)
            .body(body)
            .header("Content-Type", "application/json")
            .asObject(IncrementalImportResponse.class).getBody();
    }

    protected IncrementalImportResponse importNamedEntities(
        String project, IncrementalImportRequest body
    ) {
        String url = buildNeo4jUrl("/named-entities?project=" + project);
        logger.debug("Importing named entities to neo4j with request: {}",
            lazy(() -> MAPPER.writeValueAsString(body)));
        return Unirest.post(url).body(body)
            .header("Content-Type", "application/json")
            .asObject(IncrementalImportResponse.class)
            .getBody();
    }

    protected InputStream dumpGraph(String project, Neo4jAppDumpRequest body)
        throws URISyntaxException, IOException, InterruptedException {
        // Let's use the native HTTP client here as unirest doesn't offer an easy way to deal with
        // stream responses...
        logger.debug("Dumping graph with request: {}", lazy(() -> MAPPER.writeValueAsString(body)));
        java.net.http.HttpRequest.BodyPublisher serializedBody =
            java.net.http.HttpRequest.BodyPublishers.ofString(MAPPER.writeValueAsString(body));
        java.net.http.HttpRequest request =
            java.net.http.HttpRequest.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .uri(new URI(buildNeo4jUrl("/graphs/dump?project=" + project)))
                .POST(serializedBody)
                .header("Content-Type", "application/json")
                .build();
        return handleErrors(
            this.httpClient.send(request, java.net.http.HttpResponse.BodyHandlers.ofInputStream()));
    }

    protected void ping() throws AppNotReady {
        logger.debug("Ping...");
        String url = buildNeo4jUrl("/ping");
        try {
            int status = Unirest.get(url).asObject(String.class).getStatus();
            if (status != 200) {
                throw new AppNotReady("app is not ready");
            }
        } catch (Exception e) {
            throw new AppNotReady(e);
        }
    }

    protected boolean pingSuccessful() {
        try {
            this.ping();
            return true;
        } catch (AppNotReady e) {
            logger.info("Ping failing: {}", e.getMessage());
        }
        return false;
    }

    //CHECKSTYLE.OFF: AbbreviationAsWordInName
    protected Neo4jCSVResponse exportNeo4jCSVs(
        String projectId, Neo4jAppNeo4jCSVRequest body
    ) {
        String url = buildNeo4jUrl("/admin/neo4j-csvs?project=" + projectId);
        logger.debug("Exporting data to neo4j csv with request: {}",
            lazy(() -> MAPPER.writeValueAsString(body)));
        return Unirest.post(url).body(body)
            .header("Content-Type", "application/json")
            .asObject(Neo4jCSVResponse.class)
            .getBody();
    }
    //CHECKSTYLE.ON: AbbreviationAsWordInName

    protected Task task(String taskId, String project) {
        String url = buildNeo4jUrl("/tasks/" + taskId + "?project=" + project);
        logger.debug("Getting task {}", taskId);
        return Unirest.get(url)
            .header("Content-Type", "application/json")
            .asObject(Task.class)
            .getBody();
    }

    protected <T> T taskResult(String taskId, String project, Class<T> clazz) {
        String url = buildNeo4jUrl("/tasks/" + taskId + "/result?project=" + project);
        logger.debug("Getting task {} result", taskId);
        return Unirest.get(url)
            .header("Content-Type", "application/json")
            .asObject(clazz)
            .getBody();
    }

    // TODO: generics don't play well Unirest, returning TaskError[] instead of
    //  List<TaskError> helps
    protected TaskError[] taskErrors(String taskId, String project) {
        String url = buildNeo4jUrl("/tasks/" + taskId + "/errors?project=" + project);
        logger.debug("Getting task {} errors", taskId);
        return Unirest.get(url)
            .header("Content-Type", "application/json")
            .asObject(TaskError[].class)
            .getBody();
    }

    private String buildNeo4jUrl(String url) {
        return "http://" + this.host + ":" + this.port + url;
    }


    static InputStream handleErrors(
        java.net.http.HttpResponse<InputStream> response) {
        int statusCode = response.statusCode();
        boolean success = statusCode >= 200 && statusCode < 300;
        if (!success) {
            HttpUtils.HttpError error;
            try (InputStream errorStream = response.body()) {
                error = MAPPER.readValue(errorStream.readAllBytes(), HttpUtils.HttpError.class);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            throw new Neo4jAppError(error);
        }
        return response.body();
    }

    static class Neo4jAppError extends RuntimeException {
        protected String title;
        protected String detail;
        protected String trace;

        @JsonCreator
        protected Neo4jAppError(
            @JsonProperty("title") String title,
            @JsonProperty("detail") String detail,
            @JsonProperty("trace") String trace
        ) {
            this.title = title;
            this.detail = detail;
            this.trace = trace;
        }

        protected Neo4jAppError(String title, String detail) {
            this(title, detail, null);
        }

        protected Neo4jAppError(HttpUtils.HttpError error) {
            this(error.title, error.detail, error.trace);
        }

        HttpUtils.HttpError toHttp() {
            return new HttpUtils.HttpError(this.title, this.detail, this.trace);
        }

        @Override
        public String getMessage() {
            String msg = this.title;
            msg += "\nDetail: " + this.detail;
            if (this.trace != null) {
                msg += "\nTrace: " + this.trace;
            }
            return msg;
        }
    }

    private static ObjectMapper makeObjectMapper() {
        return new ObjectMapper() {
            @Override
            public <T> T readValue(String s, Class<T> clazz) {
                try {
                    return MAPPER.readValue(s, clazz);
                } catch (JsonProcessingException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public <T> T readValue(String value, GenericType<T> genericType) {
                try {
                    // TODO: this doesn't work properly and returns List<HashMap> instead of
                    //  List<G>... When T =  List<G>
                    TypeReference<T> typeRef = new TypeReference<>() {
                    };
                    return MAPPER.readValue(value, typeRef);
                } catch (JsonProcessingException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public String writeValue(Object o) {
                try {
                    return MAPPER.writeValueAsString(o);
                } catch (JsonProcessingException e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }

    private static class ErrorInterceptor implements Interceptor {

        @Override
        public void onResponse(
            HttpResponse<?> response, HttpRequestSummary request, Config config
        ) {
            if (!response.isSuccess()) {
                response.getParsingError().ifPresent(e -> {
                    throw new Neo4jAppError(fromException(e));
                });
            }
            response.ifFailure(HttpUtils.HttpError.class, r -> {
                HttpUtils.HttpError error = r.getBody();
                throw new Neo4jAppError(error);
            });
        }
    }
}
