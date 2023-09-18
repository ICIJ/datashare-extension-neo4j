package org.icij.datashare;

import static org.icij.datashare.LoggingUtils.lazy;
import static org.icij.datashare.json.JsonObjectMapper.MAPPER;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.util.HashMap;
import kong.unirest.HttpRequest;
import kong.unirest.HttpResponse;
import kong.unirest.Unirest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Neo4jClient {
    private static final Logger logger = LoggerFactory.getLogger(Neo4jClient.class);
    private static final int httpTimeout = 1000 * 30 * 60;
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
    }

    public boolean initProject(String project) {
        String url = buildNeo4jUrl("/projects/init?project=" + project);
        logger.debug("Initializing project: {}", project);
        HttpResponse res = doHttpRequestAsEmpty(
            Unirest.post(url).header("Content-Type", "application/json")
        );
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
        return doHttpRequest(Unirest.get(url), HashMap.class);
    }

    public Objects.IncrementalImportResponse importDocuments(
        String project, Objects.IncrementalImportRequest body
    ) {
        String url = buildNeo4jUrl("/documents?project=" + project);
        logger.debug("Importing documents to neo4j with request: {}",
            lazy(() -> MAPPER.writeValueAsString(body)));
        return doHttpRequest(
            Unirest.post(url).socketTimeout(httpTimeout).body(body)
                .header("Content-Type", "application/json"),
            Objects.IncrementalImportResponse.class
        );
    }

    public Objects.IncrementalImportResponse importNamedEntities(
        String project, Objects.IncrementalImportRequest body
    ) {
        String url = buildNeo4jUrl("/named-entities?project=" + project);
        logger.debug("Importing named entities to neo4j with request: {}",
            lazy(() -> MAPPER.writeValueAsString(body)));
        return doHttpRequest(
            Unirest.post(url).socketTimeout(httpTimeout).body(body)
                .header("Content-Type", "application/json"),
            Objects.IncrementalImportResponse.class
        );
    }

    public InputStream dumpGraph(String project, Objects.Neo4jAppDumpRequest body)
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
    public Objects.Neo4jCSVResponse exportNeo4jCSVs(
        String projectId, Objects.Neo4jAppNeo4jCSVRequest body
    ) {
        String url = buildNeo4jUrl("/admin/neo4j-csvs?project=" + projectId);
        logger.debug("Exporting data to neo4j csv with request: {}",
            lazy(() -> MAPPER.writeValueAsString(body)));
        return doHttpRequest(
            Unirest.post(url).socketTimeout(httpTimeout).body(body)
                .header("Content-Type", "application/json"),
            Objects.Neo4jCSVResponse.class
        );
    }
    //CHECKSTYLE.ON: AbbreviationAsWordInName


    private <R extends HttpRequest<R>> HttpResponse doHttpRequestAsEmpty(HttpRequest<R> request)
        throws Neo4jAppError {
        return handleUnirestErrors(request.asEmpty());
    }

    private <T, R extends HttpRequest<R>> T doHttpRequest(HttpRequest<R> request, Class<T> clazz)
        throws Neo4jAppError {
        // TODO: ideally we would like to avoid to pass the class and
        //  request.asObject(new GenericType<T>() {}) by it does not seem to work
        HttpResponse<T> res = handleUnirestErrors(request.asObject(clazz));
        return res.getBody();
    }

    private String buildNeo4jUrl(String url) {
        return "http://" + this.host + ":" + this.port + url;
    }

    static <T> HttpResponse<T> handleUnirestErrors(HttpResponse<T> response) {
        response = response.ifFailure(HttpUtils.HttpError.class, r -> {
            HttpUtils.HttpError error = r.getBody();
            throw new Neo4jAppError(error);
        });
        return response;
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


}
