package org.icij.datashare;

import static org.icij.datashare.LoggingUtils.lazy;
import static org.icij.datashare.json.JsonObjectMapper.MAPPER;

import kong.unirest.HttpRequest;
import kong.unirest.HttpResponse;
import kong.unirest.Unirest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Neo4jClient {
    private static final Logger logger = LoggerFactory.getLogger(Neo4jClient.class);
    private static final int importTimeout = 1000 * 30 * 60;
    protected final int port;
    private final String host = "127.0.0.1";

    public Neo4jClient(int port) {
        this.port = port;
    }

    public Objects.IncrementalImportResponse importDocuments(
        String database, Objects.IncrementalImportRequest body
    ) {
        String url = buildNeo4jUrl("/documents?database=" + database);
        logger.debug("Importing documents to neo4j with request: {}",
            lazy(() -> MAPPER.writeValueAsString(body)));
        return doHttpRequest(
            Unirest.post(url).socketTimeout(importTimeout).body(body)
                .header("Content-Type", "application/json"),
            Objects.IncrementalImportResponse.class
        );
    }

    public Objects.IncrementalImportResponse importNamedEntities(
        String database, Objects.IncrementalImportRequest body
    ) {
        String url = buildNeo4jUrl("/named-entities?database=" + database);
        logger.debug("Importing named entities to neo4j with request: {}",
            lazy(() -> MAPPER.writeValueAsString(body)));
        return doHttpRequest(
            Unirest.post(url).socketTimeout(importTimeout).body(body)
                .header("Content-Type", "application/json"),
            Objects.IncrementalImportResponse.class
        );
    }

    //CHECKSTYLE.OFF: AbbreviationAsWordInName
    public Objects.Neo4jCSVResponse exportNeo4jCSVs(
        String database, Objects.Neo4jAppNeo4jCSVRequest body
    ) {
        String url = buildNeo4jUrl("/admin/neo4j-csvs?database=" + database);
        logger.debug("Exporting data to neo4j csv with request: {}",
            lazy(() -> MAPPER.writeValueAsString(body)));
        return doHttpRequest(
            Unirest.post(url).socketTimeout(importTimeout).body(body)
                .header("Content-Type", "application/json"),
            Objects.Neo4jCSVResponse.class
        );
    }
    //CHECKSTYLE.ON: AbbreviationAsWordInName


    private <T, R extends HttpRequest<R>> T doHttpRequest(HttpRequest<R> request, Class<T> clazz)
        throws Neo4jAppError {
        // TODO: ideally we would like to avoid to pass the class and
        //  request.asObject(new GenericType<T>() {}) by it does not seem to work
        HttpResponse<T> res = request
            .asObject(clazz)
            .ifFailure(HttpUtils.HttpError.class, r -> {
                HttpUtils.HttpError error = r.getBody();
                throw new Neo4jAppError(error);
            });
        return res.getBody();
    }

    private String buildNeo4jUrl(String url) {
        return "http://" + this.host + ":" + this.port + url;
    }

    static class Neo4jAppError extends HttpUtils.HttpError {
        Neo4jAppError(String title, String detail) {
            super(title, detail);
        }

        Neo4jAppError(HttpUtils.HttpError error) {
            super(error.title, error.detail, error.trace);
        }
    }

}
