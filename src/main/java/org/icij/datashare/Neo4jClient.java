package org.icij.datashare;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import kong.unirest.BasicResponse;
import kong.unirest.HttpRequest;
import kong.unirest.HttpResponse;
import kong.unirest.Unirest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

import static org.icij.datashare.LoggingUtils.lazy;
import static org.icij.datashare.json.JsonObjectMapper.MAPPER;

public class Neo4jClient {
    protected final int port;
    private final String host = "127.0.0.1";
    private static final Logger logger = LoggerFactory.getLogger(Neo4jClient.class);

    public Neo4jClient(int port) {
        this.port = port;
    }

    static class Neo4jAppError extends HttpUtils.HttpError {
        Neo4jAppError(String title, String detail) {
            super(title, detail);
        }

        Neo4jAppError(HttpUtils.HttpError error) {
            super(error.title, error.detail);
        }
    }


    protected static class DocumentImportResponse {

        public final long nDocsToInsert;
        public final long nInsertedDocs;

        @JsonCreator
        DocumentImportResponse(@JsonProperty("nDocsToInsert") long nDocsToInsert, @JsonProperty("nInsertedDocs") long nInsertedDocs) {
            this.nDocsToInsert = nDocsToInsert;
            this.nInsertedDocs = nInsertedDocs;
        }

    }

    protected static class DocumentImportRequest {
        public final String query;

        @JsonCreator
        DocumentImportRequest(@JsonProperty("query") String query) {
            this.query = Optional.ofNullable(query).orElse("{}");
        }
    }

    public DocumentImportResponse importDocuments(String jsonQuery) {
        DocumentImportRequest body = new DocumentImportRequest(jsonQuery);
        String url = buildNeo4jUrl("/documents");
        logger.debug("Importing neo4j documents with request: {}", lazy(() -> MAPPER.writeValueAsString(body)));
        return doHttpRequest(Unirest.post(url).body(body), DocumentImportResponse.class);
    }

    public String ping() {
        String url = buildNeo4jUrl("/ping");
        logger.debug("Pinging neo4j app");
        return doHttpRequest(Unirest.get(url), String.class);
    }


    private <T, R extends HttpRequest> T doHttpRequest(HttpRequest<R> request, Class<T> clazz) throws Neo4jAppError {
        // TODO: ideally we would like to avoid to pass the class and request.asObject(new GenericType<T>() {})
        //  by it does not seem to work
        HttpResponse<T> res = request
                .asObject(clazz)
                .ifFailure(HttpUtils.HttpError.class, r -> {
                    if (r instanceof BasicResponse) {
                        String detail = r.getStatusText();
                        String title = r.getStatusText();
                        throw new Neo4jAppError(title, detail);
                    } else {
                        HttpUtils.HttpError error = r.getBody();
                        throw new Neo4jAppError(error);
                    }
                });
        return res.getBody();
    }

    private String buildNeo4jUrl(String url) {
        return "http://" + this.host + ":" + this.port + url;
    }

}
