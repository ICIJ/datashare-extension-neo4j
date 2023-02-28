package org.icij.datashare;

import static org.fest.assertions.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

import java.io.IOException;
import java.util.HashMap;
import java.util.Objects;
import net.codestory.http.Configuration;
import net.codestory.http.Context;
import net.codestory.http.payload.Payload;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;

public class Neo4jClientTest {

    private static Neo4jClient client;

    private static ProdWebServerRuleExtension neo4jApp;

    public static class Neo4jAppMock extends ProdWebServerRuleExtension
        implements BeforeAllCallback, AfterEachCallback {
        @Override
        public void beforeAll(ExtensionContext extensionContext) {
            client = new Neo4jClient(this.port());
            neo4jApp = this;
        }

        @Override
        public void afterEach(ExtensionContext extensionContext) {
            this.configure(Configuration.NO_ROUTE);
        }
    }

    @ExtendWith(Neo4jAppMock.class)
    @DisplayName("Neo4j client test")
    @Nested
    class ClientTest {

        private Payload mockImport(Context context) throws IOException {
            String body;
            if (Objects.equals(context.request().content(), "{}")) {
                body = "{\"nToInsert\": 3,\"nInserted\": 3}";
            } else {
                body = "{\"nToInsert\": 3,\"nInserted\": 1}";
            }
            return new Payload("application/json", body);
        }

        @Test
        public void test_get_ping() {
            // Given
            neo4jApp.configure(routes -> routes.get("/ping", (context) -> "pong"));
            // When
            String ping = client.ping();
            // Then
            assertThat(ping).isEqualTo("pong");
        }

        @Test
        public void test_should_import_documents() {
            // Given
            neo4jApp.configure(routes -> routes.post("/documents", this::mockImport));
            // When
            org.icij.datashare.Objects.IncrementalImportRequest body =
                new org.icij.datashare.Objects.IncrementalImportRequest(null);
            org.icij.datashare.Objects.IncrementalImportResponse res = client.importDocuments(body);
            // Then
            assertThat(res.nToInsert).isEqualTo(3);
            assertThat(res.nInserted).isEqualTo(3);
        }

        @Test
        public void test_should_import_documents_with_query() {
            // Given
            neo4jApp.configure(routes -> routes.post("/documents", this::mockImport));
            HashMap<String, Object> query = new HashMap<>() {
                {
                    put("key1", "value1");
                }
            };
            org.icij.datashare.Objects.IncrementalImportRequest body =
                new org.icij.datashare.Objects.IncrementalImportRequest(query);
            // When
            org.icij.datashare.Objects.IncrementalImportResponse res = client.importDocuments(body);
            // Then
            assertThat(res.nToInsert).isEqualTo(3);
            assertThat(res.nInserted).isEqualTo(1);
        }

        @Test
        public void test_import_documents_should_parse_app_error() {
            // Given
            neo4jApp.configure(routes -> routes.post("/documents",
                (context) -> {
                    String jsonError = TestUtils.makeJsonHttpError("someTitle", "someErrorDetail");
                    return new Payload("application/json", jsonError).withCode(500);
                })
            );
            // When/Then
            String expectedMsg =
                (new Neo4jClient.Neo4jAppError("someTitle", "someErrorDetail")).getMessage();
            assertThat(assertThrowsExactly(
                Neo4jClient.Neo4jAppError.class,
                () -> client.importDocuments(null)
            ).getMessage()).isEqualTo(expectedMsg);
        }

        @Test
        public void test_import_documents_should_parse_app_error_with_trace() {
            // Given
            neo4jApp.configure(routes -> routes.post("/documents",
                (context) -> {
                    String jsonError = TestUtils.makeJsonHttpError("someTitle", "someErrorDetail",
                        "sometrace here");
                    return new Payload("application/json", jsonError).withCode(500);
                })
            );
            // When/Then

            String expectedMsg = new Neo4jClient.Neo4jAppError(
                new HttpUtils.HttpError("someTitle", "someErrorDetail",
                    "sometrace here")).getMessage();
            assertThat(assertThrowsExactly(
                Neo4jClient.Neo4jAppError.class,
                () -> client.importDocuments(null)
            ).getMessage()).isEqualTo(expectedMsg);
        }


        @Test
        public void test_should_import_named_entities() {
            // Given
            neo4jApp.configure(routes -> routes.post("/named-entities", this::mockImport));
            // When
            org.icij.datashare.Objects.IncrementalImportRequest body =
                new org.icij.datashare.Objects.IncrementalImportRequest(null);
            org.icij.datashare.Objects.IncrementalImportResponse res =
                client.importNamedEntities(body);
            // Then
            assertThat(res.nToInsert).isEqualTo(3);
            assertThat(res.nInserted).isEqualTo(3);
        }

        @Test
        public void test_should_import_named_entities_with_query() {
            // Given
            neo4jApp.configure(routes -> routes.post("/named-entities", this::mockImport));
            HashMap<String, Object> query = new HashMap<>() {
                {
                    put("key1", "value1");
                }
            };
            org.icij.datashare.Objects.IncrementalImportRequest body =
                new org.icij.datashare.Objects.IncrementalImportRequest(query);
            // When
            org.icij.datashare.Objects.IncrementalImportResponse res =
                client.importNamedEntities(body);
            // Then
            assertThat(res.nToInsert).isEqualTo(3);
            assertThat(res.nInserted).isEqualTo(1);
        }
    }
}
