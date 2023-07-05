package org.icij.datashare;

import static org.fest.assertions.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
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
            String db = context.query().get("database");
            if (db.equals("mydb")) {
                if (Objects.equals(context.request().content(), "{}")) {
                    body = "{\"imported\": 3,\"nodesCreated\": 3,\"relationshipsCreated\": 3}";
                } else {
                    body = "{\"imported\": 3,\"nodesCreated\": 1,\"relationshipsCreated\": 1}";
                }
                return new Payload("application/json", body);
            }
            return new Payload("application/json",
                TestUtils.makeJsonHttpError("Bad Request", "Invalid DB " + db), 500);
        }

        private Payload mockSchema(Context context) throws IOException {
            String body;
            String db = context.query().get("database");
            if (db.equals("mydb")) {
                body = "{\"nodes\": [{\"something\": \"here\"}]," +
                    " \"relationships\": [{\"some_other\": \"there\"}]}";
                return new Payload("application/json", body);
            }
            return new Payload("application/json",
                TestUtils.makeJsonHttpError("Bad Request", "Invalid DB " + db), 500);
        }

        @Test
        public void test_should_import_documents() {
            // Given
            neo4jApp.configure(routes -> routes.post("/documents", this::mockImport));
            // When
            org.icij.datashare.Objects.IncrementalImportRequest body =
                new org.icij.datashare.Objects.IncrementalImportRequest(null);
            org.icij.datashare.Objects.IncrementalImportResponse res = client.importDocuments(
                "mydb", body);
            // Then
            assertThat(res.imported).isEqualTo(3);
            assertThat(res.nodesCreated).isEqualTo(3);
            assertThat(res.relationshipsCreated).isEqualTo(3);
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
            org.icij.datashare.Objects.IncrementalImportResponse res =
                client.importDocuments("mydb", body);
            // Then
            assertThat(res.imported).isEqualTo(3);
            assertThat(res.nodesCreated).isEqualTo(1);
            assertThat(res.relationshipsCreated).isEqualTo(1);
        }

        @Test
        public void test_import_documents_should_throw_for_invalid_db() {
            // Given
            neo4jApp.configure(
                routes -> routes.post("/documents", this::mockImport));
            // When
            org.icij.datashare.Objects.IncrementalImportRequest body =
                new org.icij.datashare.Objects.IncrementalImportRequest(null);
            assertThat(assertThrowsExactly(
                Neo4jClient.Neo4jAppError.class,
                () -> client.importDocuments("unknown", body)
            ).getMessage()).isEqualTo("Bad Request\nDetail: Invalid DB unknown");
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
                () -> client.importDocuments("mydb", null)
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
                () -> client.importDocuments("mydb", null)
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
                client.importNamedEntities("mydb", body);
            // Then
            assertThat(res.imported).isEqualTo(3);
            assertThat(res.nodesCreated).isEqualTo(3);
            assertThat(res.relationshipsCreated).isEqualTo(3);
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
                client.importNamedEntities("mydb", body);
            // Then
            assertThat(res.imported).isEqualTo(3);
            assertThat(res.nodesCreated).isEqualTo(1);
            assertThat(res.relationshipsCreated).isEqualTo(1);
        }

        @Test
        public void test_import_named_entities_should_throw_for_invalid_db() {
            // Given
            neo4jApp.configure(routes -> routes.post("/named-entities", this::mockImport));
            // When
            org.icij.datashare.Objects.IncrementalImportRequest body =
                new org.icij.datashare.Objects.IncrementalImportRequest(null);

            // Then
            assertThat(assertThrowsExactly(
                Neo4jClient.Neo4jAppError.class,
                () -> client.importNamedEntities("unknown", body)
            ).getMessage()).isEqualTo("Bad Request\nDetail: Invalid DB unknown");
        }

        @Test
        public void test_should_get_graph_schema() {
            // Given
            neo4jApp.configure(routes -> routes.get("/graphs/schema", this::mockSchema));
            // When
            HashMap<String, Object> res = client.graphSchema("mydb");
            // Then
            HashMap<String, Object> expected = new HashMap<>() {{
                put("nodes", List.of(new HashMap<String, String>() {{
                    put("something", "here");
                }}));
                put("relationships", List.of(new HashMap<String, String>() {{
                    put("some_other", "there");
                }}));
            }};
            assertThat(res).isEqualTo(expected);
        }


        @Test
        public void test_should_get_graph_schema_throw_for_invalid_db() {
            // Given
            neo4jApp.configure(routes -> routes.get("/graphs/schema", this::mockSchema));
            // When/Then
            assertThat(assertThrowsExactly(
                Neo4jClient.Neo4jAppError.class,
                () -> client.graphSchema("unknown")
            ).getMessage()).isEqualTo("Bad Request\nDetail: Invalid DB unknown");
        }
    }
}
