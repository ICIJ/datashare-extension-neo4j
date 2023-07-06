package org.icij.datashare;

import static org.fest.assertions.Assertions.assertThat;
import static org.icij.datashare.json.JsonObjectMapper.MAPPER;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
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
            String db = context.query().get("database");
            String index = context.query().get("index");
            if (db != null && db.equals("mydb")) {
                if (index != null && index.equals("myindex")) {
                    if (Objects.equals(context.request().content(), "{}")) {
                        body = "{\"imported\": 3,\"nodesCreated\": 3,\"relationshipsCreated\": 3}";
                    } else {
                        body = "{\"imported\": 3,\"nodesCreated\": 1,\"relationshipsCreated\": 1}";
                    }
                    return new Payload("application/json", body);
                }
                return new Payload("application/json",
                    TestUtils.makeJsonHttpError("Bad Request", "Invalid index " + index), 500);
            }
            return new Payload("application/json",
                TestUtils.makeJsonHttpError("Bad Request", "Invalid DB " + db), 500);
        }

        private Payload mockDump(Context context) throws IOException {
            String db = context.query().get("database");
            if (db.equals("mydb")) {
                org.icij.datashare.Objects.DumpRequest request =
                    MAPPER.readValue(context.request().content(),
                        org.icij.datashare.Objects.DumpRequest.class);
                String dump;
                if (request.query == null) {
                    switch (request.format) {
                        case CYPHER_SHELL:
                            dump = "cypher\ndump";
                            break;
                        case GRAPHML:
                            dump = "graphml\ndump";
                            break;
                        default:
                            return new Payload("application/json",
                                TestUtils.makeJsonHttpError("Bad Request",
                                    "Unknown format" + request.format), 400);
                    }
                } else {
                    dump = "filtered\ndump";
                }
                return new Payload("binary/octet-stream",
                    new ByteArrayInputStream(dump.getBytes()));
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
                "mydb", "myindex", body);
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
                client.importDocuments("mydb", "myindex", body);
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
            org.icij.datashare.Objects.IncrementalImportRequest body =
                new org.icij.datashare.Objects.IncrementalImportRequest(null);
            // When/Then
            assertThat(assertThrowsExactly(
                Neo4jClient.Neo4jAppError.class,
                () -> client.importDocuments("unknown", "myindex", body)
            ).getMessage()).isEqualTo("Bad Request\nDetail: Invalid DB unknown");
        }

        @Test
        public void test_import_documents_should_throw_for_invalid_index() {
            // Given
            neo4jApp.configure(
                routes -> routes.post("/documents", this::mockImport));
            // When
            org.icij.datashare.Objects.IncrementalImportRequest body =
                new org.icij.datashare.Objects.IncrementalImportRequest(null);
            assertThat(assertThrowsExactly(
                Neo4jClient.Neo4jAppError.class,
                () -> client.importDocuments("mydb", "unknown", body)
            ).getMessage()).isEqualTo("Bad Request\nDetail: Invalid index unknown");
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
                () -> client.importDocuments("mydb", "myindex", null)
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
                () -> client.importDocuments("mydb", "myindex", null)
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
                client.importNamedEntities("mydb", "myindex", body);
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
                client.importNamedEntities("mydb", "myindex", body);
            // Then
            assertThat(res.imported).isEqualTo(3);
            assertThat(res.nodesCreated).isEqualTo(1);
            assertThat(res.relationshipsCreated).isEqualTo(1);
        }

        @Test
        public void test_import_named_entities_should_throw_for_invalid_db() {
            // Given
            neo4jApp.configure(routes -> routes.post("/named-entities", this::mockImport));

            org.icij.datashare.Objects.IncrementalImportRequest body =
                new org.icij.datashare.Objects.IncrementalImportRequest(null);

            // When/Then
            assertThat(assertThrowsExactly(
                Neo4jClient.Neo4jAppError.class,
                () -> client.importNamedEntities("unknown", "myindex", body)
            ).getMessage()).isEqualTo("Bad Request\nDetail: Invalid DB unknown");
        }

        @Test
        public void test_import_named_entities_should_throw_for_invalid_index() {
            // Given
            neo4jApp.configure(routes -> routes.post("/named-entities", this::mockImport));
            // When
            org.icij.datashare.Objects.IncrementalImportRequest body =
                new org.icij.datashare.Objects.IncrementalImportRequest(null);

            // Then
            assertThat(assertThrowsExactly(
                Neo4jClient.Neo4jAppError.class,
                () -> client.importNamedEntities("mydb", "unknown", body)
            ).getMessage()).isEqualTo("Bad Request\nDetail: Invalid index unknown");
        }

        @Test
        public void test_dump_graph_in_cypher_shell()
            throws URISyntaxException, IOException, InterruptedException {
            // Given
            neo4jApp.configure(routes -> routes.post("/graphs/dump?database=mydb", this::mockDump));
            org.icij.datashare.Objects.DumpRequest body =
                new org.icij.datashare.Objects.DumpRequest(
                    org.icij.datashare.Objects.DumpFormat.CYPHER_SHELL, null);
            // When
            try (InputStream res = client.dumpGraph("mydb", body)) {
                String dump = new String(res.readAllBytes(), StandardCharsets.UTF_8);

                // Then
                String expectedDump = "cypher\ndump";
                assertThat(dump).isEqualTo(expectedDump);
            }
        }

        @Test
        public void test_dump_graph_in_graphml()
            throws URISyntaxException, IOException, InterruptedException {
            // Given
            neo4jApp.configure(routes -> routes.post("/graphs/dump", this::mockDump));
            org.icij.datashare.Objects.DumpRequest body =
                new org.icij.datashare.Objects.DumpRequest(
                    org.icij.datashare.Objects.DumpFormat.GRAPHML, null);
            // When
            try (InputStream res = client.dumpGraph("mydb", body)) {
                String dump = new String(res.readAllBytes(), StandardCharsets.UTF_8);
                // Then
                String expectedDump = "graphml\ndump";
                assertThat(dump).isEqualTo(expectedDump);
            }
        }

        @Test
        public void test_dump_graph_in_with_query()
            throws URISyntaxException, IOException, InterruptedException {
            // Given
            neo4jApp.configure(routes -> routes.post("/graphs/dump", this::mockDump));
            org.icij.datashare.Objects.DumpRequest body =
                new org.icij.datashare.Objects.DumpRequest(
                    org.icij.datashare.Objects.DumpFormat.CYPHER_SHELL,
                    "MATCH (something) RETURN something LIMIT 100");
            // When
            try (InputStream res = client.dumpGraph("mydb", body)) {
                String dump = new String(res.readAllBytes(), StandardCharsets.UTF_8);
                // Then
                String expectedDump = "filtered\ndump";
                assertThat(dump).isEqualTo(expectedDump);
            }
        }

        @Test
        public void test_dump_graph_should_throw_for_invalid_db() {
            // Given
            neo4jApp.configure(
                routes -> routes.post("/graphs/dump", this::mockDump));
            org.icij.datashare.Objects.DumpRequest body =
                new org.icij.datashare.Objects.DumpRequest(
                    org.icij.datashare.Objects.DumpFormat.CYPHER_SHELL, null);

            // When/Then
            assertThat(assertThrowsExactly(
                Neo4jClient.Neo4jAppError.class,
                () -> client.dumpGraph("unknown", body)
            ).getMessage()).isEqualTo("Bad Request\nDetail: Invalid DB unknown");
        }
    }
}
