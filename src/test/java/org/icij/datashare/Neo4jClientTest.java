package org.icij.datashare;

import net.codestory.http.Configuration;
import net.codestory.http.Context;
import net.codestory.http.payload.Payload;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.io.IOException;
import java.util.Objects;

import static org.fest.assertions.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class Neo4jClientTest {

    private static Neo4jClient client;

    private static ProdWebServerRuleExtension neo4jApp;

    public static class Neo4jAppMock extends ProdWebServerRuleExtension implements BeforeAllCallback, AfterEachCallback {
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
    public static class ClientTest {

        private static Payload mockDocumentsImport(Context context) throws IOException {
            String body;
            if (Objects.equals(context.request().content(), "{}")) {
                body = "{\"nDocumentsToInsert\": 3,\"nDocumentsInserted\": 3}";
            } else {
                body = "{\"nDocumentsToInsert\": 3,\"nDocumentsInserted\": 1}";
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
            neo4jApp.configure(routes -> routes.post("/documents", ClientTest::mockDocumentsImport));
            // When
            Neo4jClient.DocumentImportResponse res = client.importDocuments(null);
            // Then
            assertThat(res.nDocumentsToInsert).isEqualTo(3);
            assertThat(res.nDocumentsInserted).isEqualTo(3);
        }

        @Test
        public void test_should_import_documents_with_query() {
            // Given
            neo4jApp.configure(routes -> routes.post("/documents", ClientTest::mockDocumentsImport));
            // When
            Neo4jClient.DocumentImportResponse res = client.importDocuments("{\"someQuery\": \"here\"}");
            // Then
            assertThat(res.nDocumentsToInsert).isEqualTo(3);
            assertThat(res.nDocumentsInserted).isEqualTo(1);
        }

        @Test
        public void test_import_documents_should_throw() {
            // Given
            neo4jApp.configure(routes -> routes.post("/documents",
                    (context) -> {
                        String jsonError = TestUtils.makeJsonHttpError("someTile", "someErrorDetail");
                        return new Payload("application/json", jsonError).withCode(500);
                    })
            );
            // When/Then
            assertThrows(Neo4jClient.Neo4jAppError.class, () -> client.importDocuments(null), "someErrorDetail");
        }
    }
}
