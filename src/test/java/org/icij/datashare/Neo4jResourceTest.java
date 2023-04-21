package org.icij.datashare;

import static org.fest.assertions.Assertions.assertThat;
import static org.icij.datashare.TestUtils.assertJson;

import java.io.IOException;
import java.util.HashMap;
import net.codestory.http.filters.basic.BasicAuthFilter;
import net.codestory.http.payload.Payload;
import net.codestory.rest.FluentRestTest;
import net.codestory.rest.Response;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;

public class Neo4jResourceTest {

    static Neo4jClient client;
    private static Neo4jResource neo4jAppResource;
    private static int port;
    private static int neo4jAppPort;
    private static ProdWebServerRuleExtension neo4jApp;
    private static PropertiesProvider propertyProvider;

    public static class MockAppProperties implements BeforeAllCallback {
        @Override
        public void beforeAll(ExtensionContext extensionContext) {
            propertyProvider = new PropertiesProvider(new HashMap<>() {
                {
                    put("neo4jAppPort", Integer.toString(neo4jAppPort));
                    put("neo4jProject", "foo-datashare");
                    // TODO: fix this path ?
                    put("neo4jStartServerCmd", "src/test/resources/shell_mock");
                }
            });
        }
    }

    public static class PythonAppProperties implements BeforeAllCallback {
        @Override
        public void beforeAll(ExtensionContext extensionContext) {
            propertyProvider = new PropertiesProvider(new HashMap<>() {
                {
                    put("neo4jAppPort", Integer.toString(neo4jAppPort));
                    put("neo4jProject", "foo-datashare");
                    // TODO: how to fix the path ?
                    put("neo4jStartServerCmd", "src/test/resources/python_mock " + neo4jAppPort);
                }
            });
            client = new Neo4jClient(neo4jAppPort);
        }
    }

    public static class BindNeo4jResource extends ProdWebServerRuleExtension
        implements BeforeAllCallback, AfterEachCallback {
        @Override
        public void beforeAll(ExtensionContext extensionContext) {
            neo4jAppResource = new Neo4jResource(propertyProvider);
            this.configure(
                routes -> routes
                    .add(neo4jAppResource)
                    .filter(new BasicAuthFilter("/api", "ds", DatashareUser.singleUser("foo")))
            );
            port = this.port();
        }

        @Override
        public void afterEach(ExtensionContext extensionContext) {
            neo4jAppResource.close();
        }
    }

    public static class MockNeo4jApp extends ProdWebServerRuleExtension
        implements BeforeAllCallback {
        @Override
        public void beforeAll(ExtensionContext extensionContext) {
            neo4jAppPort = this.port();
            neo4jApp = this;
        }
    }

    public static class MockNeo4jAppWithPythonServer
        implements BeforeAllCallback, AfterEachCallback {
        @Override
        public void beforeAll(ExtensionContext extensionContext) {
            neo4jAppPort = 8080;
            if (neo4jAppPort == port) {
                throw new RuntimeException("neo4j port matches test server port by mischance...");
            }
        }

        @Override
        public void afterEach(ExtensionContext extensionContext) {
            neo4jAppResource.close();
        }
    }

    public static class SetNeo4jAppPort implements BeforeAllCallback, AfterEachCallback {
        @Override
        public void beforeAll(ExtensionContext extensionContext) {
            neo4jAppPort = 8080;
        }

        @Override
        public void afterEach(ExtensionContext extensionContext) {
            neo4jAppResource.close();
        }
    }

    @ExtendWith(MockNeo4jApp.class)
    @ExtendWith(MockAppProperties.class)
    @ExtendWith(BindNeo4jResource.class)
    @DisplayName("Neo4jResource tests with HTTP server mock")
    @Nested
    class Neo4jResourceTestWithMockNeo4jTest implements FluentRestTest {

        @Override
        public int port() {
            return port;
        }

        @Test
        public void test_not_be_running_by_default() {
            // When
            Neo4jResource.Neo4jAppStatus status = neo4jAppResource.getStopNeo4jApp();
            // Then
            assertThat(status.isRunning).isFalse();
        }

        @Test
        public void test_get_ping() throws IOException, InterruptedException {
            // When
            neo4jAppResource.startServerProcess(false);
            neo4jApp.configure(routes -> routes.get("/ping", (context) -> "pong"));
            Response res =
                get("/api/neo4j/ping").withPreemptiveAuthentication("foo", "null").response();
            // Then
            assertThat(res.code()).isEqualTo(200);
            assertThat(res.content()).isEqualTo("pong");
        }


        // TODO: test auth

        @Test
        public void test_get_status_should_return_200() {
            // When
            Response response =
                get("/api/neo4j/status").withPreemptiveAuthentication("foo", "null").response();
            // Then
            assertThat(response.code()).isEqualTo(200);
            assertJson(
                response.content(),
                Neo4jResource.Neo4jAppStatus.class,
                status -> assertThat(status.isRunning).isFalse()
            );
        }

        @Test
        public void test_get_unknown_url_should_return_404() {
            // When
            Response response =
                get("/api/neo4j/unknown-url").withPreemptiveAuthentication("foo", "null")
                    .response();
            // Then
            assertThat(response.code()).isEqualTo(404);
        }
    }

    @ExtendWith(MockNeo4jAppWithPythonServer.class)
    @ExtendWith(PythonAppProperties.class)
    @ExtendWith(BindNeo4jResource.class)
    @DisplayName("Neo4jResource tests with Python server running in a process")
    @Nested
    class Neo4jResourceTestWithPythonServerTest implements FluentRestTest {
        @Override
        public int port() {
            return port;
        }

        @Test
        public void test_get_ping_should_return_200() throws IOException, InterruptedException {
            // When
            neo4jAppResource.startServerProcess(false);
            Response response =
                get("/api/neo4j/ping").withPreemptiveAuthentication("foo", "null").response();
            // Then
            assertThat(response.code()).isEqualTo(200);
            assertThat(response.content()).isEqualTo("pong");
        }

        @Test
        public void test_get_ping_should_return_503_when_neo4j_server_is_not_started() {
            // When
            Response response =
                get("/api/neo4j/ping").withPreemptiveAuthentication("foo", "null").response();
            // Then
            assertThat(response.code()).isEqualTo(503);
            assertJson(
                response.content(),
                HttpUtils.HttpError.class,
                status -> assertThat(status.detail)
                    .isEqualTo(
                        "neo4j Python app is not running, please start it before "
                            + "calling the extension"
                    )
            );
        }

        @Test
        public void test_get_status_when_running() throws IOException, InterruptedException {
            // When
            neo4jAppResource.startServerProcess(false);
            Response response =
                get("/api/neo4j/status").withPreemptiveAuthentication("foo", "null").response();
            // Then
            assertThat(response.code()).isEqualTo(200);
            assertJson(
                response.content(),
                Neo4jResource.Neo4jAppStatus.class,
                status -> assertThat(status.isRunning).isTrue()
            );
        }

        @Test
        public void test_post_start_should_return_200() {
            // When
            Response response =
                post("/api/neo4j/start").withPreemptiveAuthentication("foo", "null").response();
            // Then
            assertThat(response.code()).isEqualTo(200);
            assertJson(
                response.content(),
                Neo4jResource.ServerStartResponse.class,
                res -> assertThat(res.alreadyRunning).isFalse()
            );
        }

        @Test
        public void test_post_start_should_return_200_when_already_started()
            throws IOException, InterruptedException {
            // When
            neo4jAppResource.startServerProcess(false);
            Response response =
                post("/api/neo4j/start").withPreemptiveAuthentication("foo", "null").response();
            // Then
            assertThat(response.code()).isEqualTo(200);
            assertJson(
                response.content(),
                Neo4jResource.ServerStartResponse.class,
                res -> assertThat(res.alreadyRunning).isTrue()
            );
        }

        @Test
        public void test_post_stop_should_return_200() {
            // When
            Response response =
                post("/api/neo4j/stop").withPreemptiveAuthentication("foo", "null").response();
            // Then
            assertThat(response.code()).isEqualTo(200);
            assertJson(
                response.content(),
                Neo4jResource.ServerStopResponse.class,
                res -> assertThat(res.alreadyStopped).isTrue()
            );
        }

        @Test
        public void test_post_stop_should_return_200_when_already_started()
            throws IOException, InterruptedException {
            // When
            neo4jAppResource.startServerProcess(false);
            Response response =
                post("/api/neo4j/stop").withPreemptiveAuthentication("foo", "null").response();
            // Then
            assertThat(response.code()).isEqualTo(200);
            assertJson(
                response.content(),
                Neo4jResource.ServerStopResponse.class,
                res -> assertThat(res.alreadyStopped).isFalse()
            );
        }
    }

    @ExtendWith(SetNeo4jAppPort.class)
    @ExtendWith(MockAppProperties.class)
    @ExtendWith(BindNeo4jResource.class)
    @DisplayName("Neo4jResource test without mock")
    @Nested
    class Neo4jResourceLifecycleTest implements FluentRestTest {
        @Override
        public int port() {
            return port;
        }

        @Test
        public void test_post_start_should_return_500_for_phantom_process()
            throws InterruptedException {
            // Given
            try (PhantomPythonServerMock ignored = new PhantomPythonServerMock()) {
                // When
                Response response =
                    post("/api/neo4j/start").withPreemptiveAuthentication("foo", "null").response();
                // Then
                assertThat(response.code()).isEqualTo(500);
                assertJson(
                    response.content(),
                    HttpUtils.HttpError.class,
                    status -> assertThat(status.detail)
                        .isEqualTo(
                            "neo4j Python app is already running likely in another phantom process")
                );
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        class PhantomPythonServerMock implements AutoCloseable {
            private final Process process;

            public PhantomPythonServerMock() throws IOException, InterruptedException {
                this.process = new ProcessBuilder(
                    "python3",
                    "-m",
                    "http.server",
                    "-d",
                    "src/test/resources/python_mock",
                    "8080"
                ).start();
                neo4jAppResource.waitForServerToBeUp();
            }

            @Override
            public void close() {
                process.destroyForcibly();
            }
        }
    }

    @DisplayName("test import")
    @ExtendWith(MockNeo4jApp.class)
    @ExtendWith(MockAppProperties.class)
    @ExtendWith(BindNeo4jResource.class)
    @Nested
    class Neo4jResourceImportTest implements FluentRestTest {
        @Override
        public int port() {
            return port;
        }

        @Test
        public void test_post_documents_import_should_return_200()
            throws IOException, InterruptedException {
            // Given
            neo4jAppResource.startServerProcess(false);
            neo4jApp.configure(
                routes -> routes.post(
                    "/documents",
                    context -> new Payload("application/json",
                        "{\"imported\": 10,\"nodesCreated\": 8}")
                )
            );
            // When
            Response response = post("/api/neo4j/documents?project=foo-datashare", "{}")
                .withPreemptiveAuthentication("foo", "null")
                .response();
            // Then
            assertThat(response.code()).isEqualTo(200);
            assertJson(
                response.content(),
                Objects.IncrementalImportResponse.class,
                res -> {
                    assertThat(res.imported).isEqualTo(10);
                    assertThat(res.nodesCreated).isEqualTo(8);
                    assertThat(res.relationshipsCreated).isEqualTo(0);
                }
            );
        }

        @Test
        public void test_post_documents_import_should_return_401_for_invalid_project() {
            // When
            Response response = post("/api/neo4j/documents?project=unknownproject")
                .withPreemptiveAuthentication("foo", "null")
                .response();
            // Then
            assertThat(response.code()).isEqualTo(401);
        }

        @Test
        public void test_post_documents_import_should_return_401_for_unauthorized_user() {
            // When
            Response response = post("/api/neo4j/documents?project=foo-datashare")
                .withPreemptiveAuthentication("unauthorized", "null")
                .response();
            // Then
            assertThat(response.code()).isEqualTo(401);
        }

        @Test
        public void test_post_named_entities_import_should_return_200()
            throws IOException, InterruptedException {
            // Given
            neo4jAppResource.startServerProcess(false);
            neo4jApp.configure(
                routes -> routes.post(
                    "/named-entities",
                    context -> new Payload("application/json",
                        "{\"imported\": 10,\"nodesCreated\": 8}")
                )
            );
            // When
            Response response = post("/api/neo4j/named-entities?project=foo-datashare", "{}")
                .withPreemptiveAuthentication("foo", "null")
                .response();
            // Then
            assertThat(response.code()).isEqualTo(200);
            assertJson(
                response.content(),
                Objects.IncrementalImportResponse.class,
                res -> {
                    assertThat(res.imported).isEqualTo(10);
                    assertThat(res.nodesCreated).isEqualTo(8);
                    assertThat(res.relationshipsCreated).isEqualTo(0);
                }
            );
        }

        @Test
        public void test_post_named_entities_import_should_return_401_for_invalid_project() {
            // When
            Response response = post(
                "/api/neo4j/named-entities?project=unknownproject").withPreemptiveAuthentication(
                "foo", "null").response();
            // Then
            assertThat(response.code()).isEqualTo(401);
        }

        @Test
        public void test_post_named_entities_import_should_return_401_for_unauthorized_user() {
            // When
            Response response = post(
                "/api/neo4j/named-entities?project=foo-datashare").withPreemptiveAuthentication(
                "unauthorized", "null").response();
            // Then
            assertThat(response.code()).isEqualTo(401);
        }
    }
}
