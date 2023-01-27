package org.icij.datashare;

import net.codestory.http.filters.basic.BasicAuthFilter;
import net.codestory.rest.FluentRestTest;
import net.codestory.rest.RestAssert;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashMap;

import static org.fest.assertions.Assertions.assertThat;
import static org.icij.datashare.TestUtils.assertJson;

public class Neo4jResourceTest {

    private static Neo4jResource neo4jAppResource;
    private static int port;
    private static int neo4jAppPort;

    private static PropertiesProvider propertyProvider;

    public static class MockAppProperties implements BeforeAllCallback {
        @Override
        public void beforeAll(ExtensionContext extensionContext) {
            propertyProvider = new PropertiesProvider(new HashMap<>() {{
                put("neo4jAppPort", Integer.toString(neo4jAppPort));
                put("neo4jStartServerCmd", "ls .");
            }});
        }
    }

    public static class PythonAppProperties implements BeforeAllCallback {
        @Override
        public void beforeAll(ExtensionContext extensionContext) {
            propertyProvider = new PropertiesProvider(new HashMap<>() {{
                put("neo4jAppPort", Integer.toString(neo4jAppPort));
                // TODO: how to fix the path ?
                put("neo4jStartServerCmd", "python -m http.server -d src/test/resources/test_app " + neo4jAppPort);
            }});
        }
    }

    public static class BindNeo4jResource extends ProdWebServerRuleExtension implements BeforeAllCallback {
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
    }

    public static class MockNeo4jApp extends ProdWebServerRuleExtension implements BeforeAllCallback, AfterEachCallback {
        @Override
        public void beforeAll(ExtensionContext extensionContext) {
            this.configure(routes -> routes.get("/ping", (context) -> new HashMap<String, String>() {{
                put("Method", "Get");
                put("Neo4jUrl", "/ping");
            }}));
            neo4jAppPort = this.port();
        }
        @Override
        public void afterEach(ExtensionContext extensionContext) {
            neo4jAppResource.stopServerProcess();
        }
    }

    public static class MockNeo4jAppWithPythonServer implements BeforeAllCallback, AfterEachCallback {
        @Override
        public void beforeAll(ExtensionContext extensionContext) {
            neo4jAppPort = 8080;
            if (neo4jAppPort == port) {
                throw new RuntimeException("neo4j port matches test server port by mischance...");
            }
        }

        @Override
        public void afterEach(ExtensionContext extensionContext) {
            neo4jAppResource.stopServerProcess();
        }
    }

    @ExtendWith(MockNeo4jApp.class)
    @ExtendWith(MockAppProperties.class)
    @ExtendWith(BindNeo4jResource.class)
    public static class Neo4jResourceTestWithMockNeo4jTest implements FluentRestTest {

        @Test
        public void test_not_be_running_by_default() {
            // When
            Neo4jResource.Neo4jAppStatus status = neo4jAppResource.getStopNeo4jApp();
            // Then
            assertThat(status.isRunning).isFalse();
        }

        @Test
        public void test_get_ping() throws IOException, URISyntaxException {
            // When
            neo4jAppResource.startServerProcess();
            get("/api/neo4j/ping").withPreemptiveAuthentication("foo", "null")
                    // Then
                    .should()
                    .respond(200)
                    .contain("\"Method\":\"Get\"").contain("\"Neo4jUrl\":\"/ping\"");
        }


        // TODO: test auth

        @Test
        public void test_get_status_should_return_200() {
            // When
            RestAssert assertion = get("/api/neo4j/status").withPreemptiveAuthentication("foo", "null");
            // Then
            assertion.should().respond(200);
            assertJson(
                    assertion.response().content(),
                    Neo4jResource.Neo4jAppStatus.class,
                    status -> assertThat(status.isRunning).isFalse()
            );
        }

        @Test
        public void test_get_unknown_url_should_return_404() {
            // When
            RestAssert assertion = get("/api/neo4j/unknown-url").withPreemptiveAuthentication("foo", "null");
            // Then
            assertion.should().respond(404);
        }

        @Override
        public int port() {
            return port;
        }
    }

    @ExtendWith(MockNeo4jAppWithPythonServer.class)
    @ExtendWith(PythonAppProperties.class)
    @ExtendWith(BindNeo4jResource.class)
    public static class Neo4jResourceTestWithPythonServerTest implements FluentRestTest {
        @Test
        public void test_get_ping_should_return_200() throws IOException, InterruptedException, URISyntaxException {
            // When
            neo4jAppResource.startServerProcess();
            neo4jAppResource.waitForServerToBeUp();
            RestAssert assertion = get("/api/neo4j/ping").withPreemptiveAuthentication("foo", "null");
            // Then
            assertion.should().respond(200);
            assertThat(assertion.response().content()).isEqualTo("pong");
        }

        @Test
        public void test_get_ping_should_return_503_when_neo4j_server_is_not_started() {
            // When
            RestAssert assertion = get("/api/neo4j/ping").withPreemptiveAuthentication("foo", "null");
            // Then
            assertion.should().respond(503);
            assertJson(
                    assertion.response().content(),
                    HttpUtils.HttpError.class,
                    status -> assertThat(status.detail)
                            .isEqualTo("Neo4j Python app is not running, please start it before calling the extension")
            );
        }

        @Test
        public void test_get_status_when_running() throws IOException, InterruptedException, URISyntaxException {
            // When
            neo4jAppResource.startServerProcess();
            neo4jAppResource.waitForServerToBeUp();
            RestAssert assertion = get("/api/neo4j/status").withPreemptiveAuthentication("foo", "null");
            // Then
            assertion.should().respond(200);
            assertJson(
                    assertion.response().content(),
                    Neo4jResource.Neo4jAppStatus.class,
                    status -> assertThat(status.isRunning).isTrue()
            );
        }

        @Override
        public int port() {
            return port;
        }
    }

}
