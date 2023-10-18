package org.icij.datashare;

import static org.fest.assertions.Assertions.assertThat;
import static org.icij.datashare.TestUtils.assertJson;
import static org.icij.datashare.json.JsonObjectMapper.MAPPER;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import net.codestory.http.annotations.Prefix;
import net.codestory.http.filters.basic.BasicAuthFilter;
import net.codestory.http.payload.Payload;
import net.codestory.rest.FluentRestTest;
import net.codestory.rest.Response;
import org.icij.datashare.text.Project;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.mockito.Mock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Neo4jResourceTest {

    public static final String SINGLE_PROJECT = "foo-datashare";
    private static final Logger logger = LoggerFactory.getLogger(Neo4jResource.class);

    @Prefix("/api/neo4j")
    static class Neo4jResourceWithApp extends Neo4jResource {

        public Neo4jResourceWithApp(Repository repository, PropertiesProvider propertiesProvider) {
            super(repository, propertiesProvider);
        }

        @Override
        protected void checkNeo4jAppStarted() {
            logger.debug(Neo4jResourceWithApp.class.getName() + " is always running");
        }

    }

    static Neo4jClient client;
    private static Neo4jResource neo4jAppResource;
    private static int port;
    private static int neo4jAppPort;
    private static ProdWebServerRuleExtension neo4jApp;
    private static PropertiesProvider propertyProvider;
    private static Repository parentRepository;

    public static class MockProperties implements AfterEachCallback {
        @Override
        public void afterEach(ExtensionContext extensionContext) {
            Neo4jResource.stopServerProcess();
        }
    }

    public static class MockAppProperties extends MockProperties implements BeforeAllCallback {
        @Override
        public void beforeAll(ExtensionContext extensionContext) {
            propertyProvider = new PropertiesProvider(new HashMap<>() {
                {
                    put("neo4jAppPort", Integer.toString(neo4jAppPort));
                    put("neo4jSingleProject", SINGLE_PROJECT);
                    put("mode", "LOCAL");
                    put("neo4jStartServerCmd", "src/test/resources/shell_mock");
                    put("neo4jAppStartTimeoutS", "2");
                }
            });
        }
    }

    public static class MockLocalModeProperties extends MockProperties
        implements BeforeAllCallback {
        @Override
        public void beforeAll(ExtensionContext extensionContext) {
            propertyProvider = new PropertiesProvider(new HashMap<>() {
                {
                    put("neo4jAppPort", Integer.toString(neo4jAppPort));
                    put("neo4jSingleProject", SINGLE_PROJECT);
                    put("mode", "LOCAL");
                    put("neo4jStartServerCmd", "src/test/resources/shell_mock");
                }
            });
        }
    }

    public static class MockEmbeddedModeProperties extends MockProperties
        implements BeforeAllCallback {
        @Override
        public void beforeAll(ExtensionContext extensionContext) {
            propertyProvider = new PropertiesProvider(new HashMap<>() {
                {
                    put("neo4jAppPort", Integer.toString(neo4jAppPort));
                    put("neo4jSingleProject", SINGLE_PROJECT);
                    put("mode", "EMBEDDED");
                    put("neo4jStartServerCmd", "src/test/resources/shell_mock");
                }
            });
        }
    }

    public static class MockCliModeProperties extends MockProperties
        implements BeforeAllCallback {
        @Override
        public void beforeAll(ExtensionContext extensionContext) {
            propertyProvider = new PropertiesProvider(new HashMap<>() {
                {
                    put("neo4jAppPort", Integer.toString(neo4jAppPort));
                    put("neo4jSingleProject", SINGLE_PROJECT);
                    put("mode", "CLI");
                    put("neo4jStartServerCmd", "src/test/resources/shell_mock");
                }
            });
        }
    }

    public static class MockServerModeProperties extends MockProperties
        implements BeforeAllCallback {
        @Override
        public void beforeAll(ExtensionContext extensionContext) {
            propertyProvider = new PropertiesProvider(new HashMap<>() {
                {
                    put("neo4jAppPort", Integer.toString(neo4jAppPort));
                    put("neo4jSingleProject", SINGLE_PROJECT);
                    put("mode", "SERVER");
                    put("neo4jStartServerCmd", "src/test/resources/shell_mock");
                }
            });
        }
    }

    public static class PythonAppProperties extends MockProperties implements BeforeAllCallback {
        @Override
        public void beforeAll(ExtensionContext extensionContext) {
            propertyProvider = new PropertiesProvider(new HashMap<>() {
                {
                    put("neo4jAppPort", Integer.toString(neo4jAppPort));
                    put("neo4jSingleProject", SINGLE_PROJECT);
                    // TODO: how to fix the path ?
                    put("neo4jStartServerCmd", "src/test/resources/python_mock " + neo4jAppPort);
                }
            });
            client = new Neo4jClient(neo4jAppPort);
        }
    }

    public abstract static class BindNeo4jResourceBase
        extends ProdWebServerRuleExtension
        implements BeforeAllCallback, AfterEachCallback {
        @Mock
        private static Repository mockedRepository;

        protected <T extends Neo4jResource> Class<T> getResourceClass() {
            return (Class<T>) Neo4jResource.class;
        }

        @Override
        public void beforeAll(ExtensionContext extensionContext)
            throws NoSuchMethodException, InvocationTargetException, InstantiationException,
            IllegalAccessException {
            initMocks(this);
            parentRepository = mockedRepository;

            neo4jAppResource = getResourceClass()
                .getConstructor(Repository.class, PropertiesProvider.class)
                .newInstance(mockedRepository, propertyProvider);
            this.configure(
                routes -> routes
                    .add(neo4jAppResource)
                    .filter(new BasicAuthFilter("/api", "ds", DatashareUser.singleUser("foo")))
            );
            port = this.port();
        }

        @Override
        public void afterEach(ExtensionContext extensionContext) {
            reset(parentRepository);
        }
    }

    public static class BindNeo4jResource extends BindNeo4jResourceBase {
        @Override
        public void beforeAll(ExtensionContext extensionContext)
            throws InvocationTargetException, NoSuchMethodException, InstantiationException,
            IllegalAccessException {
            super.beforeAll(extensionContext);
            Neo4jResource.supportNeo4jEnterprise = false;
        }
    }

    public static class BindNeo4jResourceEnterprise extends BindNeo4jResourceBase {
        @Override
        public void beforeAll(ExtensionContext extensionContext)
            throws InvocationTargetException, NoSuchMethodException, InstantiationException,
            IllegalAccessException {
            super.beforeAll(extensionContext);
            Neo4jResource.supportNeo4jEnterprise = true;
        }
    }

    public static class BindNeo4jResourceWithPid extends BindNeo4jResource {
        protected <T extends Neo4jResource> Class<T> getResourceClass() {
            return (Class<T>) Neo4jResourceWithApp.class;
        }
    }


    public static class MockNeo4jApp extends ProdWebServerRuleExtension
        implements BeforeAllCallback, AfterAllCallback {
        @Override
        public void beforeAll(ExtensionContext extensionContext) {
            neo4jAppPort = this.port();
            neo4jApp = this;
            this.configure(routes -> routes.get("/ping", "pong"));
            Neo4jResource.projects.add(SINGLE_PROJECT);
        }

        @Override
        public void afterAll(ExtensionContext extensionContext) {
            Neo4jResource.projects.remove(SINGLE_PROJECT);
        }
    }

    public static class MockNotReadyNeo4jApp extends ProdWebServerRuleExtension
        implements BeforeAllCallback {
        @Override
        public void beforeAll(ExtensionContext extensionContext) {
            neo4jAppPort = this.port();
            neo4jApp = this;
            neo4jApp.configure(
                routes -> routes.get(
                    "/ping",
                    context -> new Payload(500)
                )
            );
        }
    }

    public static class MockNeo4jAppWithPythonServer implements BeforeAllCallback {
        @Override
        public void beforeAll(ExtensionContext extensionContext) {
            neo4jAppPort = 8080;
            if (neo4jAppPort == port) {
                throw new RuntimeException("neo4j port matches test server port by mischance...");
            }
        }
    }

    public static class MockOpenPort implements BeforeAllCallback {
        @Override
        public void beforeAll(ExtensionContext extensionContext) {
            neo4jAppPort = port;
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
            Payload payload = neo4jAppResource.getStopNeo4jApp();
            Neo4jResource.Neo4jAppStatus status =
                (Neo4jResource.Neo4jAppStatus) payload.rawContent();
            // Then
            assertThat(status.isRunning).isFalse();
        }


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
        public void test_post_import_doc_should_return_503_when_neo4j_server_is_not_started() {
            // When
            Response response = post("/api/neo4j/documents?project=foo-datashare", "{}")
                .withPreemptiveAuthentication("foo", "null").response();
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
        public void test_get_status_when_running() {
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
        public void test_post_start_should_return_200_when_already_started() {
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
        public void test_post_stop_should_return_200_when_already_started() {
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

    @ExtendWith(MockNeo4jApp.class)
    @ExtendWith(MockAppProperties.class)
    @ExtendWith(MockOpenPort.class)
    @ExtendWith(BindNeo4jResource.class)
    @DisplayName("Neo4jResource test without mock")
    @Nested
    class Neo4jResourceLifecycleTest implements FluentRestTest {
        @Override
        public int port() {
            return port;
        }

        @Test
        public void test_post_start_should_return_500_for_phantom_process() {
            // When
            Response response = post("/api/neo4j/start").withPreemptiveAuthentication(
                "foo", "null").response();
            // Then
            assertThat(response.code()).isEqualTo(500);
            String expected = "neo4j Python app is already running, likely in another phantom"
                + " process";
            assertJson(
                response.content(),
                HttpUtils.HttpError.class,
                status -> assertThat(status.detail).isEqualTo(expected)
            );
        }
    }


    @DisplayName("test with mocked app")
    @ExtendWith(MockNeo4jApp.class)
    @ExtendWith(MockAppProperties.class)
    @ExtendWith(BindNeo4jResourceWithPid.class)
    @Nested
    class Neo4jResourceImportTest implements FluentRestTest {
        @Override
        public int port() {
            return port;
        }

        @Test
        public void test_init_project_should_return_200() {
            // Given
            Neo4jResource.projects.clear();
            neo4jApp.configure(
                routes -> routes.post("/projects/init", context -> new Payload(200))
            );

            // When
            Response response = post("/api/neo4j/init?project=foo-datashare")
                .withPreemptiveAuthentication("foo", "null")
                .response();

            // Then
            assertThat(response.code()).isEqualTo(200);
        }

        @Test
        public void test_init_project_should_return_201() {
            // Given
            Neo4jResource.projects.clear();
            neo4jApp.configure(
                routes -> routes.post("/projects/init", context -> new Payload(201))
            );

            // When
            Response response = post("/api/neo4j/init?project=foo-datashare")
                .withPreemptiveAuthentication("foo", "null")
                .response();

            // Then
            assertThat(response.code()).isEqualTo(201);
        }

        @Test
        public void test_init_project_should_cache() {
            // Given
            Neo4jResource.projects.add("foo-datashare");

            neo4jApp.configure(
                routes -> routes.post("/projects/init", context -> new Payload(418))
            );

            // When
            Response response = post("/api/neo4j/init?project=foo-datashare")
                .withPreemptiveAuthentication("foo", "null")
                .response();

            // Then
            assertThat(response.code()).isEqualTo(200);
        }


        @Test
        public void test_init_project_should_return_401_for_unauthorized_user() {
            // Given
            neo4jApp.configure(
                routes -> routes.post("/projects/init", context -> new Payload(418))
            );

            // When
            Response response = post("/api/neo4j/init?project=foo-datashare")
                .withPreemptiveAuthentication("unauthorized", "null")
                .response();

            // Then
            assertThat(response.code()).isEqualTo(401);
        }


        @Test
        public void test_post_documents_import_should_return_200() {
            // Given
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
        public void test_post_documents_import_should_return_403_for_invalid_project() {
            // When
            Response response = post("/api/neo4j/documents?project=unknownproject")
                .withPreemptiveAuthentication("foo", "null")
                .response();
            // Then
            assertThat(response.code()).isEqualTo(403);
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
        public void test_post_named_entities_import_should_return_200() {
            // Given
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
        public void test_post_named_entities_import_should_return_403_for_invalid_project() {
            // When
            Response response = post(
                "/api/neo4j/named-entities?project=unknownproject").withPreemptiveAuthentication(
                "foo", "null").response();
            // Then
            assertThat(response.code()).isEqualTo(403);
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

        @Test
        public void test_post_admin_neo4j_csvs_should_return_403_when_not_in_local() {
            // When
            Response response = post(
                "/api/neo4j/admin/neo4j-csvs?project=foo-datashare").withPreemptiveAuthentication(
                "foo", "null").response();
            // Then
            assertThat(response.code()).isEqualTo(403);
        }

        @Test
        public void test_post_graph_dump_should_return_200() {
            // Given
            neo4jApp.configure(
                routes -> routes.post("/graphs/dump",
                    context -> new Payload("binary/octet-stream", "somedump".getBytes())
                )
            );
            // When
            String body = "{\"format\": \"graphml\"}";
            Response response = post("/api/neo4j/graphs/dump?project=foo-datashare",
                body).withPreemptiveAuthentication("foo", "null").response();
            // Then
            assertThat(response.code()).isEqualTo(200);
            String dumpAsString = response.content();
            assertThat(dumpAsString).isEqualTo("somedump");
        }

        @Test
        public void test_post_graph_dump_should_return_401_for_unauthorized_user() {
            // Given
            neo4jApp.configure(
                routes -> routes.post("/graphs/dump",
                    context -> new Payload("binary/octet-stream", "somedump".getBytes())
                )
            );
            // When
            String body = "{\"format\": \"graphml\"}";
            Response response = post("/api/neo4j/graphs/dump?project=foo-datashare",
                body).withPreemptiveAuthentication("unauthorized", "null").response();
            // Then
            assertThat(response.code()).isEqualTo(401);
        }

        @Test
        public void test_post_graph_dump_should_return_403_for_forbidden_mask() {
            // Given
            neo4jApp.configure(
                routes -> routes.post("/graphs/dump",
                    context -> new Payload("binary/octet-stream", "somedump".getBytes())
                )
            );
            // When
            when(parentRepository.getProject("foo-datashare"))
                .thenReturn(new Project("foo-datashare", "1.2.3.4"));
            String body = "{\"format\": \"graphml\"}";
            Response response = post("/api/neo4j/graphs/dump?project=foo-datashare",
                body).withPreemptiveAuthentication("foo", "null").response();
            // Then
            assertThat(response.code()).isEqualTo(403);
        }

        @Test
        public void test_post_sorted_graph_dump_should_return_200() {
            // Given
            neo4jApp.configure(
                routes -> routes.post("/graphs/dump",
                    context -> new Payload("binary/octet-stream", "somedump".getBytes())
                )
            );
            // When
            String body = "{"
                + "\"format\": \"graphml\", "
                + "\"query\": {\"sort\": [{\"property\": \"path\", \"direction\": \"DESC\"}],"
                + " \"limit\": 10}"
                + "}";
            Response response = post("/api/neo4j/graphs/sorted-dump?project=foo-datashare",
                body).withPreemptiveAuthentication("foo", "null").response();
            // Then
            assertThat(response.code()).isEqualTo(200);
            String dumpAsString = response.content();
            assertThat(dumpAsString).isEqualTo("somedump");
        }

        @Test
        public void test_post_sorted_graph_dump_should_return_401_for_unauthorized_user() {
            // Given
            neo4jApp.configure(
                routes -> routes.post("/graphs/dump",
                    context -> new Payload("binary/octet-stream", "somedump".getBytes())
                )
            );
            // When
            String body = "{"
                + "\"format\": \"graphml\", "
                + "\"sort\": [{\"property\": \"path\", \"direction\": \"DESC\"}]"
                + "}";
            Response response = post("/api/neo4j/graphs/sorted-dump?project=foo-datashare",
                body).withPreemptiveAuthentication("unauthorized", "null").response();
            // Then
            assertThat(response.code()).isEqualTo(401);
        }

        @Test
        public void test_post_sorted_graph_dump_should_return_403_for_forbidden_mask() {
            // Given
            neo4jApp.configure(
                routes -> routes.post("/graphs/dump",
                    context -> new Payload("binary/octet-stream", "somedump".getBytes())
                )
            );
            // When
            when(parentRepository.getProject("foo-datashare"))
                .thenReturn(new Project("foo-datashare", "1.2.3.4"));
            String body = "{\"format\": \"graphml\"}";
            Response response = post("/api/neo4j/graphs/sorted-dump?project=foo-datashare",
                body).withPreemptiveAuthentication("foo", "null").response();
            // Then
            assertThat(response.code()).isEqualTo(403);
        }

        @Test
        public void test_check_extension_project_community() {
            // Given
            String project = "foo-datashare";
            assertThat(Neo4jResource.supportNeo4jEnterprise).isEqualTo(false);

            // When
            try {
                neo4jAppResource.checkExtensionProject(project);
            } catch (Neo4jResource.InvalidProjectError e) {
                fail("Project " + project + " is expected to be valid");
            }
        }

        @Test
        public void test_check_extension_project_community_should_throw() {
            // Given
            String project = "not-the-neo4j-project";
            assertThat(Neo4jResource.supportNeo4jEnterprise).isEqualTo(false);

            // When
            String expected = "Invalid project\n"
                + "Detail: Invalid project 'not-the-neo4j-project' extension is setup to support"
                + " project 'foo-datashare'";
            assertThat(assertThrows(Neo4jResource.InvalidProjectError.class,
                () -> neo4jAppResource.checkExtensionProject(project)
            ).getMessage()).isEqualTo(expected);
        }

        @Test
        public void test_check_project_init() {
            // Given
            String project = "foo-datashare";

            // When
            try {
                neo4jAppResource.checkProjectInitialized(project);
            } catch (Neo4jResource.ProjectNotInitialized ignored) {
                fail("expected " + project + " to be initialized");
            }
        }

        @Test
        public void test_check_project_init_should_throw() {
            // Given
            String project = "not-initialized";

            // When
            String expected = "Project Not Initialized\n"
                + "Detail: Project \"not-initialized\" as not been initialized";
            assertThat(assertThrows(Neo4jResource.ProjectNotInitialized.class,
                () -> neo4jAppResource.checkProjectInitialized(project)
            ).getMessage()).isEqualTo(expected);
        }


        @Test
        public void test_get_graph_nodes_count_should_return_200() {
            // Given
            String counts = "{\"documents\":1,\"namedEntities\":{\"EMAIL\":1}}";
            neo4jApp.configure(
                routes -> routes.get("/graphs/nodes/count",
                    context -> new Payload("application/json", counts.getBytes())
                )
            );
            // When
            Response response = get("/api/neo4j/graphs/nodes/count?project=foo-datashare"
            ).withPreemptiveAuthentication("foo", "null").response();
            // Then
            assertThat(response.code()).isEqualTo(200);
            assertThat(response.content()).isEqualTo(counts);
        }

        @Test
        public void test_get_graph_nodes_count_should_return_401_for_unauthorized_user() {
            // Given
            String counts = "{\"documents\": 1, \"namedEntities\": {\"EMAIL\": 1}}";
            neo4jApp.configure(
                routes -> routes.get("/graphs/nodes/count",
                    context -> new Payload("application/json", counts.getBytes())
                )
            );
            // When
            Response response = get("/api/neo4j/graphs/nodes/count?project=foo-datashare"
            ).withPreemptiveAuthentication("unauthorized", "null").response();
            // Then
            assertThat(response.code()).isEqualTo(401);
        }

        @Test
        public void test_get_graph_nodes_count_should_return_403_for_forbidden_mask() {
            // Given
            String counts = "{\"documents\": 1, \"namedEntities\": {\"EMAIL\": 1}}";
            neo4jApp.configure(
                routes -> routes.get("/graphs/nodes/count",
                    context -> new Payload("application/json", counts.getBytes())
                )
            );
            // When
            when(parentRepository.getProject("foo-datashare"))
                .thenReturn(new Project("foo-datashare", "1.2.3.4"));
            Response response = get("/api/neo4j/graphs/nodes/count?project=foo-datashare"
            ).withPreemptiveAuthentication("foo", "null").response();
            // Then
            assertThat(response.code()).isEqualTo(403);
        }

        @Test
        public void test_post_full_import_should_return_201() {
            // Given
            neo4jApp.configure(
                routes -> routes.post("/tasks",
                    context -> new Payload("application/json", "taskId", 201)
                )
            );
            // When
            Response response = post("/api/neo4j/full-imports?project=foo-datashare"
            ).withPreemptiveAuthentication("foo", "null").response();
            // Then
            assertThat(response.code()).isEqualTo(201);
            assertThat(response.content()).isEqualTo("taskId");
        }

        @Test
        public void test_post_full_import_should_return_401_for_unauthorized_user() {
            // Given
            neo4jApp.configure(
                routes -> routes.post("/tasks",
                    context -> new Payload("application/json", "taskId", 201)
                )
            );
            // When
            Response response = post("/api/neo4j/full-imports?project=foo-datashare"
            ).withPreemptiveAuthentication("unauthorized", "null").response();
            // Then
            assertThat(response.code()).isEqualTo(401);
        }

        @Test
        public void test_post_full_import_should_return_403_for_forbidden_mask() {
            // Given
            neo4jApp.configure(
                routes -> routes.post("/tasks",
                    context -> new Payload("application/json", "taskId", 201)
                )
            );
            when(parentRepository.getProject("foo-datashare"))
                .thenReturn(new Project("foo-datashare", "1.2.3.4"));
            // When
            Response response = post("/api/neo4j/full-imports?project=foo-datashare"
            ).withPreemptiveAuthentication("foo", "null").response();
            // Then
            assertThat(response.code()).isEqualTo(403);
        }

        @Test
        public void test_get_task_should_return_401_for_authorized_user() {
            // Only available for CLI users for now
            // Given
            Objects.Task running = new Objects.Task(
                "taskId", Objects.TaskType.FULL_IMPORT, Objects.TaskStatus.RUNNING,
                null, 50.0f, 0, Date.from(Instant.now()), null);
            neo4jApp.configure(routes -> routes.get("/tasks/:id", (id, context) -> running));
            // When
            Response response = get("/api/neo4j/tasks/taskId?project=foo-datashare"
            ).withPreemptiveAuthentication("foo", "null").response();
            // Then
            assertThat(response.code()).isEqualTo(401);
        }

        @Test
        public void test_get_task_should_return_401_for_unauthorized_user() {
            // Given
            Objects.Task running = new Objects.Task(
                "taskId", Objects.TaskType.FULL_IMPORT, Objects.TaskStatus.RUNNING,
                null, 50.0f, 0, Date.from(Instant.now()), null);
            neo4jApp.configure(routes -> routes.post("/tasks", context -> List.of(running)));
            // When
            Response response = get("/api/neo4j/tasks/taskId?project=foo-datashare"
            ).withPreemptiveAuthentication("unauthorized", "null").response();
            // Then
            assertThat(response.code()).isEqualTo(401);
        }

        @Test
        public void test_get_task_should_return_403_for_forbidden_mask() {
            // Given
            Objects.Task running = new Objects.Task(
                "taskId", Objects.TaskType.FULL_IMPORT, Objects.TaskStatus.RUNNING,
                null, 50.0f, 0, Date.from(Instant.now()), null);
            neo4jApp.configure(routes -> routes.post("/tasks", context -> List.of(running)));
            when(parentRepository.getProject("foo-datashare"))
                .thenReturn(new Project("foo-datashare", "1.2.3.4"));
            // When
            Response response = get("/api/neo4j/tasks/taskId?project=foo-datashare"
            ).withPreemptiveAuthentication("foo", "null").response();
            // Then
            assertThat(response.code()).isEqualTo(403);
        }
    }


    @DisplayName("test with mocked app and enterprise support")
    @ExtendWith(MockNeo4jApp.class)
    @ExtendWith(MockAppProperties.class)
    @ExtendWith(BindNeo4jResourceEnterprise.class)
    @Nested
    class Neo4jResourceEnterpriseTest implements FluentRestTest {
        @Override
        public int port() {
            return port;
        }

        @Test
        public void test_check_extension_project_enterprise() {
            // Given
            String project = "some-project";
            assertThat(project).isNotEqualTo(SINGLE_PROJECT);
            assertThat(Neo4jResource.supportNeo4jEnterprise).isEqualTo(true);
            neo4jAppResource.startServerProcess(false);

            // When
            try {
                neo4jAppResource.checkExtensionProject(project);
            } catch (Neo4jResource.InvalidProjectError e) {
                fail("Project " + project + " is expected to be valid");
            }
        }
    }

    @DisplayName("test admin import local")
    @ExtendWith(MockNeo4jApp.class)
    @ExtendWith(MockLocalModeProperties.class)
    @ExtendWith(BindNeo4jResourceWithPid.class)
    @Nested
    class Neo4jResourceAdminImportTest implements FluentRestTest {
        @Override
        public int port() {
            return port;
        }

        @Test
        public void test_post_admin_neo4j_csvs_should_return_200() throws IOException {
            // Given
            Path exportPath = null;
            byte[] exportContent = "exportbytescompressedintoatargz".getBytes();
            try {
                exportPath = Files.createTempFile("neo4j-export", ".tar.gz").toAbsolutePath();
                Files.write(exportPath, exportContent);
                String exportPathAsString = exportPath.toString();

                neo4jApp.configure(
                    routes -> routes.post(
                        "/admin/neo4j-csvs",
                        context -> new Payload(
                            "application/json",
                            "{"
                                + "\"path\": \"" + exportPathAsString + "\","
                                + "\"metadata\": {\"nodes\": [], \"relationships\": []}"
                                + "}"
                        )
                    )
                );

                // When
                Response response = post("/api/neo4j/admin/neo4j-csvs?project=foo-datashare", "{}")
                    .withPreemptiveAuthentication("foo", "null")
                    .response();

                // Then
                assertThat(response.code()).isEqualTo(200);
                assertThat(response.contentType()).isEqualTo("application/octet-stream");
                assertThat(response.content()).isEqualTo(new String(exportContent));
            } finally {
                if (exportPath != null) {
                    Files.deleteIfExists(exportPath);
                }
            }
        }

        @Test
        public void test_post_admin_neo4j_csvs_should_return_401_for_unauthorized_users() {
            // When
            Response response = post(
                "/api/neo4j/admin/neo4j-csvs?project=foo-datashare").withPreemptiveAuthentication(
                "unauthorized", "null").response();
            // Then
            assertThat(response.code()).isEqualTo(401);
        }
    }

    @DisplayName("test admin import embedded")
    @ExtendWith(MockNeo4jApp.class)
    @ExtendWith(MockEmbeddedModeProperties.class)
    @ExtendWith(BindNeo4jResourceWithPid.class)
    @Nested
    class Neo4jResourceAdminImportEmbeddedTest implements FluentRestTest {
        @Override
        public int port() {
            return port;
        }

        @Test
        public void test_post_admin_neo4j_csvs_should_return_200() throws IOException {
            // Given
            Path exportPath = null;
            byte[] exportContent = "exportbytescompressedintoatargz".getBytes();
            try {
                exportPath = Files.createTempFile("neo4j-export", ".tar.gz").toAbsolutePath();
                Files.write(exportPath, exportContent);
                String exportPathAsString = exportPath.toString();

                neo4jApp.configure(
                    routes -> routes.post(
                        "/admin/neo4j-csvs",
                        context -> new Payload(
                            "application/json",
                            "{"
                                + "\"path\": \"" + exportPathAsString + "\","
                                + "\"metadata\": {\"nodes\": [], \"relationships\": []}"
                                + "}"
                        )
                    )
                );

                // When
                Response response = post("/api/neo4j/admin/neo4j-csvs?project=foo-datashare", "{}")
                    .withPreemptiveAuthentication("foo", "null")
                    .response();

                // Then
                assertThat(response.code()).isEqualTo(200);
                assertThat(response.contentType()).isEqualTo("application/octet-stream");
                assertThat(response.content()).isEqualTo(new String(exportContent));
            } finally {
                if (exportPath != null) {
                    Files.deleteIfExists(exportPath);
                }
            }
        }

    }

    @DisplayName("test admin import server")
    @ExtendWith(MockNeo4jApp.class)
    @ExtendWith(MockServerModeProperties.class)
    @ExtendWith(BindNeo4jResourceWithPid.class)
    @Nested
    class Neo4jResourceAdminImportServerTest implements FluentRestTest {
        @Override
        public int port() {
            return port;
        }

        @Test
        public void test_post_full_import_should_return_403() throws IOException {
            // Given
            assert false;
        }

    }

    @DisplayName("test admin import CLI")
    @ExtendWith(MockNeo4jApp.class)
    @ExtendWith(MockCliModeProperties.class)
    @ExtendWith(BindNeo4jResourceWithPid.class)
    @Nested
    class Neo4jResourceCliTest implements FluentRestTest {
        @Override
        public int port() {
            return port;
        }

        @Test
        public void test_get_task_on_cli_mode() {
            // Given
            Objects.Task running = new Objects.Task(
                "taskId", Objects.TaskType.FULL_IMPORT, Objects.TaskStatus.RUNNING,
                null, 50.0f, 0, Date.from(Instant.now()), null);
            neo4jApp.configure(routes -> routes.get("/tasks/:id", (id, context) -> running));
            // When
            Objects.Task response = neo4jAppResource.task("taskId", "foo-datashare");
            // Then
            assertThat(response.id).isEqualTo(running.id);
        }

        @Test
        public void test_get_task_result_on_cli_mode() {
            // Given
            String result = "hello world";
            neo4jApp.configure(routes -> routes.get("/tasks/:id/result",
                (id, context) -> new Payload(
                    "application/json", MAPPER.writeValueAsString("hello world"))));
            // When
            String response = neo4jAppResource.taskResult("taskId", "foo-datashare", String.class);
            // Then
            assertThat(response).isEqualTo(result);
        }

        @Test
        public void test_get_task_errors_on_cli_mode() {
            // Given
            Objects.TaskError someError = new Objects.TaskError(
                "error-id", "Some Error", "some details", Date.from(Instant.now()));
            List<Objects.TaskError> expectedErrors = List.of(someError);
            neo4jApp.configure(
                routes -> routes.get("/tasks/:id/errors", (id, context) -> expectedErrors));
            // When
            List<Objects.TaskError> errors = neo4jAppResource.taskErrors("taskId", "foo-datashare");
            // Then
            assertThat(errors.size()).isEqualTo(1);
            assertThat(errors.get(0).id).isEqualTo(someError.id);
        }
    }

    @ExtendWith(MockNotReadyNeo4jApp.class)
    @ExtendWith(MockAppProperties.class)
    @ExtendWith(BindNeo4jResource.class)
    @DisplayName("Neo4jResource test with not ready app mock")
    @Nested
    class Neo4jResourceNotReadyAppTest implements FluentRestTest {
        @Override
        public int port() {
            return port;
        }

        @Test
        public void test_wait_for_server_to_be_up_when_ping_fail() {
            // When/Then
            assertThat(Neo4jResource.isOpen(neo4jAppResource.host, neo4jAppPort));
            assertThat(
                assertThrows(
                    RuntimeException.class,
                    () -> neo4jAppResource.waitForServerToBeUp()
                ).getMessage()
            ).startsWith("Couldn't start Python");
        }
    }

}
