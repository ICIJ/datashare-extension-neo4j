package org.icij.datashare;

import static org.fest.assertions.Assertions.assertThat;
import static org.icij.datashare.Objects.FullImportResponse;
import static org.icij.datashare.Objects.IncrementalImportResponse;
import static org.icij.datashare.Objects.Task;
import static org.icij.datashare.Objects.TaskStatus;
import static org.icij.datashare.Objects.TaskType;
import static org.icij.datashare.json.JsonObjectMapper.MAPPER;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.inject.Inject;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.time.Instant;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import net.codestory.http.Configuration;
import net.codestory.http.Context;
import net.codestory.http.payload.Payload;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Neo4jCliExtensionTest {
    private static final String SINGLE_PROJECT = "foo-datashare";

    protected static Neo4jCliExtension extension;

    private static int neo4jAppPort;

    private static ProdWebServerRuleExtension neo4jApp;

    private static final Logger logger = LoggerFactory.getLogger(Neo4jResource.class);

    protected static FullImportResponse fullImportResponse = new FullImportResponse(
        new IncrementalImportResponse(1, 1, 0),
        new IncrementalImportResponse(1, 1, 1)
    );


    protected static Properties asProperties(OptionSet options, String prefix) {
        Properties properties = new Properties();
        for (Map.Entry<OptionSpec<?>, List<?>> entry : options.asMap().entrySet()) {
            OptionSpec<?> spec = entry.getKey();
            if (options.has(spec) || !entry.getValue().isEmpty()) {
                properties.setProperty(
                    asPropertyKey(prefix, spec),
                    asPropertyValue(entry.getValue()));
            }
        }
        return properties;
    }

    private static String asPropertyKey(String prefix, OptionSpec<?> spec) {
        List<String> flags = spec.options();
        for (String flag : flags) {
            if (1 < flag.length()) {
                return null == prefix ? flag : (prefix + '.' + flag);
            }
        }
        throw new IllegalArgumentException("No usable non-short flag: " + flags);
    }

    private static String asPropertyValue(List<?> values) {
        String stringValue = !values.isEmpty() ? String.valueOf(values.get(values.size() - 1)) : "";
        return stringValue.isEmpty() ? "true" : stringValue;
    }


    static class Neo4jResourceWithApp extends Neo4jResource {

        @Inject
        protected Neo4jResourceWithApp(Repository repository,
                                       PropertiesProvider propertiesProvider) {
            super(repository, propertiesProvider);
        }

        @Override
        protected boolean pingSuccessful() {
            return true;
        }

        @Override
        protected void checkNeo4jAppStarted() {
            logger.debug(
                Neo4jResourceTest.Neo4jResourceWithApp.class.getName() + " is always running");
        }

    }


    private static class Neo4jAppMock extends ProdWebServerRuleExtension
        implements BeforeAllCallback, AfterAllCallback, AfterEachCallback {
        @Override
        public void beforeAll(ExtensionContext extensionContext) {
            neo4jAppPort = this.port();
            neo4jApp = this;
            this.configure(routes -> routes.get("/ping", "pong"));
            Neo4jResourceWithApp.projects.add(SINGLE_PROJECT);
            Neo4jResourceWithApp.supportNeo4jEnterprise = false;

            PropertiesProvider propertiesProvider = new PropertiesProvider(new HashMap<>() {
                {
                    put("neo4jAppPort", Integer.toString(neo4jAppPort));
                    put("neo4jSingleProject", SINGLE_PROJECT);
                    put("neo4jStartServerCmd", "src/test/resources/shell_mock");
                    put("neo4jAppStartTimeoutS", "2");
                }
            });
            Repository repository = mock(Repository.class);
            Neo4jResource resources = new Neo4jResourceWithApp(repository, propertiesProvider);
            extension = new Neo4jCliExtension(propertiesProvider, resources);
        }

        @Override
        public void afterAll(ExtensionContext extensionContext) {
            Neo4jResourceWithApp.projects.remove(SINGLE_PROJECT);
            Neo4jResourceWithApp.supportNeo4jEnterprise = null;
        }

        @Override
        public void afterEach(ExtensionContext extensionContext) {
            this.configure(Configuration.NO_ROUTE);
        }
    }


    @ExtendWith(Neo4jAppMock.class)
    @DisplayName("Neo4jCliExtension tests with HTTP server mock")
    @Nested
    class Neo4jCliExtensionWithMockNeo4jTest {
        private Payload mockCreateTask(Context context) {
            String project = context.query().get("project");
            if (!project.equals(SINGLE_PROJECT)) {
                return new Payload("application/json",
                    TestUtils.makeJsonHttpError("Not Found", "Invalid project " + project), 404);
            }
            return new Payload("text/plain", "taskId", 201);
        }

        private Payload mockGetTask(Context context, String taskId, Task task) {
            String project = context.query().get("project");
            if (!project.equals(SINGLE_PROJECT)) {
                return new Payload("application/json",
                    TestUtils.makeJsonHttpError("Not Found", "Invalid project " + project), 404);
            }
            if (!taskId.equals("taskId")) {
                return new Payload("application/json",
                    TestUtils.makeJsonHttpError("Not Found", "Unknown task " + taskId), 404);
            }
            return new Payload("application/json", task);
        }

        private Payload mockGetTaskErrors(Context context, String taskId) {
            String project = context.query().get("project");
            if (!project.equals(SINGLE_PROJECT)) {
                return new Payload("application/json",
                    TestUtils.makeJsonHttpError("Not Found", "Invalid project " + project), 404);
            }
            if (!taskId.equals("taskId")) {
                return new Payload("application/json",
                    TestUtils.makeJsonHttpError("Not Found", "Unknown task " + taskId), 404);
            }
            String errors = "["
                + "    {"
                + "        \"id\": \"errorId\","
                + "        \"title\": \"someTitle\","
                + "        \"detail\": \"some details\","
                + "        \"occurredAt\": \"2022-04-20T22:20:10.064409+00:00\""
                + "    }"
                + "]";
            return new Payload("application/json", errors);
        }

        private Payload mockGetResult(Context context, String taskId) {
            String project = context.query().get("project");
            if (!project.equals(SINGLE_PROJECT)) {
                return new Payload("application/json",
                    TestUtils.makeJsonHttpError("Not Found", "Invalid project " + project), 404);
            }
            if (!taskId.equals("taskId")) {
                return new Payload("application/json",
                    TestUtils.makeJsonHttpError("Not Found", "Unknown task " + taskId), 404);
            }
            return new Payload("application/json", fullImportResponse);
        }

        @Test
        public void test_extension_should_add_cli_options() {
            // Given
            OptionParser parser = new OptionParser();
            // When
            extension.addOptions(parser);
            OptionSet opt = parser.parse("--full-import", "--project", "some-project");
            // Then
            assertThat(opt.hasArgument("full-import"));
            assertThat(opt.hasArgument("project"));
            assertThat(opt.valueOf("project")).isEqualTo("some-project");
        }

        @Test
        public void test_extension_cli_identifier() {
            // Given
            assertThat(extension.identifier()).isEqualTo("neo4j");
        }

        @Test
        public void test_poll_task_should_handle_result() {
            // Given
            Task done = new Task(
                "taskId", TaskType.FULL_IMPORT, TaskStatus.DONE,
                null, 100.0f, 0, Date.from(Instant.now()), Date.from(Instant.now())
            );
            neo4jApp.configure(routes -> {
                routes.post("/tasks", this::mockCreateTask);
                routes.get("/tasks/:id",
                    (context, taskId) -> this.mockGetTask(context, taskId, done));
                routes.get("/tasks/:id/result", this::mockGetResult);
            });
            // When
            FullImportResponse res = extension.pollTask(
                "taskId", SINGLE_PROJECT, FullImportResponse.class);
            // Then
            assertThat(res.documents.imported).isEqualTo(fullImportResponse.documents.imported);
        }

        @Test
        public void test_poll_task_should_handle_errors() {
            // Given
            Task error = new Task(
                "taskId", TaskType.FULL_IMPORT, TaskStatus.ERROR,
                null, 50.0f, 0, Date.from(Instant.now()), Date.from(Instant.now())
            );
            neo4jApp.configure(routes -> {
                routes.post("/tasks", this::mockCreateTask);
                routes.get("/tasks/:id",
                    (context, taskId) -> this.mockGetTask(context, taskId, error));
                routes.get("/tasks/:id/errors", this::mockGetTaskErrors);
            });
            // When
            String expected = "Task(id=\"taskId\") failed with the following cause(s):"
                + "\nTitle: someTitle"
                + "\nDetail: some details";
            // Then
            assertThat(assertThrows(RuntimeException.class,
                () -> extension.pollTask("taskId", SINGLE_PROJECT, FullImportResponse.class)
            ).getMessage()).isEqualTo(expected);
        }

        @Test
        public void test_poll_task_should_handle_cancellation() {
            // Given
            Task cancelled = new Task(
                "taskId", TaskType.FULL_IMPORT, TaskStatus.CANCELLED,
                null, 50.0f, 0, Date.from(Instant.now()), Date.from(Instant.now())
            );
            neo4jApp.configure(routes -> {
                routes.post("/tasks", this::mockCreateTask);
                routes.get("/tasks/:id",
                    (context, taskId) -> this.mockGetTask(context, taskId, cancelled));
            });
            // When
            String expected = "Task(id=\"taskId\") was cancelled";
            // Then
            assertThat(assertThrows(RuntimeException.class,
                () -> extension.pollTask("taskId", SINGLE_PROJECT, FullImportResponse.class)
            ).getMessage()).isEqualTo(expected);
        }

        @Test
        public void test_extension_should_run_full_import() throws JsonProcessingException {
            // Given
            ByteArrayOutputStream captured = new ByteArrayOutputStream();
            System.setOut(new PrintStream(captured));

            OptionParser parser = new OptionParser();
            extension.addOptions(parser);
            OptionSet opts = parser.parse("--full-import", "--project", "foo-datashare");
            Properties props = asProperties(opts, null);
            Task done = new Task(
                "taskId", TaskType.FULL_IMPORT, TaskStatus.DONE,
                null, 100.0f, 0, Date.from(Instant.now()), Date.from(Instant.now())
            );
            neo4jApp.configure(routes -> {
                routes.post("/tasks", this::mockCreateTask);
                routes.get("/tasks/:id",
                    (context, taskId) -> this.mockGetTask(context, taskId, done));
                routes.get("/tasks/:id/result", this::mockGetResult);
            });
            // When
            extension.run(props);
            // Then
            String output = captured.toString().trim();
            assertThat(output).isEqualTo(MAPPER.writeValueAsString(fullImportResponse));
        }
    }
}
