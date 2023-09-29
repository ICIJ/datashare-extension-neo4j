package org.icij.datashare;

import static org.fest.assertions.Assertions.assertThat;
import static org.icij.datashare.Objects.TaskStatus.DONE;
import static org.icij.datashare.Objects.TaskType.FULL_IMPORT;
import static org.icij.datashare.json.JsonObjectMapper.MAPPER;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TimeZone;
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
            String project = context.query().get("project");
            if (project != null && project.equals("myproject")) {
                if (Objects.equals(context.request().content(), "{}")) {
                    body = "{\"imported\": 3,\"nodesCreated\": 3,\"relationshipsCreated\": 3}";
                } else {
                    body = "{\"imported\": 3,\"nodesCreated\": 1,\"relationshipsCreated\": 1}";
                }
                return new Payload("application/json", body);
            }
            return new Payload("application/json",
                TestUtils.makeJsonHttpError("Not Found", "Invalid project " + project), 404);
        }

        private Payload mockDump(Context context) throws IOException {
            String project = context.query().get("project");
            if (project != null && project.equals("myproject")) {
                org.icij.datashare.Objects.Neo4jAppDumpRequest request =
                    MAPPER.readValue(context.request().content(),
                        org.icij.datashare.Objects.Neo4jAppDumpRequest.class);
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
                TestUtils.makeJsonHttpError("Not Found", "Invalid project " + project), 404);
        }

        private Payload mockCreateFullImport(Context context) throws IOException {
            String project = context.query().get("project");
            if (!project.equals("myproject")) {
                return new Payload("application/json",
                    TestUtils.makeJsonHttpError("Not Found", "Invalid project " + project), 404);
            }
            String content = context.request().content();
            String expectedContent = "{\"taskType\":\"full_import\","
                + "\"taskId\":null,\"inputs\":null,\"createdAt\":null}";
            if (!content.equals(expectedContent)) {
                return new Payload("application/json",
                    TestUtils.makeJsonHttpError("Bad Request", "Invalid payload"), 400);
            }
            return new Payload("text/plain", "taskId", 201);
        }

        private Payload mockGetTask(Context context, String taskId) {
            String project = context.query().get("project");
            if (!project.equals("myproject")) {
                return new Payload("application/json",
                    TestUtils.makeJsonHttpError("Not Found", "Invalid project " + project), 404);
            }
            if (!taskId.equals("taskId")) {
                return new Payload("application/json",
                    TestUtils.makeJsonHttpError("Not Found", "Unknown task " + taskId), 404);
            }
            String task = "{"
                + "    \"id\": \"taskId\","
                + "    \"type\": \"full_import\","
                + "    \"inputs\": null,"
                + "    \"status\": \"DONE\","
                + "    \"progress\": \"100.0\","
                + "    \"retries\": \"1\","
                + "    \"createdAt\": \"2022-04-20T22:20:10.064409+00:00\","
                + "    \"completedAt\": \"2022-04-20T22:20:10.064409+00:00\""
                + "}";
            return new Payload("application/json", task);
        }

        private Payload mockGetTaskErrors(Context context, String taskId) {
            String project = context.query().get("project");
            if (!project.equals("myproject")) {
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
            if (!project.equals("myproject")) {
                return new Payload("application/json",
                    TestUtils.makeJsonHttpError("Not Found", "Invalid project " + project), 404);
            }
            if (!taskId.equals("taskId")) {
                return new Payload("application/json",
                    TestUtils.makeJsonHttpError("Not Found", "Unknown task " + taskId), 404);
            }
            String errors = "["
                + "    {"
                + "        \"attr\": \"someValue\""
                + "    }"
                + "]";
            return new Payload("application/json", errors);
        }


        @Test
        public void test_should_import_documents() {
            // Given
            neo4jApp.configure(routes -> routes.post("/documents", this::mockImport));
            // When
            org.icij.datashare.Objects.IncrementalImportRequest body =
                new org.icij.datashare.Objects.IncrementalImportRequest(null);
            org.icij.datashare.Objects.IncrementalImportResponse res = client.importDocuments(
                "myproject", body);
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
                client.importDocuments("myproject", body);
            // Then
            assertThat(res.imported).isEqualTo(3);
            assertThat(res.nodesCreated).isEqualTo(1);
            assertThat(res.relationshipsCreated).isEqualTo(1);
        }

        @Test
        public void test_import_documents_should_throw_for_unknown_project() {
            // Given
            neo4jApp.configure(
                routes -> routes.post("/documents", this::mockImport));
            org.icij.datashare.Objects.IncrementalImportRequest body =
                new org.icij.datashare.Objects.IncrementalImportRequest(null);
            // When/Then
            assertThat(assertThrowsExactly(
                Neo4jClient.Neo4jAppError.class,
                () -> client.importDocuments("unknown", body)
            ).getMessage()).isEqualTo("Bad Request\nDetail: Invalid project unknown");
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
                () -> client.importDocuments("myproject", null)
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
                () -> client.importDocuments("myproject", null)
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
                client.importNamedEntities("myproject", body);
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
                client.importNamedEntities("myproject", body);
            // Then
            assertThat(res.imported).isEqualTo(3);
            assertThat(res.nodesCreated).isEqualTo(1);
            assertThat(res.relationshipsCreated).isEqualTo(1);
        }

        @Test
        public void test_import_named_entities_should_throw_for_unknown_project() {
            // Given
            neo4jApp.configure(routes -> routes.post("/named-entities", this::mockImport));

            org.icij.datashare.Objects.IncrementalImportRequest body =
                new org.icij.datashare.Objects.IncrementalImportRequest(null);

            // When/Then
            assertThat(assertThrowsExactly(
                Neo4jClient.Neo4jAppError.class,
                () -> client.importNamedEntities("unknown", body)
            ).getMessage()).isEqualTo("Bad Request\nDetail: Invalid project unknown");
        }

        @Test
        public void test_dump_graph_in_cypher_shell()
            throws URISyntaxException, IOException, InterruptedException {
            // Given
            neo4jApp.configure(routes -> routes.post("/graphs/dump", this::mockDump));
            org.icij.datashare.Objects.Neo4jAppDumpRequest body =
                new org.icij.datashare.Objects.Neo4jAppDumpRequest(
                    org.icij.datashare.Objects.DumpFormat.CYPHER_SHELL, null);
            // When
            try (InputStream res = client.dumpGraph("myproject", body)) {
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
            org.icij.datashare.Objects.Neo4jAppDumpRequest body =
                new org.icij.datashare.Objects.Neo4jAppDumpRequest(
                    org.icij.datashare.Objects.DumpFormat.GRAPHML, null);
            // When
            try (InputStream res = client.dumpGraph("myproject", body)) {
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
            org.icij.datashare.Objects.Neo4jAppDumpRequest body =
                new org.icij.datashare.Objects.Neo4jAppDumpRequest(
                    org.icij.datashare.Objects.DumpFormat.CYPHER_SHELL,
                    "MATCH (something) RETURN something LIMIT 100");
            // When
            try (InputStream res = client.dumpGraph("myproject", body)) {
                String dump = new String(res.readAllBytes(), StandardCharsets.UTF_8);
                // Then
                String expectedDump = "filtered\ndump";
                assertThat(dump).isEqualTo(expectedDump);
            }
        }

        @Test
        public void test_dump_graph_should_throw_for_invalid_project() {
            // Given
            neo4jApp.configure(
                routes -> routes.post("/graphs/dump", this::mockDump));
            org.icij.datashare.Objects.Neo4jAppDumpRequest body =
                new org.icij.datashare.Objects.Neo4jAppDumpRequest(
                    org.icij.datashare.Objects.DumpFormat.CYPHER_SHELL, null);

            // When/Then
            assertThat(assertThrowsExactly(
                Neo4jClient.Neo4jAppError.class,
                () -> client.dumpGraph("unknown", body)
            ).getMessage()).isEqualTo("Bad Request\nDetail: Invalid project unknown");
        }

        @Test
        public void test_init_project() {
            // Given
            String newProject = "new-project";
            neo4jApp.configure(routes -> routes.post("/projects/init", () -> new Payload(201)));
            // When
            boolean created = client.initProject(newProject);
            // Then
            assert created;
        }

        @Test
        public void test_init_existing_project() {
            // Given
            String newProject = "existing-project";
            neo4jApp.configure(routes -> routes.post("/projects/init", () -> new Payload(200)));
            // When
            boolean created = client.initProject(newProject);
            // Then
            assert !created;
        }

        @Test
        public void test_config() {
            // Given
            neo4jApp.configure(routes -> routes.get("/config", (ctx) -> new HashMap<>()));
            // When
            HashMap<String, Object> res = client.config();
            // Then
            assertThat(res).isEqualTo(new HashMap<String, Object>());
        }

        @Test
        public void test_create_task() {
            // Given
            neo4jApp.configure(routes -> routes.post("/tasks", this::mockCreateFullImport));
            // When
            String taskId = client.fullImport("myproject");
            // Then•
            assertThat(taskId).isEqualTo("taskId");
        }

        @Test
        public void test_get_task() throws ParseException {
            // Given
            neo4jApp.configure(routes -> routes.get("/tasks/:taskId", this::mockGetTask));
            // When
            org.icij.datashare.Objects.Task task = client.task("taskId", "myproject");
            // Then
            SimpleDateFormat isoFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
            isoFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
            Date expectedDate = isoFormat.parse("2022-04-20T22:20:10.064+00:00");

            assertThat(task.id).isEqualTo("taskId");
            assertThat(task.type).isEqualTo(FULL_IMPORT);
            assertThat(task.status).isEqualTo(DONE);
            assertThat(task.inputs).isEqualTo(Map.of());
            assertThat(task.progress).isEqualTo(100.0f);
            assertThat(task.retries).isEqualTo(1);
            assertThat(task.createdAt).isEqualTo(expectedDate);
            assertThat(task.completedAt).isEqualTo(expectedDate);
        }

        @Test
        public void test_get_task_result() {
            // Given
            neo4jApp.configure(
                routes -> routes.get("/tasks/:taskId/result", this::mockGetResult)
            );
            // When
            List<Map<String, String>> result = client.taskResult(
                "taskId", "myproject", List.class);

            // Then
            assertThat(result.size()).isEqualTo(1);
            assertThat(result.get(0)).isEqualTo(Map.of("attr", "someValue"));
        }

        @Test
        public void test_get_task_error() throws ParseException, JsonProcessingException {
            // Given
            neo4jApp.configure(
                routes -> routes.get("/tasks/:taskId/errors", this::mockGetTaskErrors)
            );
            // When
            org.icij.datashare.Objects.TaskError[] errors = client.taskErrors(
                "taskId", "myproject");
            // Then
            SimpleDateFormat isoFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
            isoFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
            Date expectedDate = isoFormat.parse("2022-04-20T22:20:10.064+00:00");
            assertThat(errors.length).isEqualTo(1);
            assertThat(errors[0].id).isEqualTo("errorId");
            assertThat(errors[0].title).isEqualTo("someTitle");
            assertThat(errors[0].detail).isEqualTo("some details");
            assertThat(errors[0].occurredAt).isEqualTo(expectedDate);
        }
    }
}
