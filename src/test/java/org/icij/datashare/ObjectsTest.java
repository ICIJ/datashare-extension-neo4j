package org.icij.datashare;

import static org.fest.assertions.Assertions.assertThat;
import static org.icij.datashare.json.JsonObjectMapper.MAPPER;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;


public class ObjectsTest {

    protected static Map<String, String> TEST_FILES;

    public static class TestResources implements BeforeAllCallback {
        protected static final List<String> TEST_FILE_NAMES = List.of(
            "dump_query",
            "dump_query_empty_queries",
            "dump_query_several_queries",
            "dump_query_without_queries",
            "dump_query_without_matches"
        );

        @Override
        public void beforeAll(ExtensionContext extensionContext) {
            TEST_FILES = TEST_FILE_NAMES
                .stream()
                .collect(
                    Collectors.toMap(
                        Function.identity(),
                        name -> java.util.Objects.requireNonNull(
                                Neo4jUtilsTest.class.getResource("/objects/" + name + ".json"))
                            .getFile()
                    )
                );
        }
    }

    @Test
    public void test_dump_query_default_query_statement() {
        // Given
        long limit = 10L;
        // When
        String defaultQuery = new Objects.DumpQuery(null).asValidated(limit).getCypher();
        // Then
        String expected = "MATCH (doc:`Document`) "
            + "WITH * "
            + "ORDER BY doc.path ASC "
            + "LIMIT 10 "
            + "OPTIONAL MATCH (doc)-[rel:`APPEARS_IN`|`SENT`|`RECEIVED`]-(ne:`NamedEntity`)"
            + " RETURN apoc.coll.toSet(((collect(doc) + collect(ne)) + collect(rel)))"
            + " AS values";
        assertThat(defaultQuery).isEqualTo(expected);
    }

    @ExtendWith(TestResources.class)
    @DisplayName("object test using test resources")
    @Nested
    class WithTestFiles {

        @Test
        public void test_dump_query_as_validated() throws IOException {
            // Given
            try (FileInputStream fileInputStream = new FileInputStream(
                TEST_FILES.get("dump_query"))) {

                Objects.DumpQuery query = MAPPER.readValue(
                    fileInputStream, Objects.DumpQuery.class);

                // When
                String cypher = query.asValidated(null).getCypher();

                // Then
                String expected = "MATCH (doc:`Document`:`Important` {created: 'someDate'}) "
                    + "WHERE doc.path STARTS WITH 'some/path/prefix' "
                    + "WITH * "
                    + "OPTIONAL MATCH (doc)-[rel:`APPEARS_IN`|`SENT`|`RECEIVED`]-(ne:`NamedEntity`)"
                    + " RETURN apoc.coll.toSet(((collect(doc) + collect(ne)) + collect(rel)))"
                    + " AS values";
                assertThat(cypher).isEqualTo(expected);
            }
        }

        @Test
        public void test_dump_query_several_queries() throws IOException {
            // Given
            try (FileInputStream fileInputStream = new FileInputStream(
                TEST_FILES.get("dump_query_several_queries"))) {
                Objects.DumpQuery query = MAPPER.readValue(
                    fileInputStream, Objects.DumpQuery.class);
                String expected = "expected a single query matching documents to be specified";
                assertThat(
                    assertThrows(IllegalArgumentException.class,
                        () -> query.asValidated(null)).getMessage()
                ).isEqualTo(expected);
            }
        }

        @Test
        public void test_dump_query_without_match() throws IOException {
            // Given
            try (FileInputStream fileInputStream = new FileInputStream(
                TEST_FILES.get("dump_query_without_matches"))) {
                Objects.DumpQuery query = MAPPER.readValue(
                    fileInputStream, Objects.DumpQuery.class);
                // When
                String cypher = query.asValidated(null).getCypher();

                // Then
                String expected = "MATCH (doc:`Document`) "
                    + "WITH * "
                    + "ORDER BY doc.path ASC "
                    + "OPTIONAL MATCH (doc)-[rel:`APPEARS_IN`|`SENT`|`RECEIVED`]-(ne:`NamedEntity`)"
                    + " RETURN apoc.coll.toSet(((collect(doc) + collect(ne)) + collect(rel)))"
                    + " AS values";
                assertThat(cypher).isEqualTo(expected);
            }
        }

        @Test
        public void test_dump_query_empty_queries() throws IOException {
            // Given
            try (FileInputStream fileInputStream = new FileInputStream(
                TEST_FILES.get("dump_query_empty_queries"))) {
                Objects.DumpQuery query = MAPPER.readValue(
                    fileInputStream, Objects.DumpQuery.class);
                // When
                String cypher = query.asValidated(null).getCypher();

                // Then
                String expected = "MATCH (doc:`Document`) "
                    + "WITH * "
                    + "ORDER BY doc.path ASC "
                    + "OPTIONAL MATCH (doc)-[rel:`APPEARS_IN`|`SENT`|`RECEIVED`]-(ne:`NamedEntity`)"
                    + " RETURN apoc.coll.toSet(((collect(doc) + collect(ne)) + collect(rel)))"
                    + " AS values";
                assertThat(cypher).isEqualTo(expected);
            }
        }

        @Test
        public void test_without_dump_queries() throws IOException {
            // Given
            try (FileInputStream fileInputStream = new FileInputStream(
                TEST_FILES.get("dump_query_without_queries"))) {
                Objects.DumpQuery query = MAPPER.readValue(
                    fileInputStream, Objects.DumpQuery.class);
                // When
                String cypher = query.asValidated(null).getCypher();

                // Then
                String expected = "MATCH (doc:`Document`) "
                    + "WITH * "
                    + "ORDER BY doc.path ASC "
                    + "OPTIONAL MATCH (doc)-[rel:`APPEARS_IN`|`SENT`|`RECEIVED`]-(ne:`NamedEntity`)"
                    + " RETURN apoc.coll.toSet(((collect(doc) + collect(ne)) + collect(rel)))"
                    + " AS values";
                assertThat(cypher).isEqualTo(expected);
            }
        }
    }
}
