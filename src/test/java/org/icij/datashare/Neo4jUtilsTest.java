package org.icij.datashare;

import static org.fest.assertions.Assertions.assertThat;
import static org.icij.datashare.json.JsonObjectMapper.MAPPER;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.exc.ValueInstantiationException;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.neo4j.cypherdsl.core.Cypher;

public class Neo4jUtilsTest {

    protected static Map<String, String> TEST_FILES;

    public static class TestResources implements BeforeAllCallback {
        protected static final List<String> TEST_FILE_NAMES = List.of(
            "path_pattern_from",
            "path_pattern_to",
            "path_pattern_between",
            "path_pattern_without_names_and_types",
            "path_pattern_with_too_many_relationships",
            "path_pattern_with_missing_relationships",
            "query_optional_match",
            "query_missing_matches",
            "query_empty_matches",
            "where_nested_conditions",
            "where_variable_properties",
            "where_not",
            "where_starts_with",
            "where_ends_with"
        );

        @Override
        public void beforeAll(ExtensionContext extensionContext) {
            TEST_FILES = TEST_FILE_NAMES
                .stream()
                .collect(
                    Collectors.toMap(
                        Function.identity(),
                        name -> Objects.requireNonNull(
                                Neo4jUtilsTest.class.getResource("/neo4j_utils/" + name + ".json"))
                            .getFile()
                    )
                );
        }
    }

    @Test
    public void test_query_as_validated_should_set_limit() {
        // Given
        long limit = 100;
        List<Neo4jUtils.PatternNode> nodes = List.of(
            new Neo4jUtils.PatternNode("doc", List.of("Document"), Map.of()));
        Neo4jUtils.PathPattern pattern = new Neo4jUtils.PathPattern(
            nodes,
            null,
            false
        );
        List<Neo4jUtils.Match> matches = List.of(pattern);
        Neo4jUtils.Query query = new Neo4jUtils.Query(matches, null, null, null);

        // When
        String validatedWithLimit = query.asValidated(limit).getCypher();

        // Then
        assertThat(validatedWithLimit).endsWith("LIMIT 100");
    }

    @Test
    public void test_dump_query_as_validated_should_override_limit() {
        // Given
        long limit = 1000;
        long defaultLimit = 100;
        List<Neo4jUtils.PatternNode> nodes = List.of(
            new Neo4jUtils.PatternNode("doc", List.of("Document"), Map.of()));
        Neo4jUtils.PathPattern pattern = new Neo4jUtils.PathPattern(
            nodes,
            null,
            false
        );
        List<Neo4jUtils.Match> matches = List.of(pattern);
        Neo4jUtils.Query query = new Neo4jUtils.Query(matches, null, null, limit);

        // When
        String validatedWithLimit = query.asValidated(defaultLimit).getCypher();

        // Then
        assertThat(validatedWithLimit).endsWith("LIMIT 100");
    }

    @ExtendWith(TestResources.class)
    @DisplayName("object test using test resources")
    @Nested
    class WithTestFiles {
        @Test
        public void test_path_pattern_from_into_match() throws IOException {
            // Given
            String cypher;
            try (FileInputStream stream = new FileInputStream(
                TEST_FILES.get("path_pattern_from"))) {
                Neo4jUtils.PathPattern pattern = MAPPER.readValue(
                    stream, Neo4jUtils.PathPattern.class);
                // When
                cypher = Cypher.match(pattern.into())
                    .returning(Cypher.asterisk())
                    .build()
                    .getCypher();
            }
            // Then
            String expected = "MATCH (doc)<-[rel:`APPEARS_IN`]-(person) RETURN *";
            assertThat(cypher).isEqualTo(expected);
        }

        @Test
        public void test_path_pattern_to_into_match() throws IOException {
            // Given
            String cypher;
            try (FileInputStream stream = new FileInputStream(
                TEST_FILES.get("path_pattern_to"))) {
                Neo4jUtils.PathPattern pattern = MAPPER.readValue(
                    stream, Neo4jUtils.PathPattern.class);
                // When
                cypher = Cypher.match(pattern.into())
                    .returning(Cypher.asterisk())
                    .build()
                    .getCypher();
            }
            // Then
            String expected = "MATCH (person)-[rel:`APPEARS_IN`]->(doc) RETURN *";
            assertThat(cypher).isEqualTo(expected);
        }

        @Test
        public void test_path_pattern_between_into_match() throws IOException {
            // Given
            String cypher;
            try (FileInputStream stream = new FileInputStream(
                TEST_FILES.get("path_pattern_between"))) {
                Neo4jUtils.PathPattern pattern = MAPPER.readValue(
                    stream, Neo4jUtils.PathPattern.class);
                // When
                cypher = Cypher.match(pattern.into())
                    .returning(Cypher.asterisk())
                    .build()
                    .getCypher();
            }
            // Then
            String expected = "MATCH (person)-[rel:`LINKED`]-(doc) RETURN *";
            assertThat(cypher).isEqualTo(expected);
        }

        @Test
        public void test_path_pattern_path_pattern_without_names_and_types_into_match()
            throws IOException {
            // Given
            String cypher;
            try (FileInputStream stream = new FileInputStream(
                TEST_FILES.get("path_pattern_without_names_and_types"))) {
                Neo4jUtils.PathPattern pattern = MAPPER.readValue(
                    stream, Neo4jUtils.PathPattern.class);
                // When
                cypher = Cypher.match(pattern.into())
                    .returning(Cypher.asterisk())
                    .build()
                    .getCypher();
            }
            // Then
            String expected = "MATCH ()-->() RETURN *";
            assertThat(cypher).isEqualTo(expected);
        }

        @Test
        public void test_path_pattern_with_too_many_relationships() throws IOException {
            // Given
            try (FileInputStream stream = new FileInputStream(
                TEST_FILES.get("path_pattern_with_too_many_relationships"))) {
                // When/Then
                String expected = "Invalid number of nodes and relationships, "
                    + "found 2 nodes and 2 relationships";
                assertThat(
                    assertThrows(ValueInstantiationException.class, () -> MAPPER.readValue(
                        stream, Neo4jUtils.PathPattern.class)).getMessage()
                ).contains(expected);
            }
        }

        @Test
        public void test_path_pattern_missing_relationships() throws IOException {
            // Given
            try (FileInputStream stream = new FileInputStream(
                TEST_FILES.get("path_pattern_with_missing_relationships"))) {
                // When/Then
                String expected = "Invalid number of nodes and relationships, "
                    + "found 2 nodes and 0 relationships";
                assertThat(
                    assertThrows(ValueInstantiationException.class, () -> MAPPER.readValue(
                        stream, Neo4jUtils.PathPattern.class)).getMessage()
                ).contains(expected);
            }
        }

        @Test
        public void test_query_optional_match() throws IOException {
            // Given
            String cypher;
            try (FileInputStream stream = new FileInputStream(
                TEST_FILES.get("query_optional_match"))) {
                Neo4jUtils.Query query = MAPPER.readValue(stream, Neo4jUtils.Query.class);
                // When
                cypher = query.asValidated().getCypher();
            }
            // Then
            String expected = "OPTIONAL MATCH (person:`Person`) RETURN *";
            assertThat(cypher).isEqualTo(expected);
        }


        @Test
        public void test_query_should_throw_for_missing_matches() throws IOException {
            // Given
            try (FileInputStream stream = new FileInputStream(
                TEST_FILES.get("query_missing_matches"))) {
                // When/Then
                String expected = "missing matches";
                assertThat(
                    assertThrows(ValueInstantiationException.class, () -> MAPPER.readValue(
                        stream, Neo4jUtils.Query.class)).getMessage()
                ).contains(expected);
            }
        }

        @Test
        public void test_query_should_throw_for_empty_matches() throws IOException {
            // Given
            try (FileInputStream stream = new FileInputStream(
                TEST_FILES.get("query_empty_matches"))) {
                // When/Then
                String expected = "empty matches";
                assertThat(
                    assertThrows(ValueInstantiationException.class, () -> MAPPER.readValue(
                        stream, Neo4jUtils.Query.class)).getMessage()
                ).contains(expected);
            }
        }

        @Test
        public void test_nested_conditions_where() throws IOException {
            // Given
            String cypher;
            try (FileInputStream stream = new FileInputStream(
                TEST_FILES.get("where_nested_conditions"))) {
                Neo4jUtils.Where where = MAPPER.readValue(stream, Neo4jUtils.Where.class);
                // When
                cypher = Cypher.match(Cypher.node("Document").named("doc"))
                    .where(where.into())
                    .returning(Cypher.asterisk())
                    .build().getCypher();
            }
            // Then
            String expected = "MATCH (doc:`Document`) "
                + "WHERE (doc.path = 'some_path' AND "
                + "(doc.id = 'some_id' "
                + "OR doc.name = 'some_name' "
                + "OR doc.name STARTS WITH 'some_prefix')) "
                + "RETURN *";
            assertThat(cypher).isEqualTo(expected);
        }

        @Test
        public void test_where_variable_properties() throws IOException {
            // Given
            String cypher;
            try (FileInputStream stream = new FileInputStream(
                TEST_FILES.get("where_variable_properties"))) {
                Neo4jUtils.Where where = MAPPER.readValue(stream, Neo4jUtils.Where.class);
                // When
                cypher = Cypher.match(Cypher.node("Document").named("doc"))
                    .match(Cypher.node("Person").named("person"))
                    .where(where.into())
                    .returning(Cypher.asterisk())
                    .build().getCypher();
            }
            // Then
            String expected = "MATCH (doc:`Document`) "
                + "MATCH (person:`Person`) "
                + "WHERE person.docId = doc.id "
                + "RETURN *";
            assertThat(cypher).isEqualTo(expected);
        }

        @Test
        public void test_where_not() throws IOException {
            // Given
            String cypher;
            try (FileInputStream stream = new FileInputStream(
                TEST_FILES.get("where_not"))) {
                Neo4jUtils.Where where = MAPPER.readValue(stream, Neo4jUtils.Where.class);
                // When
                cypher = Cypher.match(Cypher.node("Document").named("doc"))
                    .where(where.into())
                    .returning(Cypher.asterisk())
                    .build().getCypher();
            }
            // Then
            String expected = "MATCH (doc:`Document`) WHERE NOT (doc.id = 'someId') RETURN *";
            assertThat(cypher).isEqualTo(expected);
        }

        @Test
        public void test_starts_with() throws IOException {
            // Given
            String cypher;
            try (FileInputStream stream = new FileInputStream(
                TEST_FILES.get("where_starts_with"))) {
                Neo4jUtils.Where where = MAPPER.readValue(stream, Neo4jUtils.Where.class);
                // When
                cypher = Cypher.match(Cypher.node("Document").named("doc"))
                    .where(where.into())
                    .returning(Cypher.asterisk())
                    .build().getCypher();
            }
            // Then
            String expected =
                "MATCH (doc:`Document`) WHERE doc.id STARTS WITH 'somePrefix' RETURN *";
            assertThat(cypher).isEqualTo(expected);
        }

        @Test
        public void test_ends_with() throws IOException {
            // Given
            String cypher;
            try (FileInputStream stream = new FileInputStream(
                TEST_FILES.get("where_ends_with"))) {
                Neo4jUtils.Where where = MAPPER.readValue(stream, Neo4jUtils.Where.class);
                // When
                cypher = Cypher.match(Cypher.node("Document").named("doc"))
                    .where(where.into())
                    .returning(Cypher.asterisk())
                    .build().getCypher();
            }
            // Then
            String expected =
                "MATCH (doc:`Document`) WHERE doc.id ENDS WITH 'someSuffix' RETURN *";
            assertThat(cypher).isEqualTo(expected);
        }
    }

}
