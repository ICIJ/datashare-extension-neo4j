package org.icij.datashare;

import static org.fest.assertions.Assertions.assertThat;
import static org.icij.datashare.json.JsonObjectMapper.MAPPER;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;


public class ObjectsTest {

    protected static String queryDumpFile;

    public static class TestResources implements BeforeAllCallback {
        @Override
        public void beforeAll(ExtensionContext extensionContext) {
            queryDumpFile = Neo4jUtilsTest.class.getResource("/objects/dump_query.json").getFile();
        }
    }

    @Test
    public void test_dump_query_default_query_statement() {
        // Given
        long limit = 10L;
        // When
        String defaultQuery = Objects.DumpQuery.defaultQueryStatement(limit).getCypher();
        // Then
        String expectedQuery = "MATCH (doc:`Document`) "
            + "OPTIONAL MATCH (doc)-[rel]-(other) "
            + "RETURN doc, other, rel "
            + "ORDER BY doc.path ASC "
            + "LIMIT 10";
        assertThat(defaultQuery).isEqualTo(expectedQuery);
    }

    @ExtendWith(TestResources.class)
    @DisplayName("object test using test resources")
    @Nested
    class WithTestFiles {

        @Test
        public void test_dump_query_should_deserialize()
            throws IOException {
            // When
            try (FileInputStream fileInputStream = new FileInputStream(queryDumpFile)) {

                Objects.DumpQuery query = MAPPER.readValue(
                    fileInputStream, Objects.DumpQuery.class);
                // Then
                // match
                Neo4jUtils.PatternNode node0 =
                    new Neo4jUtils.PatternNode("doc", List.of("Document", "Important"),
                        new HashMap<>() {{
                            put("created", "someDate");
                        }});
                Neo4jUtils.PatternNode node1 =
                    new Neo4jUtils.PatternNode("person", List.of("NamedEntity", "Person"), null);
                Neo4jUtils.PatternRelationship rel =
                    new Neo4jUtils.PatternRelationship(
                        "rel", Neo4jUtils.PatternRelationship.Direction.FROM,
                        List.of("APPEARS_IN"));
                List<Neo4jUtils.PatternNode> expectedNodes = List.of(node0, node1);
                List<Neo4jUtils.Match> matches = List.of(
                    new Neo4jUtils.PathPattern(expectedNodes, List.of(rel), false));

                // where
                Neo4jUtils.LiteralWrapper prefix = new Neo4jUtils.LiteralWrapper(
                    "some/path/prefix"
                );
                Neo4jUtils.StartsWith docHasPath = new Neo4jUtils.StartsWith(
                    new Neo4jUtils.VariableProperty("doc", "path"),
                    prefix
                );
                Neo4jUtils.IsEqualTo personHasDoc = new Neo4jUtils.IsEqualTo(
                    new Neo4jUtils.VariableProperty("person", "docId"),
                    new Neo4jUtils.VariableProperty("doc", "id")
                );
                Neo4jUtils.Where where = new Neo4jUtils.And(docHasPath, personHasDoc);

                // orderBy
                List<Neo4jUtils.OrderBy> orderBy = List.of(
                    new Neo4jUtils.SortByProperty(
                        new Neo4jUtils.VariableProperty("doc", "path"),
                        Objects.SortDirection.DESC
                    )
                );

                // Limit
                Long limit = 100L;

                Objects.DumpQuery expected = new Objects.DumpQuery(matches, where, orderBy, limit);
                assertThat(query).isEqualTo(expected);
            }
        }

        @Test
        public void test_dump_query_as_validated() throws IOException {
            // Given
            try (FileInputStream fileInputStream = new FileInputStream(queryDumpFile)) {

                Objects.DumpQuery query = MAPPER.readValue(
                    fileInputStream, Objects.DumpQuery.class);

                // When
                String cypher = query.asValidated().getCypher();

                // Then
                String expected =
                    "MATCH (doc:`Document`:`Important` {created: 'someDate'})<-[rel:`APPEARS_IN`]-(person:`NamedEntity`:`Person`) "
                        + "WHERE (doc.path STARTS WITH 'some/path/prefix' AND person.docId = doc.id) "
                        + "RETURN * "
                        + "ORDER BY doc.path DESC "
                        + "LIMIT 100";
                assertThat(cypher).isEqualTo(expected);
            }
        }
    }
}
