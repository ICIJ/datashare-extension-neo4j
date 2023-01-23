package org.icij.datashare;

import net.codestory.http.filters.basic.BasicAuthFilter;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.HashMap;

public class Neo4jResourceTest extends AbstractProdWebServerTest {
    @ClassRule
    public static ProdWebServerRule neo4jApp = new ProdWebServerRule();

    @BeforeClass
    public static void setUpNeo4j() {
        neo4jApp.configure(routes -> routes
                .get("/ping", (context) -> new HashMap<String, String>() {{
                    put("Method", "Get");
                    put("Neo4jUrl", "/ping");
                }}));
    }

    @Before
    public void setUp() {
        Neo4jResource neo4jResource = new Neo4jResource(new PropertiesProvider(new HashMap<String, String>() {{
            put("neo4jUrl", "http://localhost:" + neo4jApp.port());
        }}));
        configure(routes -> routes.add(neo4jResource).filter(new BasicAuthFilter("/api", "ds", DatashareUser.singleUser("foo"))));
    }

    @Test
    public void test_get() {
        get("/api/neo4j/ping").withPreemptiveAuthentication("foo", "null").should().respond(200)
                .contain("\"Method\":\"Get\"").contain("\"Neo4jUrl\":\"/ping\"");
    }

    @Test
    public void test_unknown_url() {
        get("/api/neo4j/unknown-url").withPreemptiveAuthentication("foo", "null").should().respond(404);
    }

}
