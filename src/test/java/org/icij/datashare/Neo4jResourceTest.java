package org.icij.datashare;

import net.codestory.http.filters.basic.BasicAuthFilter;
import net.codestory.rest.RestAssert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.HashMap;

import static org.fest.assertions.Assertions.assertThat;
import static org.icij.datashare.TestUtils.assertJson;

public class Neo4jResourceTest extends AbstractProdWebServerTest {
    @ClassRule
    public static ProdWebServerRule neo4jAppMock = new ProdWebServerRule();

    public Neo4jResource neo4jApp;

    @BeforeClass
    public static void setUpNeo4j() {
        neo4jAppMock.configure(routes -> routes.get("/ping", (context) -> new HashMap<String, String>() {{
            put("Method", "Get");
            put("Neo4jUrl", "/ping");
        }}));
    }

    @Before
    public void setUp() {
        this.neo4jApp = new Neo4jResource(new PropertiesProvider(new HashMap<>() {{
            put("neo4jAppPort", Integer.toString(neo4jAppMock.port()));
        }}));
        configure(routes -> routes.add(this.neo4jApp).filter(new BasicAuthFilter("/api", "ds", DatashareUser.singleUser("foo"))));
    }


    @Test
    public void test_not_be_running_by_default() {
        Neo4jResource.Neo4jAppStatus status = this.neo4jApp.getStopNeo4jApp();
        assertThat(status.isRunning).isFalse();
    }


    // TODO: test auth

    @Test
    public void test_get_ping() {
        get("/api/neo4j/ping").withPreemptiveAuthentication("foo", "null")
                .should()
                .respond(200)
                .contain("\"Method\":\"Get\"")
                .contain("\"Neo4jUrl\":\"/ping\"");
    }

    @Test
    public void test_get_status() {
        RestAssert assertion = get("/api/neo4j/status").withPreemptiveAuthentication("foo", "null");
        assertion.should().respond(200);
        assertJson(assertion.response().content(), Neo4jResource.Neo4jAppStatus.class, status -> assertThat(status.isRunning).isFalse());
    }

    @Test
    public void test_unknown_url() {
        get("/api/neo4j/unknown-url").withPreemptiveAuthentication("foo", "null").should().respond(404);
    }

}
