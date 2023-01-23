package org.icij.datashare;


import kong.unirest.Unirest;
import kong.unirest.apache.ApacheClient;
import net.codestory.http.Context;
import net.codestory.http.annotations.Get;
import net.codestory.http.annotations.Prefix;
import net.codestory.http.payload.Payload;
import org.apache.http.impl.client.HttpClientBuilder;

import javax.inject.Inject;

@Prefix("/api/neo4j")
public class Neo4jResource {
    // TODO: add url rather than hardcode it
    private final String neo4jUrl;

    @Inject
    public Neo4jResource(PropertiesProvider propertiesProvider) {
        // TODO: check the host/port here
        neo4jUrl = propertiesProvider.get("neo4jUrl").orElse("http://neo4j-app:8080");
        Unirest.config().httpClient(ApacheClient.builder(HttpClientBuilder.create().build()));
    }

    @Get("/ping")
    public Payload getPingMethod(Context context) {
        kong.unirest.HttpResponse<byte[]> httpResponse = Unirest.get(neo4jUrl + "/ping").asBytes();
        return new Payload(httpResponse.getBody()).withCode(httpResponse.getStatus());
    }
}
