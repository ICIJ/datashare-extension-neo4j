package org.icij.datashare;


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import kong.unirest.Unirest;
import kong.unirest.apache.ApacheClient;
import net.codestory.http.annotations.Get;
import net.codestory.http.annotations.Post;
import net.codestory.http.annotations.Prefix;
import net.codestory.http.payload.Payload;
import org.apache.http.impl.client.HttpClientBuilder;

import javax.inject.Inject;
import java.io.IOException;
import java.net.Socket;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

@Prefix("/api/neo4j")
public class Neo4jResource {
    // TODO: add some logging ?
    // TODO: can we do better than harding this name ?
    private static final String NEO4J_APP_BIN = "neo4j_app";
    private final PropertiesProvider propertiesProvider;
    private final int port;
    private final String host = "127.0.0.1";
    private volatile Process serverProcess;

    @Inject
    public Neo4jResource(PropertiesProvider propertiesProvider) {
        this.propertiesProvider = propertiesProvider;
        this.port = Integer.parseInt(propertiesProvider.get("neo4jAppPort").orElse("8080"));
        Unirest.config().httpClient(ApacheClient.builder(HttpClientBuilder.create().build()));
    }

    private static void waitForServerToBeUp(String host, int port) throws InterruptedException {
        for (int nbTries = 0; nbTries < 60; nbTries++) {
            if (isOpen(host, port)) {
                return;
            } else {
                Thread.sleep(500);
            }
        }
        throw new RuntimeException("Couldn't read Python 30s after starting it !");
    }

    private static boolean isOpen(String host, int port) {
        try (Socket ignored = new Socket(host, port)) {
            return true;
        } catch (IOException ignored) {
            return false;
        }
    }

    @Post("/start")
    public ServerStartResponse postStartNeo4jApp() throws IOException, URISyntaxException, InterruptedException {
        // TODO: check that the user is allowed
        boolean alreadyRunning = this.serverProcess != null;
        if (!alreadyRunning) {
            this.startNeo4jApp();
        }
        return new ServerStartResponse(alreadyRunning);
    }

    @Post("/stop")
    public ServerStopResponse postStopNeo4jApp() {
        // TODO: check that the user is allowed
        boolean alreadyStopped = true;
        if (this.serverProcess != null) {
            alreadyStopped = false;
            stopServerProcess();
        }
        return new ServerStopResponse(alreadyStopped);
    }

    @Get("/status")
    public Neo4jAppStatus getStopNeo4jApp() {
        // TODO: check that the user is allowed
        return new Neo4jAppStatus(this.serverProcess != null);
    }

    @Get("/ping")
    public Payload getPingMethod() {
        kong.unirest.HttpResponse<byte[]> httpResponse = Unirest.get(this.getNeo4jUrl("/ping")).asBytes();
        return new Payload(httpResponse.getBody()).withCode(httpResponse.getStatus());
    }

    private void startNeo4jApp() throws IOException, URISyntaxException, InterruptedException {
        if (this.serverProcess == null) {
            synchronized (this) {
                if (this.serverProcess == null) {
                    // TODO: is it the right place ???
                    // TODO: read the name from the property provider rather than harcoding
                    ProcessBuilder pb = startServerProcess();
                    // TODO: read the binary path from the config ?
                    // TODO: do some smarter thing with the Python server stdout and sterr (pb.redirectErrorStream...)
                    this.serverProcess = pb.start();
                    // TODO: smart recovery in case of failure
                    waitForServerToBeUp(this.host, this.port);
                }
            }
        }
    }

    private ProcessBuilder startServerProcess() throws IOException, URISyntaxException {
        Path propertiesPath = Paths.get(Neo4jResource.class.getResource("").getPath(), "datashare.properties");
        this.propertiesProvider.getProperties().store(Files.newOutputStream(propertiesPath), "Datashare properties");
        // TODO: do some starter thing to find the right binary name depending on the platform
        Path serverBinaryPath = Paths.get(ClassLoader.getSystemResource(NEO4J_APP_BIN).toURI());
        String[] startServerCmd = this.propertiesProvider
                // TODO: when retrieving a custom command there are chance that we run a sever
                //  specifying a port which is different than the on in the datashare properties...
                //  to use carefully (test mainly)
                .get("neo4jStartServerCmd").map(s -> s.split("\\s+")).orElse(new String[]{serverBinaryPath.toFile().getAbsolutePath(), propertiesPath.toFile().getAbsolutePath()});
        return new ProcessBuilder(startServerCmd);
    }


    private void stopServerProcess() {
        if (this.serverProcess != null) {
            synchronized (this) {
                if (this.serverProcess != null) {
                    // TODO: check if we want to force destroyForcibly
                    this.serverProcess.destroy();
                    this.serverProcess = null;
                }
            }
        }
    }


    private String getNeo4jUrl(String url) {
        return "http://" + this.host + ":" + this.port + url;
    }


    public static class ServerStopResponse {
        public final boolean alreadyStopped;

        ServerStopResponse(boolean alreadyStopped) {
            this.alreadyStopped = alreadyStopped;
        }
    }

    public static class ServerStartResponse {
        public final boolean alreadyRunning;

        ServerStartResponse(boolean alreadyRunning) {
            this.alreadyRunning = alreadyRunning;
        }
    }

    public static class Neo4jAppStatus {
        // TODO: replace this one with a date or something like this
        public final boolean isRunning;

        @JsonCreator
        Neo4jAppStatus(@JsonProperty("isRunning") boolean isRunning) {
            this.isRunning = isRunning;
        }
    }

}
