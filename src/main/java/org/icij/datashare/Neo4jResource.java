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
import java.io.File;
import java.io.IOException;
import java.lang.ref.Cleaner;
import java.lang.reflect.Array;
import java.net.Socket;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

@Prefix("/api/neo4j")
public class Neo4jResource implements AutoCloseable {
    // TODO: add some logging ?
    // TODO: can we do better than harding this name ?
    private static final String NEO4J_APP_BIN = "neo4j_app";
    private final PropertiesProvider propertiesProvider;
    private final int port;
    private final String host = "127.0.0.1";
    protected volatile Process serverProcess;

    // TODO: not sure that we want to delete the process when this deleted...
    // Cleaner which will ensure the Python server will be close when the resource is deleted
    private final Cleaner cleaner;
    private Cleaner.Cleanable cleanable;

    static class CleaningState implements Runnable {
        private final Neo4jResource resource;

        CleaningState(Neo4jResource resource) {
            this.resource = resource;
        }

        public void run() {
            try {
                this.resource.startServerProcess();
            } catch (IOException | URISyntaxException e) {
                throw new RuntimeException(e);
            }
        }
    }


    protected static class ServerStopResponse {
        public final boolean alreadyStopped;

        @JsonCreator
        ServerStopResponse(@JsonProperty("alreadyStopped") boolean alreadyStopped) {
            this.alreadyStopped = alreadyStopped;
        }
    }

    protected static class ServerStartResponse {
        public final boolean alreadyRunning;

        @JsonCreator
        ServerStartResponse(@JsonProperty("alreadyRunning") boolean alreadyRunning) {
            this.alreadyRunning = alreadyRunning;
        }
    }

    // TODO: move this elsewhere
    protected static class Neo4jAppStatus {
        // TODO: replace this one with a date or something like this
        public final boolean isRunning;

        @JsonCreator
        Neo4jAppStatus(@JsonProperty("isRunning") boolean isRunning) {
            this.isRunning = isRunning;
        }
    }

    static class Neo4jNotRunningError extends RuntimeException {
        public Neo4jNotRunningError() {
            super("Neo4j Python app is not running, please start it before calling the extension");
        }

        public HttpUtils.HttpError toJsonError() {
            return new HttpUtils.HttpError().withDetail(this.getMessage());
        }
    }

    static class Neo4jAlreadyRunningError extends RuntimeException {
        public Neo4jAlreadyRunningError() {
            super("Neo4j Python is already running in likely in another phantom process");
        }

        public HttpUtils.HttpError toJsonError() {
            return new HttpUtils.HttpError().withDetail(this.getMessage());
        }
    }

    static class InvalidNeo4jCommandError extends RuntimeException {
        public InvalidNeo4jCommandError(String emptyNeo4jServerCommand) {
            super(emptyNeo4jServerCommand);
        }
    }


    @Inject
    public Neo4jResource(PropertiesProvider propertiesProvider, Cleaner cleaner) {
        this.propertiesProvider = propertiesProvider;
        this.port = Integer.parseInt(propertiesProvider.get("neo4jAppPort").orElse("8080"));
        this.cleaner = cleaner;
        Unirest.config().httpClient(ApacheClient.builder(HttpClientBuilder.create().build()));
    }

    protected void waitForServerToBeUp() throws InterruptedException {
        for (int nbTries = 0; nbTries < 60; nbTries++) {
            if (isOpen(this.host, this.port)) {
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
    public Payload postStartNeo4jApp() throws IOException, InterruptedException, URISyntaxException {
        // TODO: check that the user is allowed
        boolean alreadyRunning = this.serverProcess != null;
        if (!alreadyRunning) {
            try {
                this.startNeo4jApp();
            } catch (Neo4jAlreadyRunningError e) {
                return new Payload("application/problem+json", e.toJsonError()).withCode(500);
            }
        }
        return new Payload(new ServerStartResponse(alreadyRunning));
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
        return new Neo4jAppStatus(this.isNeoAppRunning());
    }

    @Get("/ping")
    public Payload getPingMethod() {
        try {
            checkNeo4jAppStarted();
        } catch (Neo4jNotRunningError e) {
            return new Payload("application/problem+json", e.toJsonError()).withCode(503);
        }
        kong.unirest.HttpResponse<byte[]> httpResponse = Unirest.get(this.getNeo4jUrl("/ping")).asBytes();
        return new Payload(httpResponse.getBody()).withCode(httpResponse.getStatus());

    }

    protected boolean isNeoAppRunning() {
        return this.serverProcess != null;
    }

    private void checkNeo4jAppStarted() {
        if (!this.isNeoAppRunning()) {
            throw new Neo4jNotRunningError();
        }
    }

    private void startNeo4jApp() throws IOException, InterruptedException, URISyntaxException {
        if (!this.isNeoAppRunning()) {
            synchronized (this) {
                if (!this.isNeoAppRunning()) {
                    if (isOpen(host, port)) {
                        throw new Neo4jAlreadyRunningError();
                    }
                    this.startServerProcess();
                    // TODO: smart recovery in case of failure
                    this.waitForServerToBeUp();
                }
            }
        }
    }

    protected void startServerProcess() throws IOException, URISyntaxException {
        // TODO: should I handle the potential nullpointer exception thrown by getPath
        Path propertiesPath = Paths.get(Neo4jResource.class.getResource("").getPath(), "datashare.properties");
        this.propertiesProvider.getProperties().store(Files.newOutputStream(propertiesPath), "Datashare properties");
        Path serverBinaryPath = Paths.get(ClassLoader.getSystemResource(NEO4J_APP_BIN).toURI());

        String[] startServerCmd = this.propertiesProvider
                // TODO: this might allow to run arbitrary code...
                .get("neo4jStartServerCmd")
                .map(s -> s.split("\\s+"))
                // TODO: fix this mess when we can afford calling getServerBinaryPath -> replace with orElse
                .orElse(new String[]{serverBinaryPath.toAbsolutePath().toString(), propertiesPath.toAbsolutePath().toString()});

        checkServerCommand(startServerCmd);

        this.serverProcess = new ProcessBuilder(startServerCmd).start();
        this.cleanable = cleaner.register(this, new CleaningState(this));
    }

    protected void checkServerCommand(String[] startServerCmd) {
        String mainCommand = (String) Optional.ofNullable(Array.get(startServerCmd, 0)).orElseThrow(
                () -> new InvalidNeo4jCommandError("Empty neo4j server command")
        );
        File maybeFile = new File(mainCommand);
        if (!maybeFile.isFile() || !maybeFile.canExecute()) {
            String msg = mainCommand + " does not seem to be an executable file found on the filesystem";
            throw new InvalidNeo4jCommandError(msg);
        }
    }

    protected void stopServerProcess() {
        if (isNeoAppRunning()) {
            synchronized (this) {
                if (isNeoAppRunning()) {
                    // TODO: check if we want to force destroyForcibly
                    this.serverProcess.toHandle().descendants().forEach(ProcessHandle::destroy);
                    this.serverProcess.destroy();
                    this.serverProcess = null;
                }
            }
        }
    }

    private String getNeo4jUrl(String url) {
        return "http://" + this.host + ":" + this.port + url;
    }

    @Override
    public void close() {
        this.cleanable.clean();
    }

}
