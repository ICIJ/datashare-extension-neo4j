package org.icij.datashare;


import static java.io.File.createTempFile;
import static org.icij.datashare.LoggingUtils.lazy;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.ref.Cleaner;
import java.lang.reflect.Array;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import javax.inject.Inject;
import javax.inject.Provider;
import kong.unirest.Unirest;
import kong.unirest.apache.ApacheClient;
import net.codestory.http.Context;
import net.codestory.http.annotations.Get;
import net.codestory.http.annotations.Post;
import net.codestory.http.annotations.Prefix;
import net.codestory.http.errors.UnauthorizedException;
import net.codestory.http.payload.Payload;
import org.apache.http.impl.client.HttpClientBuilder;
import org.icij.datashare.user.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Prefix("/api/neo4j")
public class Neo4jResource implements AutoCloseable {
    private static final String NEO4J_APP_BIN = "neo4j-app";
    private static final String SYSLOG_SPLIT_CHAR = "@";

    // All these properties have to start with "neo4j" in order to be properly filtered
    private static final HashMap<String, String> DEFAULT_NEO4J_PROPERTIES = new HashMap<>() {
        {
            put("neo4jAppPort", "8008");
            put("neo4jHost", "neo4j");
            put("neo4jImportDir", "/home/dev/.neo4j/import");
            put("neo4jImportPrefix", "/.neo4j/import");
            put("neo4jPort", "7687");
            put("neo4jProject", "local-datashare");
            put("neo4jAppSyslogFacility", "LOCAL7");
        }
    };
    private static final Logger logger = LoggerFactory.getLogger(Neo4jResource.class);
    protected static volatile Process serverProcess;
    // TODO: not sure that we want to delete the process when this deleted...
    // Cleaner which will ensure the Python server will be close when the resource is deleted
    private static Cleaner cleaner;
    protected final Neo4jClient client;
    private final PropertiesProvider propertiesProvider;
    private final int port;
    private final String host = "127.0.0.1";
    private final String projectId;
    protected Path appBinaryPath;
    private Cleaner.Cleanable cleanPythonProcess;


    @Inject
    public Neo4jResource(PropertiesProvider propertiesProvider) {
        this.propertiesProvider = propertiesProvider;
        Properties neo4jDefaultProps = new Properties();
        neo4jDefaultProps.putAll(DEFAULT_NEO4J_PROPERTIES);
        this.propertiesProvider.mergeWith(neo4jDefaultProps);
        this.port = Integer.parseInt(propertiesProvider.get("neo4jAppPort").orElse("8080"));
        logger.info("Loading the neo4j extension which will run on port " + this.port);
        this.client = new Neo4jClient(this.port);
        if (cleaner == null) {
            cleaner = Cleaner.create();
        }
        // TODO: for now we support a single project for the extension, we'll figure out later
        //  how to support multiple ones
        projectId = propertiesProvider
            .get("neo4jProject")
            .orElseThrow(
                (() -> new IllegalArgumentException("neo4jProject is missing from properties")));
        Unirest.config().httpClient(ApacheClient.builder(HttpClientBuilder.create().build()));
    }

    private static boolean isOpen(String host, int port) {
        try (Socket ignored = new Socket(host, port)) {
            return true;
        } catch (IOException ignored) {
            return false;
        }
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

    @Post("/start")
    public Payload postStartNeo4jApp() throws IOException, InterruptedException {
        // TODO: check that the user is allowed
        boolean alreadyRunning;
        try {
            alreadyRunning = this.startNeo4jApp();
        } catch (Neo4jAlreadyRunningError e) {
            return new Payload("application/problem+json", e).withCode(500);
        }
        return new Payload(new ServerStartResponse(alreadyRunning));
    }

    @Post("/stop")
    public ServerStopResponse postStopNeo4jApp() {
        // TODO: check that the user is allowed
        return new ServerStopResponse(stopServerProcess());
    }

    @Get("/status")
    public Neo4jAppStatus getStopNeo4jApp() {
        // TODO: check that the user is allowed
        return new Neo4jAppStatus(this.isNeoAppRunning());
    }

    @Post("/documents?project=:project")
    public Payload postDocuments(String project, Context context) throws IOException {
        checkAccess(project, context);
        // TODO: this should throw a bad request when not parsed correcly...
        org.icij.datashare.Objects.IncrementalImportRequest incrementalImportRequest =
            context.extract(org.icij.datashare.Objects.IncrementalImportRequest.class);
        return doImport(() -> this.importDocuments(project, incrementalImportRequest));
    }

    @Post("/named-entities?project=:project")
    public Payload postNamedEntities(String project, Context context) throws IOException {
        checkAccess(project, context);
        // TODO: this should throw a bad request when not parsed correcly...
        org.icij.datashare.Objects.IncrementalImportRequest incrementalImportRequest =
            context.extract(org.icij.datashare.Objects.IncrementalImportRequest.class);
        return doImport(() -> this.importNamedEntities(project, incrementalImportRequest));
    }

    @Get("/ping")
    public Payload getPingMethod() {
        try {
            return new Payload(this.ping());
        } catch (Neo4jNotRunningError e) {
            return new Payload("application/problem+json", e).withCode(503);
        }
    }

    private String ping() {
        checkNeo4jAppStarted();
        return client.ping();
    }

    protected boolean isNeoAppRunning() {
        return serverProcess != null;
    }

    private void checkNeo4jAppStarted() {
        if (!this.isNeoAppRunning()) {
            throw new Neo4jNotRunningError();
        }
    }

    private boolean startNeo4jApp() throws IOException, InterruptedException {
        boolean alreadyRunning = this.isNeoAppRunning();
        if (!alreadyRunning) {
            synchronized (this) {
                if (!this.isNeoAppRunning()) {
                    if (isOpen(host, port)) {
                        throw new Neo4jAlreadyRunningError();
                    }
                    // TODO: since the process is static, if we instance the extension with a
                    //  different config, the new process with an updated config won't launch...
                    //  (might be acceptable)
                    this.startServerProcess();
                }
            }
        }
        return alreadyRunning;
    }

    protected void startServerProcess() throws IOException, InterruptedException {
        File propertiesFile = createTempFile("neo4j-", "-datashare.properties");
        logger.debug("Copying Datashare properties to temporary location {}",
            lazy(propertiesFile::getAbsolutePath));

        this.propertiesProvider
            .getProperties()
            .store(Files.newOutputStream(propertiesFile.toPath().toAbsolutePath()), null);

        String[] startServerCmd;
        Optional<String> propertiesCmd = this.propertiesProvider
            // TODO: this might allow to run arbitrary code...
            .get("neo4jStartServerCmd");
        if (propertiesCmd.isPresent()) {
            startServerCmd = propertiesCmd.get().split("\\s+");
        } else {
            // Let's copy the app binary somewhere accessible on the fs
            try (InputStream serverBytesStream = this.getClass().getClassLoader()
                .getResourceAsStream(NEO4J_APP_BIN)) {
                Path tmpServerBinaryPath =
                    createTempFile("neo4j-", "-app").toPath().toAbsolutePath();
                logger.debug("Copying neo4j app to {}", tmpServerBinaryPath);
                try (OutputStream serverBinaryOutputStream = Files.newOutputStream(
                    tmpServerBinaryPath)) {
                    serverBinaryOutputStream.write(serverBytesStream.readAllBytes());
                    serverBinaryOutputStream.flush();
                    appBinaryPath = tmpServerBinaryPath;
                }
                (new File(tmpServerBinaryPath.toAbsolutePath().toString())).setExecutable(true);
            }
            startServerCmd =
                new String[] {appBinaryPath.toString(), propertiesFile.getAbsolutePath()};
        }
        // Bind syslog
        cleaner.register(this.getClass(), () -> {
            logger.info("Closing syslog server...");
            LoggingUtils.SyslogServerSingleton.getInstance().close();
        });
        LoggingUtils.SyslogServerSingleton syslogServer =
            LoggingUtils.SyslogServerSingleton.getInstance();
        String syslogFacility = this.propertiesProvider
            .get("neo4jAppSyslogFacility")
            .orElseThrow(() -> new IllegalArgumentException(
                "neo4jAppSyslogFacility is missing from properties"));
        syslogServer.addHandler(new LoggingUtils.SyslogMessageHandler(
            Neo4jResource.class.getName(), syslogFacility, SYSLOG_SPLIT_CHAR));
        logger.info("Starting syslog server...");
        syslogServer.run();

        this.cleanPythonProcess = cleaner.register(this, new KillPythonProcess(this));
        checkServerCommand(startServerCmd);
        logger.info("Starting Python app running \"{}\"",
            lazy(() -> String.join(" ", startServerCmd)));
        serverProcess = new ProcessBuilder(startServerCmd).start();
        this.waitForServerToBeUp();
    }

    protected void checkServerCommand(String[] startServerCmd) {
        String mainCommand = (String) Optional.ofNullable(Array.get(startServerCmd, 0))
            .orElseThrow(() -> new InvalidNeo4jCommandError("Empty neo4j server command"));
        File maybeFile = new File(mainCommand);
        if (!maybeFile.isFile()) {
            String msg = maybeFile + " is not a file";
            throw new InvalidNeo4jCommandError(msg);
        } else if (!maybeFile.canExecute()) {
            String msg = maybeFile.getAbsolutePath() + " is not executable";
            throw new InvalidNeo4jCommandError(msg);
        }
    }

    protected boolean stopServerProcess() {
        boolean alreadyStopped = !isNeoAppRunning();
        if (!alreadyStopped) {
            synchronized (this) {
                if (isNeoAppRunning()) {
                    // TODO: check if we want to force destroyForcibly
                    serverProcess.toHandle().descendants().forEach(ProcessHandle::destroy);
                    serverProcess.destroy();
                    serverProcess = null;
                }
            }
        }
        return alreadyStopped;
    }

    protected org.icij.datashare.Objects.IncrementalImportResponse importDocuments(
        String projectId,
        org.icij.datashare.Objects.IncrementalImportRequest request
    ) {
        checkExtensionProject(projectId);
        checkNeo4jAppStarted();
        return client.importDocuments(request);
    }

    protected org.icij.datashare.Objects.IncrementalImportResponse importNamedEntities(
        String projectId, org.icij.datashare.Objects.IncrementalImportRequest request) {
        checkExtensionProject(projectId);
        checkNeo4jAppStarted();
        return client.importNamedEntities(request);
    }

    @Override
    public void close() {
        if (this.cleanPythonProcess != null) {
            this.cleanPythonProcess.clean();
        }
    }

    private Payload doImport(
        Provider<org.icij.datashare.Objects.IncrementalImportResponse> importProvider) {
        try {
            return new Payload(importProvider.get()).withCode(200);
        } catch (InvalidProjectError e) {
            return new Payload("application/problem+json", e).withCode(401);
        } catch (Neo4jNotRunningError e) {
            return new Payload("application/problem+json", e).withCode(503);
        } catch (Neo4jClient.Neo4jAppError e) { // TODO: this should be done automatically...
            return new Payload("application/problem+json", e).withCode(500);
        }
    }

    private void checkAccess(String project, Context context) {
        if (!((User) context.currentUser()).isGranted(project)) {
            throw new UnauthorizedException();
        }
    }

    // TODO: remove this for multiple projects support
    private void checkExtensionProject(String candidateProject) {
        if (!Objects.equals(this.projectId, candidateProject)) {
            InvalidProjectError error = new InvalidProjectError(this.projectId, candidateProject);
            logger.error(error.getMessage());
            throw error;
        }
    }

    static class KillPythonProcess implements Runnable {
        private final Neo4jResource resource;

        KillPythonProcess(Neo4jResource resource) {
            this.resource = resource;
        }

        public void run() {
            this.resource.stopServerProcess();
            if (this.resource.appBinaryPath != null) {
                File appBinFile = new File(this.resource.appBinaryPath.toAbsolutePath().toString());
                if (appBinFile.exists()) {
                    appBinFile.delete();
                }
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

    static class Neo4jNotRunningError extends HttpUtils.HttpError {
        public static final String title = "neo4j app not running";
        public static final String detail =
            "neo4j Python app is not running, please start it before calling the extension";

        public Neo4jNotRunningError() {
            super(title, detail);
        }
    }

    static class Neo4jAlreadyRunningError extends HttpUtils.HttpError {
        public static final String title = "neo4j app already running";
        public static final String detail =
            "neo4j Python app is already running likely in another phantom process";

        Neo4jAlreadyRunningError() {
            super(title, detail);
        }
    }

    static class InvalidProjectError extends HttpUtils.HttpError {
        public static final String title = "Invalid project";

        public InvalidProjectError(String project, String invalidProject) {
            super(title,
                "Invalid project '"
                    + invalidProject
                    + "' extension is setup to support project '"
                    + project
                    + "'");
        }
    }

    static class InvalidNeo4jCommandError extends RuntimeException {
        public InvalidNeo4jCommandError(String emptyNeo4jServerCommand) {
            super(emptyNeo4jServerCommand);
        }
    }
}
