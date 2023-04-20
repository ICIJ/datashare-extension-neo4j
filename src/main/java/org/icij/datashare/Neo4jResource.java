package org.icij.datashare;


import static java.io.File.createTempFile;
import static java.nio.file.Files.createTempDirectory;
import static org.icij.datashare.LoggingUtils.lazy;
import static org.icij.datashare.Neo4jAppLoader.getExtensionVersion;
import static org.icij.datashare.json.JsonObjectMapper.MAPPER;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.ref.Cleaner;
import java.net.Socket;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
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
public class Neo4jResource {
    private static final String NEO4J_APP_BIN = "neo4j-app";
    private static final Path TMP_ROOT = Path.of(
        FileSystems.getDefault().getSeparator(), "tmp");
    private static final String SYSLOG_SPLIT_CHAR = "@";

    private static final String PID_FILE_PATTERN = "glob:" + NEO4J_APP_BIN + "_*" + ".pid";
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
    // TODO: not sure that we want to delete the process when this deleted...
    // Cleaner which will ensure the Python server will be close when the resource is deleted
    private static Cleaner cleaner;
    protected final Neo4jClient client;
    private final PropertiesProvider propertiesProvider;
    private final int port;
    private final String host = "127.0.0.1";
    private final String projectId;
    private final Neo4jAppLoader appLoader;
    protected Path appBinaryPath;


    @Inject
    public Neo4jResource(PropertiesProvider propertiesProvider) {
        this.propertiesProvider = propertiesProvider;
        Properties neo4jDefaultProps = new Properties();
        neo4jDefaultProps.putAll(DEFAULT_NEO4J_PROPERTIES);
        this.propertiesProvider.mergeWith(neo4jDefaultProps);
        this.appLoader = new Neo4jAppLoader(this.propertiesProvider);
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
        throw new RuntimeException("Couldn't start Python 30s after starting it !");
    }

    @Post("/start")
    public Payload postStartNeo4jApp(Context context) throws IOException, InterruptedException {
        // TODO: check that the user is allowed
        boolean forceMigrations = false;
        if (!context.request().content().isBlank()) {
            forceMigrations = context.extract(
                org.icij.datashare.Objects.StartNeo4jAppRequest.class).forceMigration;
        }
        boolean alreadyRunning;
        try {
            alreadyRunning = this.startNeo4jApp(forceMigrations);
        } catch (Neo4jAlreadyRunningError e) {
            return new Payload("application/problem+json", e).withCode(500);
        }
        return new Payload(new ServerStartResponse(alreadyRunning));
    }

    @Post("/stop")
    public ServerStopResponse postStopNeo4jApp() throws IOException, InterruptedException {
        // TODO: check that the user is allowed
        return new ServerStopResponse(stopServerProcess());
    }

    @Get("/status")
    public Neo4jAppStatus getStopNeo4jApp() throws IOException, InterruptedException {
        // TODO: check that the user is allowed
        boolean isRunning = neo4jAppPid() != null;
        return new Neo4jAppStatus(isRunning);
    }

    @Post("/documents?project=:project")
    public Payload postDocuments(String project, Context context)
        throws IOException {
        checkAccess(project, context);
        // TODO: this should throw a bad request when not parsed correcly...
        org.icij.datashare.Objects.IncrementalImportRequest incrementalImportRequest =
            context.extract(org.icij.datashare.Objects.IncrementalImportRequest.class);
        return wrapNeo4jAppCall(() -> {
            try {
                return this.importDocuments(project, incrementalImportRequest);
            } catch (IOException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Post("/named-entities?project=:project")
    public Payload postNamedEntities(String project, Context context)
        throws IOException {
        checkAccess(project, context);
        // TODO: this should throw a bad request when not parsed correcly...
        org.icij.datashare.Objects.IncrementalImportRequest incrementalImportRequest =
            context.extract(org.icij.datashare.Objects.IncrementalImportRequest.class);
        return wrapNeo4jAppCall(() -> {
            try {
                return this.importNamedEntities(project, incrementalImportRequest);
            } catch (IOException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
    }

    //CHECKSTYLE.OFF: AbbreviationAsWordInName
    @Post("/admin/neo4j-csvs?project=:project")
    public Payload postNeo4jCSVs(String project, Context context) throws IOException {
        checkAccess(project, context);
        org.icij.datashare.Objects.Neo4jCSVRequest request =
            context.extract(org.icij.datashare.Objects.Neo4jCSVRequest.class);
        return wrapNeo4jAppCall(() -> {
            try {
                return this.exportNeo4jCSVs(request);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }
    //CHECKSTYLE.ON: AbbreviationAsWordInName

    @Get("/ping")
    public Payload getPingMethod() throws IOException, InterruptedException {
        try {
            return new Payload(this.ping());
        } catch (Neo4jNotRunningError e) {
            return new Payload("application/problem+json", e).withCode(503);
        }
    }

    private String ping() throws IOException, InterruptedException {
        checkNeo4jAppStarted();
        return client.ping();
    }

    private void checkNeo4jAppStarted() throws IOException, InterruptedException {
        if (neo4jAppPid() == null) {
            throw new Neo4jNotRunningError();
        }
    }

    private boolean startNeo4jApp(boolean forceMigrations)
        throws IOException, InterruptedException {
        boolean alreadyRunning = neo4jAppPid() != null;
        if (!alreadyRunning) {
            synchronized (Neo4jResource.class) {
                if (neo4jAppPid() == null) {
                    if (isOpen(host, port)) {
                        throw new Neo4jAlreadyRunningError();
                    }
                    this.startServerProcess(forceMigrations);
                }
            }
        }
        return alreadyRunning;
    }

    protected void startServerProcess(boolean forceMigrations)
        throws IOException, InterruptedException {
        File propertiesFile = createTempFile("neo4j-", "-datashare.properties");
        logger.debug("Copying Datashare properties to temporary location {}",
            lazy(propertiesFile::getAbsolutePath));

        this.propertiesProvider
            .getProperties()
            .store(Files.newOutputStream(propertiesFile.toPath().toAbsolutePath()), null);

        List<String> startServerCmd;
        Optional<String> propertiesCmd = this.propertiesProvider
            .get("neo4jStartServerCmd");
        if (propertiesCmd.isPresent()) {
            startServerCmd = Arrays.asList(propertiesCmd.get().split("\\s+"));
        } else {
            String version = getExtensionVersion();
            logger.debug("Load neo4j app version {}", version);
            File serverBinary = this.appLoader.downloadApp(version);
            // Let's copy the app binary somewhere accessible on the fs
            try (InputStream serverBytesStream = new FileInputStream(serverBinary)) {
                Path tmpServerBinaryPath = Files.createTempDirectory("neo4j-app")
                    .resolve(serverBinary.toPath().getFileName()).toAbsolutePath();
                logger.debug("Copying neo4j app binary to {}", tmpServerBinaryPath);
                try (OutputStream serverBinaryOutputStream = Files.newOutputStream(
                    tmpServerBinaryPath)) {
                    serverBinaryOutputStream.write(
                        Objects.requireNonNull(serverBytesStream).readAllBytes());
                    serverBinaryOutputStream.flush();
                    appBinaryPath = tmpServerBinaryPath;
                }
                (new File(tmpServerBinaryPath.toAbsolutePath().toString())).setExecutable(true);
            }
            startServerCmd = Arrays.asList(
                appBinaryPath.toString(),
                "--config-path",
                propertiesFile.getAbsolutePath()
            );
            if (forceMigrations) {
                startServerCmd.add("--force-migrations");
            }
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

        checkServerCommand(startServerCmd);
        logger.info("Starting Python app running \"{}\"",
            lazy(() -> String.join(" ", startServerCmd)));
        Process serverProcess = new ProcessBuilder(startServerCmd).start();
        ProcessUtils.dumpPid(
            Files.createTempFile(TMP_ROOT, NEO4J_APP_BIN + "_", ".pid").toFile(),
            serverProcess.pid()
        );
        this.waitForServerToBeUp();
    }

    protected void checkServerCommand(List<String> startServerCmd) {
        if (startServerCmd.isEmpty()) {
            throw new InvalidNeo4jCommandError("Empty neo4j server command");
        }
        String mainCommand = startServerCmd.get(0);
        File maybeFile = new File(mainCommand);
        if (!maybeFile.isFile()) {
            String msg = maybeFile + " is not a file";
            throw new InvalidNeo4jCommandError(msg);
        } else if (!maybeFile.canExecute()) {
            String msg = maybeFile.getAbsolutePath() + " is not executable";
            throw new InvalidNeo4jCommandError(msg);
        }
    }

    protected static boolean stopServerProcess() throws IOException, InterruptedException {
        boolean alreadyStopped = neo4jAppPid() == null;
        if (!alreadyStopped) {
            synchronized (Neo4jResource.class) {
                Long pid = neo4jAppPid();
                if (pid != null) {
                    ProcessUtils.killProcessById(pid);
                    Path maybePidPath = neo4jAppPidPath();
                    if (maybePidPath != null) {
                        Files.delete(maybePidPath);
                    }
                }
            }
        }
        return alreadyStopped;
    }

    private static Path neo4jAppPidPath() throws IOException {
        PathMatcher pathMatcher = FileSystems.getDefault().getPathMatcher(PID_FILE_PATTERN);
        try (Stream<Path> paths = Files.list(TMP_ROOT)) {
            List<Path> pidFilePaths = paths
                .filter(file -> !Files.isDirectory(file) && pathMatcher.matches(file.getFileName()))
                .collect(Collectors.toList());
            if (pidFilePaths.isEmpty()) {
                return null;
            }
            if (pidFilePaths.size() != 1) {
                String msg = "Found several matching PID files "
                    + pidFilePaths
                    + ", to avoid phantom Python process,"
                    + " kill these processes and clean the PID files";
                throw new RuntimeException(msg);
            }
            return pidFilePaths.get(0);
        }
    }

    private static Long neo4jAppPid() throws IOException, InterruptedException {
        Path maybePidPath = neo4jAppPidPath();
        if (maybePidPath != null) {
            Long maybePid = ProcessUtils.isProcessRunning(maybePidPath, 500, TimeUnit.MILLISECONDS);
            if (maybePid != null) {
                return maybePid;
            }
            // If the process is dead, let's clean the pid file
            Files.delete(maybePidPath);
        }
        return null;
    }


    protected org.icij.datashare.Objects.IncrementalImportResponse importDocuments(
        String projectId,
        org.icij.datashare.Objects.IncrementalImportRequest request
    ) throws IOException, InterruptedException {
        checkExtensionProject(projectId);
        checkNeo4jAppStarted();
        return client.importDocuments(request);
    }

    protected org.icij.datashare.Objects.IncrementalImportResponse importNamedEntities(
        String projectId, org.icij.datashare.Objects.IncrementalImportRequest request
    ) throws IOException, InterruptedException {
        checkExtensionProject(projectId);
        checkNeo4jAppStarted();
        return client.importNamedEntities(request);
    }

    //CHECKSTYLE.OFF: AbbreviationAsWordInName
    protected InputStream exportNeo4jCSVs(org.icij.datashare.Objects.Neo4jCSVRequest request)
        throws IOException {
        // Define a temp dir
        Path tmpDir = createTempDirectory(
            Path.of(FileSystems.getDefault().getSeparator(), "tmp"), "neo4j-export");
        org.icij.datashare.Objects.Neo4jAppNeo4jCSVRequest neo4jAppRequest = request.toNeo4j(
            tmpDir.toAbsolutePath().toString());
        try {
            org.icij.datashare.Objects.Neo4jCSVResponse res =
                client.exportNeo4jCSVs(neo4jAppRequest);
            logger.info("Exported data from ES to neo4j, statistics: {}",
                lazy(() -> MAPPER.writeValueAsString(res.metadata)));
            InputStream is = new FileInputStream(res.path);
            return new BufferedInputStream(is);
        } finally {
            Files.delete(tmpDir);
        }
    }
    //CHECKSTYLE.ON: AbbreviationAsWordInName

    private <T> Payload wrapNeo4jAppCall(Provider<T> responseProvider) {
        try {
            return new Payload(responseProvider.get()).withCode(200);
        } catch (InvalidProjectError e) {
            return new Payload("application/problem+json", e).withCode(401);
        } catch (Neo4jNotRunningError e) {
            return new Payload("application/problem+json", e).withCode(503);
        } catch (Neo4jClient.Neo4jAppError e) { // TODO: this should be done automatically...
            logger.error("internal error on the python app side {}", e.getMessage());
            return new Payload("application/problem+json", e).withCode(500);
        } catch (Exception e) { // TODO: this should be done automatically...
            logger.error("internal error on the java extension side {}", e.getMessage());
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
            try {
                this.resource.stopServerProcess();
            } catch (IOException | InterruptedException e) {
                throw new RuntimeException(e);
            }
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
