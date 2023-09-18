package org.icij.datashare;


import static java.io.File.createTempFile;
import static org.icij.datashare.HttpUtils.fromException;
import static org.icij.datashare.HttpUtils.parseContext;
import static org.icij.datashare.LoggingUtils.lazy;
import static org.icij.datashare.Neo4jAppLoader.getExtensionVersion;
import static org.icij.datashare.json.JsonObjectMapper.MAPPER;
import static org.icij.datashare.text.Project.isAllowed;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.ref.Cleaner;
import java.net.Socket;
import java.net.URISyntaxException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import kong.unirest.Unirest;
import kong.unirest.apache.ApacheClient;
import net.codestory.http.Context;
import net.codestory.http.annotations.Get;
import net.codestory.http.annotations.Post;
import net.codestory.http.annotations.Prefix;
import net.codestory.http.errors.ForbiddenException;
import net.codestory.http.payload.Payload;
import org.apache.http.impl.client.HttpClientBuilder;
import org.icij.datashare.user.User;
import org.neo4j.cypherdsl.core.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
@Prefix("/api/neo4j")
public class Neo4jResource {
    private final Repository repository;
    private static final String NEO4J_APP_BIN = "neo4j-app";
    private static final Path TMP_ROOT = Path.of(
        FileSystems.getDefault().getSeparator(), "tmp");
    private static final long NEO4J_DEFAULT_DUMPED_DOCUMENTS = 1000;
    private static final String SYSLOG_SPLIT_CHAR = "@";

    private static final String PID_FILE_PATTERN = "glob:" + NEO4J_APP_BIN + "_*" + ".pid";
    // All these properties have to start with "neo4j" in order to be properly filtered
    private static final HashMap<String, String> DEFAULT_NEO4J_PROPERTIES = new HashMap<>() {
        {
            put("neo4jAppPort", "8008");
            put("neo4jAppStartTimeoutS", "30");
            put("neo4jAppSyslogFacility", "LOCAL7");
            put("neo4jHost", "neo4j");
            put("neo4jPassword", "");
            put("neo4jPort", "7687");
            put("neo4jDocumentNodesLimit", String.valueOf(NEO4J_DEFAULT_DUMPED_DOCUMENTS));
            put("neo4jSingleProject", "local-datashare");
            put("neo4jUriScheme", "neo4j");
            put("neo4jUser", "");
        }
    };

    protected static final HashSet<String> projects = new HashSet<>();
    protected static Boolean supportNeo4jEnterprise;

    private static final Logger logger = LoggerFactory.getLogger(Neo4jResource.class);
    // TODO: not sure that we want to delete the process when this deleted...
    // Cleaner which will ensure the Python server will be close when the resource is deleted
    private static Cleaner cleaner;
    protected final Neo4jClient client;
    private final PropertiesProvider propertiesProvider;
    private final int port;
    protected final String host = "127.0.0.1";
    private String neo4jSingleProjectId;
    private final Neo4jAppLoader appLoader;
    protected Path appBinaryPath;


    @Inject
    public Neo4jResource(Repository repository, PropertiesProvider propertiesProvider) {
        this.repository = repository;
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
        propertiesProvider
            .get("neo4jSingleProject")
            .ifPresent(projectId -> {
                if (!projectId.isEmpty()) {
                    neo4jSingleProjectId = projectId;
                }
            });
        Unirest.config().httpClient(ApacheClient.builder(HttpClientBuilder.create().build()));
    }

    protected static boolean isOpen(String host, int port) {
        try (Socket ignored = new Socket(host, port)) {
            return true;
        } catch (IOException ignored) {
            return false;
        }
    }

    protected void waitForServerToBeUp() {
        long start = System.currentTimeMillis();
        long timeout = Long.parseLong(
            this.propertiesProvider.get("neo4jAppStartTimeoutS").orElse("30")
        ) * 1000;
        long elapsed = 0;
        while (elapsed < timeout) {
            if (isOpen(this.host, this.port) && pingSuccessful()) {
                return;
            } else {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    throw new RuntimeException("Thread killed while slipping", e);
                }
            }
            elapsed = System.currentTimeMillis() - start;
        }
        long timeouts = timeout / 1000;
        throw new RuntimeException("Couldn't start Python " + timeouts + "s after starting it !");
    }

    protected boolean pingSuccessful() {
        return client.pingSuccessful();
    }

    @Post("/start")
    public Payload postStartNeo4jApp(Context context) {
        return wrapNeo4jAppCall(() -> {
                boolean forceMigrations = false;
                if (!context.request().content().isBlank()) {
                    forceMigrations = parseContext(context,
                        org.icij.datashare.Objects.StartNeo4jAppRequest.class
                    ).forceMigration;
                }
                boolean alreadyRunning = this.startNeo4jApp(forceMigrations);
                return new Payload(new ServerStartResponse(alreadyRunning));
            }
        );
    }

    @Post("/stop")
    public Payload postStopNeo4jApp() {
        return wrapNeo4jAppCall(() -> new ServerStopResponse(stopServerProcess())
        );
    }

    @Get("/status")
    public Payload getStopNeo4jApp() {
        return wrapNeo4jAppCall(() -> {
            boolean isRunning = neo4jAppPid() != null;
            return new Neo4jAppStatus(isRunning);
        });
    }

    @Post("/init?project=:project")
    public Payload postInitProject(String project, Context context) {
        return wrapNeo4jAppCall(() -> {
            checkProjectAccess(project, context);
            boolean created = this.initProject(project);
            int code;
            if (created) {
                code = 201;
            } else {
                code = 200;
            }
            return new Payload(created).withCode(code);
        });
    }

    @Post("/documents?project=:project")
    public Payload postDocuments(String project, Context context) {
        return wrapNeo4jAppCall(() -> {
            checkProjectAccess(project, context);
            org.icij.datashare.Objects.IncrementalImportRequest incrementalImportRequest =
                parseContext(context, org.icij.datashare.Objects.IncrementalImportRequest.class);
            return this.importDocuments(project, incrementalImportRequest);
        });
    }

    @Post("/named-entities?project=:project")
    public Payload postNamedEntities(String project, Context context) {
        return wrapNeo4jAppCall(() -> {
            checkProjectAccess(project, context);
            // TODO: this should throw a bad request when not parsed correcly...
            org.icij.datashare.Objects.IncrementalImportRequest incrementalImportRequest =
                parseContext(context, org.icij.datashare.Objects.IncrementalImportRequest.class);
            return this.importNamedEntities(project, incrementalImportRequest);
        });
    }

    //CHECKSTYLE.OFF: AbbreviationAsWordInName
    @Post("/admin/neo4j-csvs?project=:project")
    public Payload postNeo4jCSVs(String project, Context context) {
        return wrapNeo4jAppCall(() -> {
            checkCheckLocal();
            checkProjectAccess(project, context);
            org.icij.datashare.Objects.Neo4jAppNeo4jCSVRequest request = parseContext(
                context, org.icij.datashare.Objects.Neo4jAppNeo4jCSVRequest.class);
            return this.exportNeo4jCSVs(project, request);
        });
    }
    //CHECKSTYLE.ON: AbbreviationAsWordInName

    @Post("/graphs/dump?project=:project")
    public Payload postGraphDump(String project, Context context) {
        return wrapNeo4jAppCall(() -> {
            checkProjectAccess(project, context);
            org.icij.datashare.Objects.DumpRequest request = parseContext(
                context, org.icij.datashare.Objects.DumpRequest.class);
            return this.dumpGraph(project, request);
        });
    }

    @Post("/graphs/sorted-dump?project=:project")
    public Payload postSortedGraphDump(String project, Context context) {
        return wrapNeo4jAppCall(() -> {
            checkProjectAccess(project, context);
            org.icij.datashare.Objects.SortedDumpRequest request = parseContext(
                context, org.icij.datashare.Objects.SortedDumpRequest.class);
            return this.sortedDumpGraph(project, request);
        });
    }


    protected void checkNeo4jAppStarted() {
        if (neo4jAppPid() == null) {
            throw new Neo4jNotRunningError();
        }
    }

    private boolean startNeo4jApp(boolean forceMigrations) {
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

    protected void startServerProcess(boolean forceMigrations) {
        File propertiesFile;
        try {
            propertiesFile = createTempFile("neo4j-", "-datashare.properties");
        } catch (IOException e) {
            throw new RuntimeException("Failed create temporary properties file", e);
        }
        logger.debug("Copying Datashare properties to temporary location {}",
            lazy(propertiesFile::getAbsolutePath));

        try {
            this.propertiesProvider
                .getProperties()
                .store(Files.newOutputStream(propertiesFile.toPath().toAbsolutePath()), null);
        } catch (IOException e) {
            throw new RuntimeException("Failed to write properties in temporary location", e);
        }

        List<String> startServerCmd;
        Optional<String> propertiesCmd = this.propertiesProvider
            .get("neo4jStartServerCmd")
            .filter(p -> !p.isEmpty());
        if (propertiesCmd.isPresent()) {
            startServerCmd = Arrays.asList(propertiesCmd.get().split("\\s+"));
        } else {
            String version = getExtensionVersion();
            logger.debug("Load neo4j app version {}", version);
            File serverBinary;
            try {
                serverBinary = this.appLoader.downloadApp(version);
            } catch (IOException e) {
                throw new RuntimeException("Failed to download app", e);
            }
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
            } catch (IOException e) {
                throw new RuntimeException("Failed to read app binary", e);
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
        Process serverProcess;
        try {
            serverProcess = new ProcessBuilder(startServerCmd).start();
        } catch (IOException e) {
            throw new RuntimeException("Failed to start server process", e);
        }
        this.waitForServerToBeUp();
        try {
            ProcessUtils.dumpPid(
                Files.createTempFile(TMP_ROOT, NEO4J_APP_BIN + "_", ".pid").toFile(),
                serverProcess.pid()
            );
        } catch (IOException e) {
            throw new RuntimeException("Failed to dump app PID in " + TMP_ROOT, e);
        }
    }

    protected boolean supportsNeo4jEnterprise() {
        checkNeo4jAppStarted();
        synchronized (Neo4jResource.class) {
            if (supportNeo4jEnterprise == null) {
                Boolean support = (Boolean) client.config().get("supportsNeo4jEnterprise");
                supportNeo4jEnterprise = Objects.requireNonNull(support,
                    "Couldn't read enterprise support from config");
            }
        }
        return Neo4jResource.supportNeo4jEnterprise;
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

    protected static boolean stopServerProcess() {
        boolean alreadyStopped = neo4jAppPid() == null;
        if (!alreadyStopped) {
            synchronized (Neo4jResource.class) {
                Long pid = neo4jAppPid();
                if (pid != null) {
                    ProcessUtils.killProcessById(pid);
                    Path maybePidPath = neo4jAppPidPath();
                    if (maybePidPath != null) {
                        try {
                            Files.delete(maybePidPath);
                        } catch (IOException e) {
                            throw new RuntimeException(
                                "Failed to delete PID file at " + maybePidPath, e
                            );
                        }
                    }
                }
            }
        }
        return alreadyStopped;
    }

    private static Path neo4jAppPidPath() {
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
        } catch (IOException e) {
            throw new RuntimeException("Failed to list files in " + TMP_ROOT, e);
        }
    }

    private static Long neo4jAppPid() {
        Path maybePidPath = neo4jAppPidPath();
        if (maybePidPath != null) {
            Long maybePid = ProcessUtils.isProcessRunning(maybePidPath, 500, TimeUnit.MILLISECONDS);
            if (maybePid != null) {
                return maybePid;
            }
            // If the process is dead, let's clean the pid file
            try {
                Files.delete(maybePidPath);
            } catch (IOException e) {
                throw new RuntimeException("Failed to deleted PID file at " + maybePidPath, e);
            }
        }
        return null;
    }

    protected boolean initProject(String projectId) {
        checkExtensionProject(projectId);
        checkNeo4jAppStarted();
        boolean created;
        synchronized (projects) {
            if (!projects.contains(projectId)) {
                created = client.initProject(projectId);
                projects.add(projectId);
            } else {
                created = false;
            }
        }
        return created;
    }

    protected org.icij.datashare.Objects.IncrementalImportResponse importDocuments(
        String projectId,
        org.icij.datashare.Objects.IncrementalImportRequest request
    ) {
        checkExtensionProject(projectId);
        checkNeo4jAppStarted();
        return client.importDocuments(projectId, request);
    }

    protected org.icij.datashare.Objects.IncrementalImportResponse importNamedEntities(
        String projectId, org.icij.datashare.Objects.IncrementalImportRequest request
    ) {
        checkExtensionProject(projectId);
        checkNeo4jAppStarted();
        return client.importNamedEntities(projectId, request);
    }

    //CHECKSTYLE.OFF: AbbreviationAsWordInName
    protected InputStream exportNeo4jCSVs(
        String projectId, org.icij.datashare.Objects.Neo4jAppNeo4jCSVRequest request
    ) {
        // TODO: the database should be chosen dynamically with the Mode (local vs. server) and
        //  the project
        checkExtensionProject(projectId);
        checkNeo4jAppStarted();
        // Define a temp dir
        Path exportDir = null;
        try {
            org.icij.datashare.Objects.Neo4jCSVResponse res =
                client.exportNeo4jCSVs(projectId, request);
            logger.info("Exported data from ES to neo4j, statistics: {}",
                lazy(() -> MAPPER.writeValueAsString(res.metadata)));
            exportDir = Paths.get(res.path);
            InputStream is = new FileInputStream(res.path);
            return new BufferedInputStream(is);
        } catch (FileNotFoundException e) {
            throw new RuntimeException("Failed to read exported CSV", e);
        } finally {
            if (exportDir != null) {
                try {
                    Files.walk(exportDir)
                        .sorted(Comparator.reverseOrder())
                        .map(Path::toFile)
                        .forEach(File::delete);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }
    //CHECKSTYLE.ON: AbbreviationAsWordInName

    protected InputStream dumpGraph(
        String projectId, org.icij.datashare.Objects.DumpRequest request
    ) throws URISyntaxException, IOException, InterruptedException {
        org.icij.datashare.Objects.Neo4jAppDumpRequest neo4jAppRequest = validateDumpRequest(
            request);
        checkExtensionProject(projectId);
        checkNeo4jAppStarted();
        return client.dumpGraph(projectId, neo4jAppRequest);
    }

    protected InputStream sortedDumpGraph(
        String projectId, org.icij.datashare.Objects.SortedDumpRequest request
    ) throws URISyntaxException, IOException, InterruptedException {
        checkExtensionProject(projectId);
        checkNeo4jAppStarted();
        Statement statement = request.query.defaultQueryStatement(getDocumentNodesLimit());
        org.icij.datashare.Objects.Neo4jAppDumpRequest neo4jAppRequest =
            new org.icij.datashare.Objects.Neo4jAppDumpRequest(
                request.format, statement.getCypher()
            );
        return client.dumpGraph(projectId, neo4jAppRequest);
    }

    private <T> Payload wrapNeo4jAppCall(Callable<T> responseProvider) {
        try {
            return new Payload(responseProvider.call());
        } catch (InvalidProjectError e) {
            return new Payload("application/problem+json", e).withCode(403);
        } catch (Neo4jNotRunningError e) {
            return new Payload("application/problem+json", e).withCode(503);
        } catch (Neo4jClient.Neo4jAppError e) {
            HttpUtils.HttpError returned = e.toHttp();
            logger.error(
                "internal error on the python app side {}", returned.getMessageWithTrace()
            );
            return new Payload("application/problem+json", returned).withCode(500);
        } catch (ForbiddenException e) {
            return new Payload("application/problem+json", fromException(e)).withCode(403);
        } catch (HttpUtils.JacksonParseError e) {
            HttpUtils.HttpError returned = fromException(e);
            logger.error(returned.getMessageWithTrace());
            return new Payload("application/problem+json", returned).withCode(400);
        } catch (Exception e) {
            HttpUtils.HttpError returned = fromException(e);
            logger.error("internal error on the java extension side {}",
                returned.getMessageWithTrace());
            return new Payload("application/problem+json", returned).withCode(500);
        }
    }

    protected void checkProjectAccess(String project, Context context) throws ForbiddenException {
        if (!((User) context.currentUser()).isGranted(project)) {
            throw new ForbiddenException();
        }
        if (!isAllowed(repository.getProject(project), context.request().clientAddress())) {
            throw new ForbiddenException();
        }
    }

    protected void checkCheckLocal() {
        if (!isLocal()) {
            throw new ForbiddenException();
        }
    }

    protected void checkExtensionProject(String candidateProject) {
        if (!supportsNeo4jEnterprise()) {
            if (this.neo4jSingleProjectId != null) {
                if (!Objects.equals(this.neo4jSingleProjectId, candidateProject)) {
                    InvalidProjectError error = new InvalidProjectError(
                        this.neo4jSingleProjectId, candidateProject);
                    logger.error(error.getMessage());
                    throw error;
                }
            }
        }
    }

    protected org.icij.datashare.Objects.Neo4jAppDumpRequest validateDumpRequest(
        org.icij.datashare.Objects.DumpRequest request) {
        String validated = null;
        if (isLocal()) {
            if (request.query != null) {
                validated = request.query.asValidated().getCypher();
            }
        } else {
            long defaultLimit = getDocumentNodesLimit();
            if (request.query == null) {
                validated = org.icij.datashare.Objects.DumpQuery.defaultQueryStatement(
                    defaultLimit).getCypher();
            } else {
                validated = request.query.asValidated(getDocumentNodesLimit()).getCypher();
            }
        }
        return new org.icij.datashare.Objects.Neo4jAppDumpRequest(request.format, validated);
    }

    private boolean isLocal() {
        String mode = propertiesProvider.get("mode").orElse("SERVER");
        return mode.equals("LOCAL") || mode.equals("EMBEDDED");
    }

    private long getDocumentNodesLimit() {
        return propertiesProvider
            .get("neo4jDocumentNodesLimit")
            .map(Long::parseLong)
            .orElse(NEO4J_DEFAULT_DUMPED_DOCUMENTS);
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

    protected static class Neo4jAlreadyRunningError extends HttpUtils.HttpError {
        protected Neo4jAlreadyRunningError() {
            super(Neo4jAlreadyRunningError.class.getSimpleName(),
                "neo4j Python app is already running, likely in another phantom process");
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
