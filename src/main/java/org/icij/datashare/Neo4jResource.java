package org.icij.datashare;


import static java.io.File.createTempFile;
import static org.icij.datashare.LoggingUtils.lazy;
import static org.icij.datashare.Neo4jAppLoader.getExtensionVersion;
import static org.icij.datashare.Objects.DumpQuery;
import static org.icij.datashare.Objects.DumpRequest;
import static org.icij.datashare.Objects.GraphCount;
import static org.icij.datashare.Objects.IncrementalImportRequest;
import static org.icij.datashare.Objects.IncrementalImportResponse;
import static org.icij.datashare.Objects.Neo4jAppDumpRequest;
import static org.icij.datashare.Objects.Neo4jAppNeo4jCSVRequest;
import static org.icij.datashare.Objects.Neo4jCSVResponse;
import static org.icij.datashare.Objects.SortedDumpRequest;
import static org.icij.datashare.Objects.Task;
import static org.icij.datashare.Objects.TaskError;
import static org.icij.datashare.Objects.TaskSearch;
import static org.icij.datashare.Objects.TaskType;
import static org.icij.datashare.json.JsonObjectMapper.MAPPER;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import net.codestory.http.Context;
import net.codestory.http.errors.ForbiddenException;
import net.codestory.http.errors.UnauthorizedException;
import org.neo4j.cypherdsl.core.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Neo4jResource implements AutoCloseable {
    private static final String NEO4J_APP_BIN = "neo4j-app";
    private static final Path TMP_ROOT = Path.of(
        FileSystems.getDefault().getSeparator(), "tmp");
    private static final long NEO4J_DEFAULT_DUMPED_DOCUMENTS = 1000;
    private static final String SYSLOG_SPLIT_CHAR = "@";
    protected static final String TASK_POLL_INTERVAL_S = "neo4jCliTaskPollIntervalS";
    protected static final String NEO4J_PROCESS_INHERIT_OUTPUTS = "neo4jProcessInheritOutputs";
    protected static final String NEO4J_APP_LOG_IN_JSON = "neo4jAppLogInJson";

    private static final String PID_FILE_PATTERN = "glob:" + NEO4J_APP_BIN + "_*" + ".pid";
    // All these properties have to start with "neo4j" in order to be properly filtered
    protected static final List<List<Object>> DEFAULT_CLI_OPTIONS = List.of(
        List.of(
            NEO4J_APP_LOG_IN_JSON,
            false,
            "Should the Python process log in JSON format"
        ),
        List.of("neo4jAppStartTimeoutS", 30, "Python neo4j service start timeout."),
        List.of("neo4jAppPort", 8008, "Python neo4j service port"),
        List.of(
            "neo4jAppSyslogFacility",
            "",
            "Syslog facility used to log from the Python neo4j service to Datashare Java app"
        ),
        List.of(
            "neo4jSingleProject",
            "local-datashare",
            "Name of the single project which will be able to user the extension when using neo4j"
                + " Community Edition"
        ),
        List.of("neo4jHost", "127.0.0.1", "Hostname of the neo4j DB."),
        List.of("neo4jPort", 7687, "Port of the neo4j DB."),
        List.of(
            "neo4jUriScheme",
            "bolt",
            "URI scheme used to connect to the neo4j DB (can be: bolt, neo4j, bolt+s, neo4j+s,"
                + " ....)"
        ),
        List.of("neo4jUser", "neo4j", "User name used to connect to the neo4j DB"),
        List.of(
            "neo4jPassword",
            "please-change-this-password",
            "Password used to connect to the neo4j DB"
        ),
        List.of(
            TASK_POLL_INTERVAL_S,
            2,
            "Interval in second used to poll task statuses when in CLI mode"
        ),
        List.of(
            NEO4J_PROCESS_INHERIT_OUTPUTS,
            true,
            "Should the Python process outputs be redirected to the Java process outputs ?"
        )
    );
    //CHECKSTYLE.OFF: MemberName
    //CHECKSTYLE.OFF: AbbreviationAsWordInName
    Map<String, String> DEFAULT_NEO4J_PROPERTIES_MAP = DEFAULT_CLI_OPTIONS.stream()
        .collect(
            Collectors.toMap((item) -> item.get(0).toString(), (item) -> item.get(1).toString())
        );
    //CHECKSTYLE.ON: AbbreviationAsWordInName
    //CHECKSTYLE.ON: MemberName

    protected static final HashSet<String> projects = new HashSet<>();

    private static final TaskSearch FULL_IMPORT_SEARCH = new TaskSearch(
        TaskType.FULL_IMPORT, null);

    protected static Boolean supportNeo4jEnterprise;

    protected static final Logger logger = LoggerFactory.getLogger(Neo4jResource.class);
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

    protected Neo4jResource(PropertiesProvider propertiesProvider) {
        this.propertiesProvider = propertiesProvider;
        Properties neo4jDefaultProps = new Properties();
        neo4jDefaultProps.putAll(DEFAULT_NEO4J_PROPERTIES_MAP.entrySet().stream()
            .filter(entry -> !entry.getValue().isEmpty())
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
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
                // TODO: in addition we should ping the Python service, ports might be open slightly
                //  ahead of readiness
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
        long timeoutS = timeout / 1000;
        throw new RuntimeException("Couldn't start Python " + timeoutS + "s after starting it !");
    }

    protected boolean pingSuccessful() {
        return client.pingSuccessful();
    }

    protected void checkNeo4jAppStarted() {
        if (neo4jAppPid() == null) {
            throw new Neo4jNotRunningError();
        }
    }

    boolean startNeo4jApp(boolean forceMigrations) {
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

        Properties setProperties = filterUnset(this.propertiesProvider.getProperties());

        try {
            setProperties.store(
                Files.newOutputStream(propertiesFile.toPath().toAbsolutePath()), null);
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
            startServerCmd = new ArrayList<>(Arrays.asList(
                appBinaryPath.toString(),
                "--config-path",
                propertiesFile.getAbsolutePath()
            ));
            if (forceMigrations) {
                startServerCmd.add("--force-migrations");
            }
        }
        // Bind syslog
        bindSyslog();

        checkServerCommand(startServerCmd);
        logger.info("Starting Python app running \"{}\"",
            lazy(() -> String.join(" ", startServerCmd)));
        ProcessBuilder builder = new ProcessBuilder(startServerCmd);
        Boolean inheritOutputs = propertiesProvider.get(NEO4J_PROCESS_INHERIT_OUTPUTS)
            .map(Boolean::parseBoolean).orElse(true);
        if (inheritOutputs) {
            // We don't inherit the input to avoid killing the Python process when ^C the
            // java process
            builder.redirectError(ProcessBuilder.Redirect.INHERIT)
                .redirectOutput(ProcessBuilder.Redirect.INHERIT);
        }
        Process serverProcess;
        try {
            serverProcess = builder.start();
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

    protected static Long neo4jAppPid() {
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

    protected void bindSyslog() {
        this.propertiesProvider
            .get("neo4jAppSyslogFacility")
            .ifPresent(syslogFacility -> {
                logger.info("Binding syslog server...");
                cleaner.register(this.getClass(), () -> {
                    logger.info("Closing syslog server...");
                    LoggingUtils.SyslogServerSingleton.getInstance().close();
                });
                LoggingUtils.SyslogServerSingleton syslogServer =
                    LoggingUtils.SyslogServerSingleton.getInstance();
                syslogServer.addHandler(new LoggingUtils.SyslogMessageHandler(
                    Neo4jResource.class.getName(), syslogFacility, SYSLOG_SPLIT_CHAR));
                logger.info("Starting syslog server...");
                syslogServer.run();
            });
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

    protected IncrementalImportResponse importDocuments(
        String projectId, IncrementalImportRequest request
    ) {
        checkProject(projectId);
        checkNeo4jAppStarted();
        return client.importDocuments(projectId, request);
    }

    protected IncrementalImportResponse importNamedEntities(
        String projectId, IncrementalImportRequest request
    ) {
        checkProject(projectId);
        checkNeo4jAppStarted();
        return client.importNamedEntities(projectId, request);
    }

    protected String runFullImport(String projectId, boolean force) {
        checkProject(projectId);
        checkNeo4jAppStarted();
        return client.fullImport(projectId, force);
    }

    protected List<Task> searchFullImports(String projectId) {
        checkProject(projectId);
        checkNeo4jAppStarted();
        return client.taskSearch(projectId, FULL_IMPORT_SEARCH);
    }

    protected Task task(String taskId, String projectId) {
        checkProject(projectId);
        checkNeo4jAppStarted();
        return client.task(taskId, projectId);
    }

    protected <T> T taskResult(String taskId, String projectId, Class<T> clazz) {
        checkProject(projectId);
        checkNeo4jAppStarted();
        return client.taskResult(taskId, projectId, clazz);
    }

    protected List<TaskError> taskErrors(String taskId, String projectId) {
        checkProject(projectId);
        checkNeo4jAppStarted();
        return List.of(client.taskErrors(taskId, projectId));
    }

    protected GraphCount graphCounts(String projectId) {
        checkProject(projectId);
        checkNeo4jAppStarted();
        return client.graphCounts(projectId);
    }


    //CHECKSTYLE.OFF: AbbreviationAsWordInName
    protected InputStream exportNeo4jCSVs(String projectId, Neo4jAppNeo4jCSVRequest request) {
        // TODO: the database should be chosen dynamically with the Mode (local vs. server) and
        //  the project
        checkProject(projectId);
        checkNeo4jAppStarted();
        // Define a temp dir
        Path exportDir = null;
        try {
            Neo4jCSVResponse res = client.exportNeo4jCSVs(projectId, request);
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
        String projectId, DumpRequest request
    ) throws URISyntaxException, IOException, InterruptedException {
        Neo4jAppDumpRequest neo4jAppRequest = validateDumpRequest(request);
        checkProject(projectId);
        checkNeo4jAppStarted();
        return client.dumpGraph(projectId, neo4jAppRequest);
    }

    protected InputStream sortedDumpGraph(
        String projectId, SortedDumpRequest request
    ) throws URISyntaxException, IOException, InterruptedException {
        checkProject(projectId);
        checkNeo4jAppStarted();
        Statement statement = request.query.defaultQueryStatement(getDocumentNodesLimit());
        Neo4jAppDumpRequest neo4jAppRequest = new Neo4jAppDumpRequest(
            request.format, statement.getCypher());
        return client.dumpGraph(projectId, neo4jAppRequest);
    }

    protected void checkTaskAccess(String taskId, String project, Context context)
        throws UnauthorizedException {
        if (!isServer()) {
            return;
        }
        // TODO: in the future, check that the context user is that task user
        throw new UnauthorizedException();
    }


    protected void checkCheckLocal() {
        if (!isLocal()) {
            throw new ForbiddenException();
        }
    }

    protected void checkProject(String project) {
        checkExtensionProject(project);
        checkProjectInitialized(project);
    }

    protected void checkExtensionProject(String project) {
        if (!supportsNeo4jEnterprise()) {
            if (this.neo4jSingleProjectId != null) {
                if (!Objects.equals(this.neo4jSingleProjectId, project)) {
                    InvalidProjectError error =
                        new InvalidProjectError(this.neo4jSingleProjectId, project);
                    logger.error(error.getMessage());
                    throw error;
                }
            }
        }
    }

    protected void checkProjectInitialized(String project) {
        if (!projects.contains(project)) {
            throw new ProjectNotInitialized(project);
        }
    }

    protected Neo4jAppDumpRequest validateDumpRequest(
        DumpRequest request) {
        String validated = null;
        if (isLocal()) {
            if (request.query != null) {
                validated = request.query.asValidated().getCypher();
            }
        } else {
            long defaultLimit = getDocumentNodesLimit();
            if (request.query == null) {
                validated = DumpQuery.defaultQueryStatement(
                    defaultLimit).getCypher();
            } else {
                validated = request.query.asValidated(getDocumentNodesLimit()).getCypher();
            }
        }
        return new Neo4jAppDumpRequest(request.format, validated);
    }

    private boolean isLocal() {
        String mode = getMode();
        return mode.equals("LOCAL") || mode.equals("EMBEDDED");
    }

    private boolean isServer() {
        return getMode().equals("SERVER");
    }


    private String getMode() {
        return propertiesProvider.get("mode").orElse("SERVER");
    }

    private long getDocumentNodesLimit() {
        return propertiesProvider
            .get("neo4jDocumentNodesLimit")
            .map(Long::parseLong)
            .orElse(NEO4J_DEFAULT_DUMPED_DOCUMENTS);
    }

    static Properties filterUnset(Properties properties) {
        Properties filtered = new Properties(properties.size());
        properties.forEach((key, value) -> {
            if (!((String) value).isEmpty()) {
                filtered.setProperty(key.toString(), value.toString());
            }
        });
        return filtered;
    }

    @Override
    public void close() throws Exception {
        this.client.close();
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

    static class ProjectNotInitialized extends HttpUtils.HttpError {
        public static final String title = "Project Not Initialized";

        public ProjectNotInitialized(String project) {
            super(title, "Project \"" + project + "\" as not been initialized");
        }
    }

    static class InvalidNeo4jCommandError extends RuntimeException {
        public InvalidNeo4jCommandError(String emptyNeo4jServerCommand) {
            super(emptyNeo4jServerCommand);
        }
    }

}
