package org.icij.datashare;

import static java.lang.Thread.sleep;
import static java.util.Arrays.asList;
import static org.icij.datashare.Neo4jResource.TASK_POLL_INTERVAL_S;
import static org.icij.datashare.json.JsonObjectMapper.MAPPER;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.inject.Injector;
import com.google.inject.Module;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.function.Function;
import java.util.stream.Collectors;
import joptsimple.OptionParser;
import org.icij.datashare.Objects.FullImportResponse;
import org.icij.datashare.Objects.Task;
import org.icij.datashare.Objects.TaskError;
import org.icij.datashare.Objects.TaskStatus;
import org.icij.datashare.cli.spi.CliExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Neo4jCliExtension implements CliExtension {
    private PropertiesProvider propertiesProvider;
    private Neo4jResource neo4jResource;

    private static final Logger logger = LoggerFactory.getLogger(Neo4jResource.class);

    private static final String NEO4J_EXTENSION_NAME = "neo4j";
    private static final String FULL_IMPORT = "full-import";
    private static final String PROJECT = "project";

    public Neo4jCliExtension() {
    }

    protected Neo4jCliExtension(
        PropertiesProvider propertiesProvider, Neo4jResource neo4jResource) {
        this.propertiesProvider = propertiesProvider;
        this.neo4jResource = neo4jResource;
    }

    @Override
    public void init(Function<Module[], Injector> injectModuleFn) {
        Injector injector = injectModuleFn.apply(new Module[] {});
        this.propertiesProvider = injector.getInstance(PropertiesProvider.class);
        this.neo4jResource = injector.getInstance(Neo4jResource.class);
    }

    @Override
    public void addOptions(OptionParser parser) {
        parser.accepts(FULL_IMPORT,
            "Performs a full import, importing all available documents and named entities "
                + "from Datashare to neo4j");
        parser.acceptsAll(asList(PROJECT, "p"), "Name of the datashare project")
            .withRequiredArg()
            .ofType(String.class);

        neo4jResource.addOptions(parser);
    }

    @Override
    public String identifier() {
        return NEO4J_EXTENSION_NAME;
    }

    @Override
    public void run(Properties properties) {
        if (properties.getProperty(FULL_IMPORT) != null) {
            String project = properties.getProperty(PROJECT);
            this.fullImport(project);
        }
    }

    protected void fullImport(String project) {
        this.checkResource();
        logger.info("Starting neo4j app...");
        this.neo4jResource.startNeo4jApp(true);
        logger.info("Initializing neo4j project...");
        this.neo4jResource.initProject(project);
        logger.info("Creating import task...");
        String taskId = this.neo4jResource.runFullImport(project, true);
        logger.info("Polling import task status...");
        FullImportResponse res = this.pollTask(taskId, project, FullImportResponse.class);
        try {
            // Logs are handled by the logger, output however goes on stdout to allow piping
            // the command
            System.out.println(MAPPER.writeValueAsString(res));
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to parse full import response as JSON", e);
        }
    }

    protected void checkResource() {
        Objects.requireNonNull(neo4jResource, "neo4jResource is needed to perform import");
    }

    protected <T> T pollTask(String taskId, String project, Class<T> resultClass) {
        Task task = null;
        long s = Long.parseLong(
            propertiesProvider.get(TASK_POLL_INTERVAL_S).orElse("2")
        ) * 1000;
        while (task == null || !TaskStatus.READY_STATES.contains(task.status)) {
            task = neo4jResource.task(taskId, project);
            Float progress = Optional.ofNullable(task.progress).orElse(0.0f);
            logger.info(
                "Task(id=\"{}\", status={}, progress={})", task.id, task.status, progress
            );
            try {
                sleep(s);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        logger.info("task {} has status {}, stopped polling", task, task.status);
        switch (task.status) {
            case DONE: {
                return neo4jResource.taskResult(task.id, project, resultClass);
            }
            case ERROR: {
                logger.error("task {} failed", task.id);
                List<TaskError> errors = neo4jResource.taskErrors(task.id, project);
                String msg = errors.stream()
                    .map(e -> "Title: " + e.title + "\nDetail: " + e.detail)
                    .collect(Collectors.joining("\n"));
                msg = "Task(id=\"" + taskId + "\") failed with the following cause(s):\n" + msg;
                throw new RuntimeException(msg);
            }
            case CANCELLED: {
                logger.error("task {} was cancelled !", task.id);
                throw new RuntimeException("Task(id=\"" + taskId + "\") was cancelled");
            }
            default:
                throw new IllegalArgumentException("unexpected status: " + task.status.name());
        }
    }
}
