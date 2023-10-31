package org.icij.datashare;


import static org.icij.datashare.HttpUtils.fromException;
import static org.icij.datashare.HttpUtils.parseContext;
import static org.icij.datashare.Objects.DumpRequest;
import static org.icij.datashare.Objects.IncrementalImportRequest;
import static org.icij.datashare.Objects.Neo4jAppNeo4jCSVRequest;
import static org.icij.datashare.Objects.SortedDumpRequest;
import static org.icij.datashare.Objects.StartNeo4jAppRequest;
import static org.icij.datashare.text.Project.isAllowed;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import java.util.concurrent.Callable;
import net.codestory.http.Context;
import net.codestory.http.annotations.Get;
import net.codestory.http.annotations.Post;
import net.codestory.http.annotations.Prefix;
import net.codestory.http.errors.ForbiddenException;
import net.codestory.http.errors.HttpException;
import net.codestory.http.payload.Payload;
import org.icij.datashare.user.User;

@Singleton
@Prefix("/api/neo4j")
public class Neo4jResourceWeb extends Neo4jResource {
    private final Repository repository;

    @Inject
    public Neo4jResourceWeb(Repository repository, PropertiesProvider propertiesProvider) {
        super(propertiesProvider);
        this.repository = repository;
    }

    @Post("/start")
    public Payload postStartNeo4jApp(Context context) {
        return wrapNeo4jAppCall(() -> {
                boolean forceMigrations = false;
                if (!context.request().content().isBlank()) {
                    forceMigrations = parseContext(
                        context, StartNeo4jAppRequest.class).forceMigration;
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
    public Payload getNeo4jAppStatus() {
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
            IncrementalImportRequest incrementalImportRequest = parseContext(
                context, IncrementalImportRequest.class);
            return this.importDocuments(project, incrementalImportRequest);
        });
    }

    @Post("/named-entities?project=:project")
    public Payload postNamedEntities(String project, Context context) {
        return wrapNeo4jAppCall(() -> {
            checkProjectAccess(project, context);
            // TODO: this should throw a bad request when not parsed correcly...
            IncrementalImportRequest incrementalImportRequest = parseContext(
                context, IncrementalImportRequest.class);
            return this.importNamedEntities(project, incrementalImportRequest);
        });
    }

    //CHECKSTYLE.OFF: AbbreviationAsWordInName
    @Post("/admin/neo4j-csvs?project=:project")
    public Payload postNeo4jCSVs(String project, Context context) {
        return wrapNeo4jAppCall(() -> {
            checkCheckLocal();
            checkProjectAccess(project, context);
            Neo4jAppNeo4jCSVRequest
                request = parseContext(context, Neo4jAppNeo4jCSVRequest.class);
            return this.exportNeo4jCSVs(project, request);
        });
    }
    //CHECKSTYLE.ON: AbbreviationAsWordInName

    @Post("/graphs/dump?project=:project")
    public Payload postGraphDump(String project, Context context) {
        return wrapNeo4jAppCall(() -> {
            checkProjectAccess(project, context);
            DumpRequest request = parseContext(context, DumpRequest.class);
            return this.dumpGraph(project, request);
        });
    }

    @Post("/graphs/sorted-dump?project=:project")
    public Payload postSortedGraphDump(String project, Context context) {
        return wrapNeo4jAppCall(() -> {
            checkProjectAccess(project, context);
            SortedDumpRequest request = parseContext(context, SortedDumpRequest.class);
            return this.sortedDumpGraph(project, request);
        });
    }

    @Get("/tasks/:taskId?project=:project")
    public Payload getTask(String taskId, String project, Context context) {
        return wrapNeo4jAppCall(() -> {
            checkProjectAccess(project, context);
            checkTaskAccess(taskId, project, context);
            return task(taskId, project);
        });
    }

    @Get("/graphs/counts?project=:project")
    public Payload getGraphCounts(String project, Context context) {
        return wrapNeo4jAppCall(() -> {
            checkProjectAccess(project, context);
            return graphCounts(project);
        });
    }

    @Post("/full-imports?project=:project")
    public Payload postFullImport(String project, Context context) {
        return wrapNeo4jAppCall(() -> {
            checkProjectAccess(project, context);
            checkCheckLocal();
            return new Payload(runFullImport(project, true)).withCode(201);
        });
    }

    @Get("/full-imports?project=:project")
    public Payload getSearchFullImports(String project, Context context) {
        return wrapNeo4jAppCall(() -> {
            checkProjectAccess(project, context);
            return searchFullImports(project);
        });
    }


    protected void checkProjectAccess(String project, Context context) throws ForbiddenException {
        if (!((User) context.currentUser()).isGranted(project)) {
            throw new ForbiddenException();
        }
        if (!isAllowed(repository.getProject(project), context.request().clientAddress())) {
            throw new ForbiddenException();
        }
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
            return new Payload("application/problem+json", returned).withCode(e.status);
        } catch (HttpException e) {
            return new Payload("application/problem+json", fromException(e))
                .withCode(e.code());
        } catch (ProjectNotInitialized e) {
            return new Payload("application/problem+json", fromException(e)).withCode(503);
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

}
