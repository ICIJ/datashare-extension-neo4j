package org.icij.datashare;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.HashMap;

public class Objects {
    static class IncrementalImportResponse {
        //CHECKSTYLE.OFF: MemberName
        public final long nodesImported;
        public final long nodesCreated;
        public final long relationshipsCreated;
        //CHECKSTYLE.ON: MemberName

        //CHECKSTYLE.OFF: ParameterName
        @JsonCreator
        IncrementalImportResponse(@JsonProperty("nodesImported") long nodesImported,
                                  @JsonProperty("nodesCreated") long nodesCreated,
                                  @JsonProperty("relationshipsCreated") long relationshipsCreated) {
            this.nodesImported = nodesImported;
            this.nodesCreated = nodesCreated;
            this.relationshipsCreated = relationshipsCreated;
        }
        //CHECKSTYLE.ON: ParameterName
    }

    static class IncrementalImportRequest {
        public HashMap<String, Object> query;

        @JsonCreator
        IncrementalImportRequest(@JsonProperty("query") HashMap<String, Object> query) {
            this.query = query;
        }
    }

    static class StartNeo4jAppRequest {
        public boolean forceMigration;

        @JsonCreator
        StartNeo4jAppRequest(@JsonProperty("forceMigration") boolean forceMigration) {
            this.forceMigration = forceMigration;
        }
    }
}
