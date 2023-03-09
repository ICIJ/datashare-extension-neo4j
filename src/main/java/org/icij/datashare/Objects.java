package org.icij.datashare;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.HashMap;

public class Objects {
    static class IncrementalImportResponse {
        //CHECKSTYLE.OFF: MemberName
        public final long nToInsert;
        public final long nInserted;
        //CHECKSTYLE.ON: MemberName

        //CHECKSTYLE.OFF: ParameterName
        @JsonCreator
        IncrementalImportResponse(@JsonProperty("nToInsert") long nToInsert,
                                  @JsonProperty("nInserted") long nInserted) {
            this.nToInsert = nToInsert;
            this.nInserted = nInserted;
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
