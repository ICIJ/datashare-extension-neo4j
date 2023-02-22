package org.icij.datashare;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.HashMap;

public class Objects {
    static class IncrementalImportResponse {

        public final long nToInsert;
        public final long nInserted;

        @JsonCreator
        IncrementalImportResponse(@JsonProperty("nToInsert") long nToInsert, @JsonProperty("nInserted") long nInserted) {
            this.nToInsert = nToInsert;
            this.nInserted = nInserted;
        }
    }

    static class IncrementalImportRequest {
        public HashMap<String, Object> query;

        @JsonCreator
        IncrementalImportRequest(@JsonProperty("query") HashMap<String, Object> query) {
            this.query = query;
        }
    }
}
