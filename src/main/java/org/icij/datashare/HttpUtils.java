package org.icij.datashare;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIncludeProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

public class HttpUtils {

    // Follow the JSON error + detail spec https://datatracker.ietf.org/doc/html/draft-ietf-appsawg-http-problem-00#page-4
    @JsonIncludeProperties({"title", "detail", "trace"})
    public static class HttpError extends RuntimeException {
        public String title;
        public String detail;
        public String trace;

        @JsonCreator
        HttpError(
                @JsonProperty("title") String title,
                @JsonProperty("detail") String detail,
                @JsonProperty("trace") String trace
        ) {
            super(title + "\nDetail: " + detail);
            this.title = title;
            this.detail = detail;
            this.trace = trace;
        }

        public HttpError(String title, String detail) {
            this(title, detail, null);
        }

        @Override
        public String getMessage() {
            String msg = super.getMessage();
            if (this.trace != null) {
                msg += "\nTrace: " + this.trace;
            }
            return msg;
        }
    }
}
