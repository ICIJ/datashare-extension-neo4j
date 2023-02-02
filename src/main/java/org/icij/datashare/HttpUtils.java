package org.icij.datashare;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIncludeProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.PrintWriter;
import java.io.StringWriter;

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
                @JsonProperty("detail") String detail
        ) {
            super(detail);
            this.title = title;
            this.detail = detail;
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            this.printStackTrace(pw);
            this.trace = sw.toString();
        }
    }

}
