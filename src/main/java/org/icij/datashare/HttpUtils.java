package org.icij.datashare;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIncludeProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import net.codestory.http.Context;

public class HttpUtils {

    // Follow the JSON error + detail spec https://datatracker.ietf.org/doc/html/draft-ietf-appsawg-http-problem-00#page-4
    @JsonIncludeProperties({"title", "detail", "trace"})
    protected static class HttpError extends RuntimeException {
        public String title;
        public String detail;
        public String trace;

        @JsonCreator
        protected HttpError(
            @JsonProperty("title") String title,
            @JsonProperty("detail") String detail,
            @JsonProperty("trace") String trace
        ) {
            super();
            this.title = title;
            this.detail = detail;
            this.trace = trace;
        }

        protected HttpError(String title, String detail) {
            this(title, detail, null);
        }

        @Override
        public String getMessage() {
            String msg = "Title: " + this.title;
            msg += "\nDetail: " + this.detail;
            if (this.trace != null) {
                msg += "\nTrace: " + this.trace;
            }
            return msg;
        }
    }

    protected static HttpError fromException(Exception e) {
        if (e instanceof HttpError) {
            return (HttpError) e;
        }
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        e.printStackTrace(pw);
        return new HttpError(e.getClass().getTypeName(), e.getMessage(), sw.toString());
    }

    protected static class BadRequest extends Exception {
        protected BadRequest(String message, Throwable cause) {
            super(message, cause);
        }
    }

    protected static <T> T parseContext(Context context, Class<T> clazz) throws BadRequest {
        try {
            return context.extract(clazz);
        } catch (IOException e) {
            throw new BadRequest("Failed to parse request", e);
        }
    }
}
