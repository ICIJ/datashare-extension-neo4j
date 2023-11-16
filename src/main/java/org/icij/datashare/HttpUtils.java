package org.icij.datashare;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonIncludeProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.TypeFactory;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import net.codestory.http.Context;

public class HttpUtils {

    private static final ObjectMapper EXT_MAPPER;

    static {
        EXT_MAPPER = new ObjectMapper().setTypeFactory(
            TypeFactory.defaultInstance().withClassLoader(Neo4jResource.class.getClassLoader())
        );
    }

    // Follow the JSON error + detail spec https://datatracker.ietf.org/doc/html/rfc9457
    @JsonIncludeProperties({"title", "detail", "trace"})
    @JsonIgnoreProperties(value = {"trace"})
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
            return this.title + "\nDetail: " + this.detail;
        }

        protected String getMessageWithTrace() {
            String msg = this.getMessage();
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

    protected static class JacksonParseError extends RuntimeException {
        protected JacksonParseError(String message, Throwable cause) {
            super(message, cause);
        }
    }

    protected static <T> T parseRequestContent(String content, Class<T> clazz)
        throws JacksonParseError {
        try {
            return EXT_MAPPER.readValue(content, clazz);
        } catch (IOException | IllegalArgumentException e) {
            throw new JacksonParseError("Failed to parse request", e);
        }
    }
}
