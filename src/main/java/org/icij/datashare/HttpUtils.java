package org.icij.datashare;

public class HttpUtils {

    // Follow the JSON error + detail spec https://datatracker.ietf.org/doc/html/draft-ietf-appsawg-http-problem-00#page-4
    public static class HttpError {
        public String detail;

        public HttpError withDetail(String detail) {
            this.detail = detail;
            return this;
        }

    }
}
