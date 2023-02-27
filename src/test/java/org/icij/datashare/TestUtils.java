package org.icij.datashare;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.LoggerFactory;

import static ch.qos.logback.classic.Level.toLocationAwareLoggerInteger;
import static org.fest.assertions.Fail.fail;
import static org.icij.datashare.json.JsonObjectMapper.MAPPER;

public class TestUtils {

    public static <T> void assertJson(String json, Class<T> cls, AssertionChain<T> assertionChain) {
        try {
            T object = MAPPER.readValue(json, cls);
            assertionChain.doAssert(object);
        } catch (JsonProcessingException e) {
            String msg = "Failed to convert the json string into a valid " + cls.getName() + ":";
            msg += "\nJSON string: " + json;
            msg += "\nerror: " + e;
            fail(msg);
        }
    }

    public static void delayedAssert(long millis, Runnable assertions) throws InterruptedException {
        Thread.sleep(millis);
        try {
            assertions.run();
        } catch (AssertionError e) {
            throw new AssertionError(
                    "Assertion failed after waiting " + millis + "ms. Fix the test or wait for a little longer !",
                    e
            );
        }
    }

    public interface AssertionChain<T> {
        void doAssert(T obj) throws AssertionError;
    }

    public interface TestWithLogCapture {
        LogCapture logCapture();
    }

    public static class LogCaptureExtension implements BeforeEachCallback {
        @Override
        public void beforeEach(ExtensionContext extensionContext) {
            // TODO: improve this to retrieve the parent capture in case of nested tests
            LogCapture capture = extensionContext.getTestInstance().map(test -> ((TestWithLogCapture) test).logCapture()).orElseThrow();
            capture.clear();
            Logger logger = (Logger) LoggerFactory.getLogger(Neo4jResource.class.getName());
            capture.setContext((LoggerContext) LoggerFactory.getILoggerFactory());
            logger.setLevel(Level.DEBUG);
            logger.addAppender(capture);
            capture.start();
        }
    }

    public static class LogCapture extends ListAppender<ILoggingEvent> {
        public boolean containsLog(String loggerName, Level level, String string) {
            return this.list
                    .stream()
                    .anyMatch(event -> event.getLoggerName().equals(loggerName)
                            && event.toString().equals(string)
                            && toLocationAwareLoggerInteger(event.getLevel()) == level.toInt()
                    );
        }

        public int countLogs() {
            return this.list.size();
        }

        public void clear() {
            this.list.clear();
        }
    }


    public static String makeJsonHttpError(String title, String detail) {
        return "{\"title\": \"" + title + "\", " + "\"detail\": \"" + detail + "\"}";
    }

    public static String makeJsonHttpError(String title, String detail, String trace) {
        return "{\"title\": \"" + title + "\", " + "\"detail\": \"" + detail + "\", \"trace\": \"" + trace + "\"}";
    }

}
