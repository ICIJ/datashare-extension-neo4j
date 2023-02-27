package org.icij.datashare;

import ch.qos.logback.classic.Level;
import org.graylog2.syslog4j.Syslog;
import org.graylog2.syslog4j.SyslogIF;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.*;

import static org.fest.assertions.Assertions.assertThat;
import static org.icij.datashare.TestUtils.delayedAssert;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

public class LoggingUtilsTest {
    private static final SyslogIF syslogClient = Syslog.getInstance("udp");
    private static final LoggingUtils.SyslogServerSingleton syslogServer = LoggingUtils.SyslogServerSingleton.getInstance();

    public static class SyslogExtension implements BeforeAllCallback, AfterAllCallback, BeforeEachCallback {
        @Override
        public void beforeAll(ExtensionContext extensionContext) {
            syslogServer.run();
        }

        @Override
        public void afterAll(ExtensionContext extensionContext) {
            // This will disable logging everywhere...
            syslogServer.close();
        }

        @Override
        public void beforeEach(ExtensionContext extensionContext) {
            syslogServer.removeAllEventHandlers();
        }
    }

    @ExtendWith(SyslogExtension.class)
    @ExtendWith(TestUtils.LogCaptureExtension.class)
    @DisplayName("SyslogMessageHandler server test")
    @Nested
    class SyslogMessageHandlerTest implements TestUtils.TestWithLogCapture {
        public final  TestUtils.LogCapture logCapture = new TestUtils.LogCapture();

        @Test
        public void test_syslog_handler_should_log() throws InterruptedException {
            // Given
            String facility = "LOCAL7";
            syslogClient.getConfig().setFacility(facility);
            String splitChar = ">";
            syslogServer.addHandler(new LoggingUtils.SyslogMessageHandler(Neo4jResource.class.getName(), facility, splitChar));

            // When
            syslogClient.info("neo4j_app.module_0>Log number one");
            syslogClient.debug("neo4j_app.module_1>This helps me debugging the Python app");
            syslogClient.warn("neo4j_app.module_2>This is a warning\nwritten on several lines");

            // Then
            delayedAssert(500, () -> {
                assertThat(logCapture.countLogs()).isEqualTo(3);
                assertThat(logCapture.containsLog(
                        "org.icij.datashare.neo4j_app.module_0",
                        Level.INFO,
                        "Log number one")
                );
                assertThat(logCapture.containsLog(
                        "org.icij.datashare.neo4j_app.module_1",
                        Level.DEBUG,
                        "This helps me debugging the Python app")
                );
                assertThat(logCapture.containsLog(
                        "org.icij.datashare.neo4j_app.module_3",
                        Level.WARN,
                        "This is a warning\nwritten on several lines")
                );
            });
        }

        @Test
        public void test_syslog_handler_should_filter_invalid_message() throws InterruptedException {
            // Given
            String facility = "LOCAL7";
            syslogClient.getConfig().setFacility(facility);
            String splitChar = ">";
            syslogServer.addHandler(new LoggingUtils.SyslogMessageHandler(Neo4jResource.class.getName(), facility, splitChar));

            // When
            syslogClient.info("neo4j_app.module_0Logwithoutsplitchar");

            // Then
            delayedAssert(1000, () -> assertThat(logCapture.countLogs()).isEqualTo(0));
        }

        @Test
        public void test_syslog_handler_should_use_facility() throws InterruptedException {
            // Given
            String facility0 = "LOCAL6";
            String facility1 = "LOCAL7";
            String splitChar = ">";
            syslogServer.addHandler(new LoggingUtils.SyslogMessageHandler(Neo4jResource.class.getName(), facility0, splitChar));

            // When
            syslogClient.getConfig().setFacility(facility0);
            syslogClient.info("neo4j_app.module>notfiltered");
            syslogClient.getConfig().setFacility(facility1);
            syslogClient.info("neo4j_app.module>filtered");

            // Then
            delayedAssert(1000, () -> {
                assertThat(logCapture.countLogs()).isEqualTo(1);
                assertThat(logCapture.containsLog(
                        "org.icij.datashare.neo4j_app.module", Level.INFO, "notfiltered")
                );
            });
        }

        @Override
        public TestUtils.LogCapture logCapture() {
            return logCapture;
        }
    }

    @Test
    public void test_syslog_handler_should_raise_for_invalid_facility() {
        // Given
        String facility = "thisistotalyunknown";
        String splitChar = ">";

        // When/Then
        assertThrowsExactly(
                IllegalArgumentException.class,
                () -> new LoggingUtils.SyslogMessageHandler(Neo4jResource.class.getName(), facility, splitChar),
                "Invalid facility \"thisistotalyunknown\""
        );
    }

    @Test
    public void test_syslog_handler_should_for_non_local_facility() {
        String facility = "USER";
        String splitChar = ">";

        // When/Then
        assertThrowsExactly(
                IllegalArgumentException.class,
                () -> new LoggingUtils.SyslogMessageHandler(Neo4jResource.class.getName(), facility, splitChar),
                "Expected local facility, found \"USER\""
        );
    }

    @Test
    public void test_syslog_handler_should_raise_for_invalid_split_char() {
        // Given
        String facility = "LOCAL7";
        String splitChar = ">>";

        // When/Then
        assertThrowsExactly(
                IllegalArgumentException.class,
                () -> new LoggingUtils.SyslogMessageHandler(Neo4jResource.class.getName(), facility, splitChar),
                "Expected splitChar to be of length 1 in order to reduce overhead, found \">>\""
        );
    }
}
