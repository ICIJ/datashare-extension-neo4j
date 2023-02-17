package org.icij.datashare;

import ch.qos.logback.classic.Level;
import org.graylog2.syslog4j.Syslog;
import org.graylog2.syslog4j.SyslogIF;
import org.graylog2.syslog4j.server.SyslogServer;
import org.graylog2.syslog4j.server.SyslogServerIF;
import org.graylog2.syslog4j.server.impl.net.udp.UDPNetSyslogServerConfig;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;

import static org.fest.assertions.Assertions.assertThat;
import static org.icij.datashare.TestUtils.delayedAssert;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

public class LoggingUtilsTest {
    private static Thread syslogServerThread;
    private static final SyslogIF syslogClient = Syslog.getInstance("udp");
    private static final SyslogServerIF syslogServer = SyslogServer.getInstance("udp");

    protected static void startServer(LoggingUtils.SyslogMessageHandler handler) {
        UDPNetSyslogServerConfig config = new UDPNetSyslogServerConfig();
        config.addEventHandler(handler);
        config.setUseStructuredData(true);
        syslogServer.initialize("udp", config);
        syslogServerThread = new Thread(syslogServer);
        syslogServerThread.start();
    }

    public static class SyslogExtension implements AfterEachCallback {
        @Override
        public void afterEach(ExtensionContext extensionContext) {
            if (syslogServer != null) {
                syslogServer.getConfig().removeAllEventHandlers();
                syslogServer.shutdown();
            }
            if (syslogServerThread != null) {
                syslogServerThread.interrupt();
            }
        }
    }

    @ExtendWith(SyslogExtension.class)
    @ExtendWith(TestUtils.LogCaptureExtension.class)
    @DisplayName("SyslogMessageHandler server test")
    public static class SyslogMessageHandlerTest {
        public final static TestUtils.LogCapture logCapture = new TestUtils.LogCapture();

        @Test
        public void test_syslog_handler_should_log() throws InterruptedException {
            // Given
            String facility = "LOCAL7";
            syslogClient.getConfig().setFacility(facility);
            String splitChar = ">";
            LoggingUtils.SyslogMessageHandler handler = new LoggingUtils.SyslogMessageHandler(facility, splitChar);
            startServer(handler);

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
            LoggingUtils.SyslogMessageHandler handler = new LoggingUtils.SyslogMessageHandler(facility, splitChar);
            startServer(handler);

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
            LoggingUtils.SyslogMessageHandler handler = new LoggingUtils.SyslogMessageHandler(facility0, splitChar);
            startServer(handler);

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
    }

    @Test
    public void test_syslog_handler_should_raise_for_invalid_facility() {
        // Given
        String facility = "thisistotalyunknown";
        String splitChar = ">";

        // When/Then
        assertThrowsExactly(
                IllegalArgumentException.class,
                () -> new LoggingUtils.SyslogMessageHandler(facility, splitChar),
                "Invalid facility \"thisistotalyunknown\""
        );
    }

    @Test
    public void test_syslog_handler_should_for_invalid_split_char() {
        // Given
        String facility = "LOCAL7";
        String splitChar = ">>";

        // When/Then
        assertThrowsExactly(
                IllegalArgumentException.class,
                () -> new LoggingUtils.SyslogMessageHandler(facility, splitChar),
                "Expected splitChar to be of length 1 in order to reduce overhead, found \">>\""
        );
    }
}
