package org.icij.datashare;

import org.graylog2.syslog4j.server.SyslogServerEventIF;
import org.graylog2.syslog4j.server.SyslogServerIF;
import org.graylog2.syslog4j.server.SyslogServerSessionEventHandlerIF;
import org.graylog2.syslog4j.util.SyslogUtility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import java.net.SocketAddress;
import java.util.Arrays;
import java.util.Date;
import java.util.Optional;
import java.util.concurrent.Callable;

public class LoggingUtils {

    public static final String PACKAGE_NAME = LoggingUtils.class.getPackage().getName();

    public static class SyslogMessageHandler implements SyslogServerSessionEventHandlerIF {
        private final int facility;
        private final String splitChar;
        private static final Logger LOGGER = LoggerFactory.getLogger(Neo4jClient.class);

        private static class SyslogMessage {
            public String loggerName;
            public String content;

            public SyslogMessage(String loggerName, String content) {
                this.loggerName = loggerName;
                this.content = content;
            }
        }

        public SyslogMessageHandler(String facility, String splitChar) {
            int facilityAsInt = SyslogUtility.getFacility(facility);
            if (facilityAsInt < 0) {
                throw new IllegalArgumentException("Invalid facility \"" + facility + "\"");
            }
            this.facility = facilityAsInt;
            if (splitChar.length() > 1) {
                throw new IllegalArgumentException("Expected splitChar to be of length 1 in order to reduce overhead, found \"" + splitChar + "\"");
            }
            this.splitChar = splitChar;
        }

        @Override
        public void event(Object session, SyslogServerIF syslogServer, SocketAddress socketAddress, SyslogServerEventIF event) {
            this.parseMessage(event).ifPresent(syslogMsg -> {
                String loggerName = PACKAGE_NAME + "." + syslogMsg.loggerName;
                LoggerFactory.getLogger(loggerName)
                        .atLevel(Level.valueOf(SyslogUtility.getLevelString(event.getLevel())))
                        .log(syslogMsg.content);
            });
        }

        @Override
        public void exception(Object session, SyslogServerIF syslogServer, SocketAddress socketAddress, Exception exception) {
            LOGGER.info("Exception thrown while reading from syslog handler: {}", lazy(exception::getMessage));
        }

        @Override
        public Object sessionOpened(SyslogServerIF syslogServer, SocketAddress socketAddress) {
            LOGGER.info("Starting syslog handler session, listening on {}", socketAddress);
            return new Date();
        }

        @Override
        public void sessionClosed(Object session, SyslogServerIF syslogServer, SocketAddress socketAddress, boolean timeout) {
            LOGGER.info("Closing syslog handler session {}", session);
        }

        @Override
        public void initialize(SyslogServerIF syslogServer) {
            LOGGER.trace("Initialize syslog handler");
        }

        @Override
        public void destroy(SyslogServerIF syslogServer) {
            LOGGER.trace("Destroying syslog handler");
        }

        protected Optional<SyslogMessage> parseMessage(SyslogServerEventIF event) {
            // TODO: we could improve parsing by defining a proper structure
            int eventFacility = event.getFacility() << 3;
            if (eventFacility != this.facility) {
                return Optional.empty();
            }
            String message = event.getMessage();
            // We use a lightweight split to avoid overhead
            String[] split = message.split(this.splitChar);
            if (split.length >= 2) {
                String loggerName = split[1];
                Iterable<String> stringIt = () -> Arrays.stream(split).skip(1).iterator();
                String content = String.join(this.splitChar, stringIt);
                return Optional.of(new SyslogMessage(loggerName, content));
            }
            return Optional.empty();
        }

    }

    static Object lazy(Callable<?> callable) {
        return new Object() {
            @Override
            public String toString() {
                try {
                    Object result = callable.call();
                    if (result == null) {
                        return "null";
                    }

                    return result.toString();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }
}
