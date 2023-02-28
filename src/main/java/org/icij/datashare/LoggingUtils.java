package org.icij.datashare;

import java.net.SocketAddress;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Optional;
import java.util.concurrent.Callable;
import org.graylog2.syslog4j.server.SyslogServer;
import org.graylog2.syslog4j.server.SyslogServerEventIF;
import org.graylog2.syslog4j.server.SyslogServerIF;
import org.graylog2.syslog4j.server.SyslogServerSessionEventHandlerIF;
import org.graylog2.syslog4j.server.impl.net.udp.UDPNetSyslogServerConfig;
import org.graylog2.syslog4j.util.SyslogUtility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

public class LoggingUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(LoggingUtils.class);

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

    public enum SyslogServerSingleton implements AutoCloseable {
        SERVER_INSTANCE;
        private final SyslogServerIF syslogServer;
        private final HashSet<SyslogServerSessionEventHandlerIF> handlers = new HashSet<>();


        SyslogServerSingleton() {
            UDPNetSyslogServerConfig config = new UDPNetSyslogServerConfig();
            config.setUseStructuredData(true);
            syslogServer = SyslogServer.getInstance("udp");
            syslogServer.initialize("udp", config);
            syslogServer.setThread(new Thread(syslogServer));
        }

        public static SyslogServerSingleton getInstance() {
            return SERVER_INSTANCE;
        }

        // TODO: support handlers removal
        public void addHandler(SyslogMessageHandler handler) {
            synchronized (handlers) {
                if (!handlers.contains(handler)) {
                    LOGGER.debug("Adding handler " + handler + " to syslog server ");
                    handlers.add(handler);
                    syslogServer.getConfig().addEventHandler(handler);
                }
            }
        }

        public void removeAllEventHandlers() {
            synchronized (handlers) {
                handlers.clear();
                syslogServer.getConfig().removeAllEventHandlers();
            }
        }

        public void run() {
            synchronized (syslogServer) {
                // TODO: support restart by creating a new Thread once the instance has been closed
                if (syslogServer.getThread().getState() == Thread.State.NEW) {
                    syslogServer.getThread().start();
                }
            }
        }

        @Override
        public void close() {
            // Note: this will close the server for all clients... this method is meant to be
            // called when the app closes
            syslogServer.getThread().interrupt();
            syslogServer.shutdown();
        }

    }

    public static class SyslogMessageHandler implements SyslogServerSessionEventHandlerIF {
        private final int facility;
        private final String splitChar;
        private final String baseLoggerName;

        public SyslogMessageHandler(String baseLoggerName, String facility, String splitChar) {
            if (!facility.toLowerCase().startsWith("local")) {
                throw new IllegalArgumentException(
                    "Expected local facility, found \"" + facility + "\"");
            }
            int facilityAsInt = SyslogUtility.getFacility(facility);
            if (facilityAsInt < 0) {
                throw new IllegalArgumentException("Invalid facility \"" + facility + "\"");
            }
            this.facility = facilityAsInt;
            if (splitChar.length() > 1) {
                throw new IllegalArgumentException(
                    "Expected splitChar to be of length 1 in order to reduce overhead, found \""
                        + splitChar
                        + "\"");
            }
            this.baseLoggerName = baseLoggerName;
            this.splitChar = splitChar;
        }

        @Override
        public void event(Object session, SyslogServerIF syslogServer, SocketAddress socketAddress,
                          SyslogServerEventIF event) {
            this.parseMessage(event).ifPresent(syslogMsg -> {
                String loggerName = baseLoggerName + "." + syslogMsg.loggerName;
                LoggerFactory.getLogger(loggerName)
                    .atLevel(Level.valueOf(SyslogUtility.getLevelString(event.getLevel())))
                    .log(syslogMsg.content);
            });
        }

        @Override
        public void exception(Object session, SyslogServerIF syslogServer,
                              SocketAddress socketAddress, Exception exception) {
            LOGGER.error("Exception thrown while reading from syslog handler: {}",
                lazy(exception::getMessage));
        }

        @Override
        public Object sessionOpened(SyslogServerIF syslogServer, SocketAddress socketAddress) {
            LOGGER.trace("Starting syslog handler session, listening on {}", socketAddress);
            return new Date();
        }

        @Override
        public void sessionClosed(Object session, SyslogServerIF syslogServer,
                                  SocketAddress socketAddress, boolean timeout) {
            LOGGER.trace("Closing syslog handler session {}", session);
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
            // TODO: improve parsing by defining a proper structure
            int eventFacility = event.getFacility() << 3;
            if (eventFacility != this.facility) {
                return Optional.empty();
            }
            String message = event.getMessage();
            // We use a lightweight split to avoid overhead
            String[] split = message.split(this.splitChar);
            if (split.length >= 2) {
                String loggerName = split[0];
                Iterable<String> stringIt = () -> Arrays.stream(split).skip(1).iterator();
                String content = String.join(this.splitChar, stringIt);
                return Optional.of(new SyslogMessage(loggerName, content));
            }
            return Optional.empty();
        }

        @Override
        public String toString() {
            return this.getClass().getName()
                + "(facility="
                + this.facility
                + ", splitChar=\""
                + this.splitChar
                + "\")";
        }

        private static class SyslogMessage {
            public String loggerName;
            public String content;

            public SyslogMessage(String loggerName, String content) {
                this.loggerName = loggerName;
                this.content = content;
            }
        }

    }
}
