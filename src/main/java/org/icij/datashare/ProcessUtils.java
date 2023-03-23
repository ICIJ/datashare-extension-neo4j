package org.icij.datashare;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

public class ProcessUtils {

    public static void dumpPid(File pidFile, Long pid) throws IOException {
        boolean ignored = pidFile.getParentFile().mkdirs();
        try (FileOutputStream fos = new FileOutputStream(pidFile)) {
            fos.write(pid.toString().getBytes(StandardCharsets.UTF_8));
        }
    }

    public static Long isProcessRunning(Path pidPath, int timeout, TimeUnit timeunit)
        throws IOException, InterruptedException {
        try (Stream<String> lines = Files.lines(pidPath)) {
            Long pid = Long.parseLong(
                lines.findFirst()
                    .orElseThrow(() -> new RuntimeException("PID file is empty"))
                    .strip()
            );
            if (isProcessRunning(pid, timeout, timeunit)) {
                return pid;
            }
        }
        return null;
    }

    public static boolean isProcessRunning(Long pid, int timeout, TimeUnit timeunit)
        throws IOException, InterruptedException {
        boolean isWindows = System.getProperty("os.name").toLowerCase().startsWith("windows");
        if (isWindows) {
            throw new RuntimeException("Datashare neo4j extension is not supported on Windows");
        }
        ProcessBuilder builder = new ProcessBuilder();
        builder.command("ps", "-p", pid.toString());
        Process process = builder.start();
        if (process.waitFor(timeout, timeunit)) {
            return process.exitValue() == 0;
        } else {
            throw new RuntimeException(
                "Failed to process "
                    + pid
                    + "status using the ps command in less than "
                    + timeout
                    + timeunit.toString()
            );
        }
    }

    public static void killProcessById(Long pid) {
        killProcessById(pid, false);
    }

    public static void killProcessById(Long pid, boolean force) {
        Stream<ProcessHandle> liveProcesses = ProcessHandle.allProcesses();
        liveProcesses
            .filter(handle -> handle.isAlive() && pid.equals(handle.pid()))
            .findFirst()
            .ifPresent(parent -> parent
                .descendants()
                .forEach(child -> {
                    if (force) {
                        child.destroyForcibly();
                    } else {
                        child.destroy();
                    }
                })
            );
    }
}
