package org.icij.datashare;

import com.google.common.hash.Hashing;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Stream;
import javax.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Neo4jAppLoader {
    public static final String NEO4J_APP_BIN = "neo4j-app";
    protected static final String MANIFEST_NAME = "manifest.txt";

    protected static final Logger logger = LoggerFactory.getLogger(Neo4jAppLoader.class);
    private static final String RELEASE_BASE_URL =
        "https://github.com/ICIJ/datashare-extension-neo4j/releases/download";
    private static final String OS = System.getProperty("os.name").toLowerCase();
    private static final String ARCH = System.getProperty("os.arch").toLowerCase();
    protected static boolean IS_DARWIN = (OS.contains("mac"));
    protected static boolean IS_UNIX =
        (OS.contains("nix") || OS.contains("nux") || OS.indexOf("aix") > 0);
    protected static boolean IS_X86_64 =
        (ARCH.contains("amd64") || ARCH.contains("x86_64"));
    protected static boolean IS_ARM =
        (ARCH.contains("aarch64") || ARCH.contains("arm64"));

    private final PropertiesProvider propertiesProvider;

    @Inject
    public Neo4jAppLoader(PropertiesProvider propertiesProvider) {
        this.propertiesProvider = propertiesProvider;
    }

    public static String getExtensionVersion() throws IOException {
        Properties neo4jExtensionProps = new Properties();
        InputStream propStream = ClassLoader.getSystemResourceAsStream(
            "datashare-extension-neo4j.properties");
        neo4jExtensionProps.load(propStream);
        return Objects.requireNonNull(neo4jExtensionProps.getProperty("project.version"),
            "Couldn't find project.version in extension properties");
    }

    protected static String getBinaryPrefix() {
        if (IS_UNIX) {
            String arch;
            if (IS_X86_64) {
                arch = "x86_64";
            } else if (IS_ARM) {
                arch = "arm64";
            } else {
                throw new RuntimeException(
                    "Extension is not supported for the "
                        + OS
                        + " Linux architecture, only arm64/aarc64 and x86_64/amd64 are supported"
                );
            }
            return NEO4J_APP_BIN + "-unknown-linux-" + arch;
        } else if (IS_DARWIN) {
            return NEO4J_APP_BIN + "-darwin-universal2";
        } else {
            throw new RuntimeException(
                "Extension is not supported for the " + OS + " operation system"
            );
        }
    }

    protected static String parseManifest(Path manifestPath, String binaryName) throws IOException {
        try (Stream<String> lines = Files.lines(manifestPath)) {
            String hashLine = lines.filter(line -> line.endsWith("bins/" + binaryName))
                .findFirst()
                .orElseThrow(
                    () -> new RuntimeException(
                        "Couldn't not find hash for " + binaryName + " in " + MANIFEST_NAME
                    )
                );
            return Arrays.stream(hashLine.split("\\s")).findFirst()
                .orElseThrow(
                    () -> new RuntimeException("Couldn't not parse manifest line " + hashLine)
                );
        }
    }

    protected static void verifyNeo4jAppBinary(
        Path manifestPath, String binaryName, File binaryFile
    ) throws IOException {
        String expectedHash = parseManifest(manifestPath, binaryName);
        try (InputStream inputStream = new FileInputStream(binaryFile)) {
            String binaryHash = Hashing.sha256().hashBytes(inputStream.readAllBytes()).toString();
            if (!binaryHash.equals(expectedHash)) {
                String msg = "Expected a SHA-256 "
                    + expectedHash
                    + " for binary "
                    + binaryName
                    + ", found "
                    + binaryHash
                    + ", asset has probably been tempered";
                throw new RuntimeException(msg);
            }
        }
    }

    public File downloadApp(String version) throws IOException {
        String binaryName = getBinaryPrefix() + "-" + version;
        String urlAsString = RELEASE_BASE_URL + "/" + version + "/" + binaryName;
        URL assetUrl = new URL(urlAsString);
        ReadableByteChannel readableByteChannel = Channels.newChannel(assetUrl.openStream());
        File binFile = this.propertiesProvider
            .get(PropertiesProvider.EXTENSIONS_DIR)
            .map(dir -> new File(Paths.get(dir, binaryName).toUri()))
            .orElse(Files.createTempDirectory(NEO4J_APP_BIN).resolve(binaryName).toFile());
        if (!binFile.exists()) {
            logger.debug(
                "Downloading python binary from url {} to {}",
                assetUrl,
                binFile.getAbsolutePath()
            );
            try (FileOutputStream fileOutputStream = new FileOutputStream(binFile)) {
                fileOutputStream.getChannel().transferFrom(
                    readableByteChannel, 0, Long.MAX_VALUE
                );
            }
        } else {
            logger.debug("Found existing python binary on the file system");
        }
        Path manifestPath = Path.of(Objects.requireNonNull(
            ClassLoader.getSystemResource(MANIFEST_NAME),
            "Couldn't locate manifest file"
        ).getPath());
        verifyNeo4jAppBinary(manifestPath, binaryName, binFile);
        return binFile;
    }
}
