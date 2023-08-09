package org.icij.datashare;

import static org.fest.assertions.Assertions.assertThat;
import static org.icij.datashare.Neo4jAppLoader.getExtensionVersion;
import static org.icij.datashare.Neo4jAppLoader.parseManifest;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.common.hash.Hashing;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.io.TempDir;

public class Neo4jAppLoaderTest {
    private final static String mockedBinaryName = "neo4j-app";
    @TempDir
    private static Path tmpDir;
    private static String mockedBinaryPath;
    private static Path mockedManifestPath;

    private final Neo4jAppLoader loader = new Neo4jAppLoader(
        new PropertiesProvider(new HashMap<>() {
        })
    );

    @Test
    public void test_download_app() throws IOException {
        // Given
        String mockedExtensionVersion = "0.1.0rc0";

        // When
        File binFile = loader.downloadApp(mockedExtensionVersion);

        // Then
        assertThat(binFile.exists());
    }

    @Test
    public void test_parse_manifest() throws IOException {
        // Given
        Path manifestPath = Paths.get("src", "main", "resources", "manifest.txt");

        // When
        try (InputStream manifestInputStream = Files.newInputStream(manifestPath)) {
            String hash = parseManifest(
                manifestInputStream, "neo4j-app-darwin-universal2-0.1.0rc0");
            // Then
            assertThat(hash).isEqualTo(
                "ddbfda6bba1a97c08d3d68ec631836314c4f5136ea9df9a4dd5b2bc73242a3cd");
        }
    }

    @Test
    public void test_parse_manifest_should_raise_for_invalid_binary() throws IOException {
        // Given
        Path manifestPath = Paths.get("src", "main", "resources", "manifest.txt");

        // When/Then
        try (InputStream manifestInputStream = Files.newInputStream(manifestPath)) {
            String msg = assertThrows(
                RuntimeException.class,
                () -> parseManifest(manifestInputStream, "invalid_binary")
            ).getMessage();
            assertThat(msg).isEqualTo("Couldn't not find hash for invalid_binary in manifest.txt");
        }
    }

    @Test
    public void test_get_extension_version() {
        // When
        String version = getExtensionVersion();
        // Then
        assertThat(version).isNotNull();
    }


    public static class MockManifestExtension implements BeforeEachCallback {
        @Override
        public void beforeEach(ExtensionContext extensionContext) throws IOException {
            byte[] content = "somebinarycontent".getBytes();
            String mockedHash = Hashing.sha256().hashBytes(content).toString();
            mockedManifestPath = tmpDir.resolve("manifest.txt").toAbsolutePath();
            try (OutputStream f = Files.newOutputStream(mockedManifestPath)) {
                f.write((mockedHash + " " + "bins/" + mockedBinaryName).getBytes());
            }
            mockedBinaryPath = tmpDir.resolve(mockedBinaryName).toAbsolutePath().toString();
            try (OutputStream f = Files.newOutputStream(Path.of(mockedBinaryPath))) {
                f.write(content);
            }
        }
    }

    @ExtendWith(MockManifestExtension.class)
    @Nested
    class Neo4jAppLoaderMockedBinaryTest {
        @Test
        public void test_verify_app_binary() throws IOException {
            // Given
            try (
                InputStream mockedManifestInputStream = Files.newInputStream(mockedManifestPath)) {

                // When/Then
                Neo4jAppLoader.verifyNeo4jAppBinary(
                    mockedManifestInputStream, mockedBinaryName, new File(mockedBinaryPath)
                );
            }
        }

        @Test
        public void test_verify_app_binary_should_raise_when_binary_is_tempered()
            throws IOException {
            // When
            try (FileOutputStream fos = new FileOutputStream(mockedBinaryPath, true)) {
                fos.write("some additional data".getBytes());
            }
            try (InputStream mockedManifestInputStream = Files.newInputStream(mockedManifestPath)) {
                // Then
                String expectedMsg = "Expected a SHA-256 [a-z0-9]+ for binary neo4j-app, "
                    + "found [a-z0-9]+, asset has probably been tempered";
                File binFile = new File(mockedBinaryPath);
                String msg = assertThrows(
                    RuntimeException.class,
                    () -> Neo4jAppLoader.verifyNeo4jAppBinary(
                        mockedManifestInputStream, mockedBinaryName, binFile)
                ).getMessage();
                assertThat(msg).matches(expectedMsg);
            }
        }
    }

}
