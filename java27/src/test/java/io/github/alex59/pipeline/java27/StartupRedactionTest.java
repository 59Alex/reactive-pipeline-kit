package io.github.alex59.pipeline.java27;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import jdk.jfr.consumer.RecordingFile;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

@Timeout(40)
class StartupRedactionTest {
    @TempDir Path directory;

    @Test
    void java27DefaultStartupRedactionHidesSensitiveKeysButKeepsSafeValue() throws Exception {
        Map<String, String> properties = recordInIsolatedChild("default", List.of());
        assertEquals("[REDACTED]", properties.get("fixture.token"));
        assertEquals("[REDACTED]", properties.get("fixture.password"));
        assertEquals("synthetic-visible", properties.get("fixture.safe"));
        assertEquals("synthetic-custom", properties.get("fixture.confidential"));
    }

    @Test
    void appendedStartupFilterRedactsCustomKeyWithoutReplacingDefaults() throws Exception {
        Map<String, String> properties = recordInIsolatedChild("extended",
                List.of("-XX:FlightRecorderOptions:redact-key=+fixture.confidential"));
        assertEquals("[REDACTED]", properties.get("fixture.confidential"));
        assertEquals("[REDACTED]", properties.get("fixture.token"));
        assertEquals("[REDACTED]", properties.get("fixture.password"));
        assertEquals("synthetic-visible", properties.get("fixture.safe"));
    }

    private Map<String, String> recordInIsolatedChild(String name, List<String> startupOptions) throws Exception {
        Path file = directory.resolve(name + ".jfr");
        Path log = directory.resolve(name + ".log");
        Path java = Path.of(System.getProperty("java.home"), "bin", "java");
        Path fixtureClasses = Path.of(StartupRedactionFixture.class.getProtectionDomain().getCodeSource().getLocation().toURI());
        List<String> command = new ArrayList<>();
        command.add(java.toString());
        command.addAll(startupOptions);
        command.addAll(List.of("-Dfixture.token=synthetic-token", "-Dfixture.password=synthetic-password",
                "-Dfixture.safe=synthetic-visible", "-Dfixture.confidential=synthetic-custom",
                "-cp", fixtureClasses.toString(), StartupRedactionFixture.class.getName(), file.toString()));
        ProcessBuilder builder = new ProcessBuilder(command).directory(directory.toFile());
        builder.environment().clear();
        builder.redirectErrorStream(true).redirectOutput(log.toFile());
        Process process = builder.start();
        try {
            assertTrue(process.waitFor(20, TimeUnit.SECONDS), "Fixture JVM did not exit");
            assertEquals(0, process.exitValue(), () -> readLog(log));
        } finally {
            if (process.isAlive()) {
                process.destroyForcibly();
                assertTrue(process.waitFor(5, TimeUnit.SECONDS), "Fixture JVM did not terminate");
            }
        }
        Map<String, String> properties = new HashMap<>();
        for (var event : RecordingFile.readAllEvents(file)) {
            assertNotEquals("jdk.InitialEnvironmentVariable", event.getEventType().getName());
            if (event.getEventType().getName().equals("jdk.InitialSystemProperty")) {
                String key = event.getString("key");
                if (key.startsWith("fixture.")) properties.put(key, event.getString("value"));
            }
        }
        assertEquals(4, properties.size(), "Expected each synthetic startup property to be recorded");
        return properties;
    }

    private static String readLog(Path log) {
        try { return Files.readString(log); }
        catch (Exception failure) { return "Unable to read fixture log: " + failure.getClass().getSimpleName(); }
    }
}
