package io.github.alex59.pipeline.java27;

import java.nio.file.Path;
import jdk.jfr.Recording;

/** Child-process fixture; launched with cleared environment and synthetic properties only. */
public final class StartupRedactionFixture {
    private StartupRedactionFixture() { }

    public static void main(String[] args) throws Exception {
        if (Runtime.version().feature() != 27) throw new IllegalStateException("Fixture requires JDK 27");
        try (Recording recording = new Recording()) {
            recording.enable("jdk.InitialSystemProperty");
            recording.disable("jdk.InitialEnvironmentVariable");
            recording.start();
            recording.stop();
            recording.dump(Path.of(args[0]));
        }
    }
}
