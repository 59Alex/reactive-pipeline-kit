import io.github.alex59.pipeline.Pipeline;
import io.github.alex59.pipeline.java27.JfrStepObserver;
import java.nio.file.Files;
import java.nio.file.Path;
import jdk.jfr.Recording;
import jdk.jfr.consumer.RecordingFile;

public final class JfrExample {
    public static void main(String[] args) throws Exception {
        Path destination = Path.of(args.length == 0 ? "pipeline-example.jfr" : args[0]).toAbsolutePath();
        Files.createDirectories(destination.getParent());
        try (Recording recording = new Recording()) {
            recording.enable(JfrStepObserver.EVENT_NAME).withoutStackTrace();
            recording.start();
            Integer result = Pipeline.<Integer>start().map("increment", value -> value + 1)
                .observe(new JfrStepObserver()).run(6).block();
            if (!Integer.valueOf(7).equals(result)) throw new AssertionError(result);
            recording.stop();
            recording.dump(destination);
        }
        long count = RecordingFile.readAllEvents(destination).stream()
            .filter(event -> event.getEventType().getName().equals(JfrStepObserver.EVENT_NAME)).count();
        if (count != 2) throw new AssertionError("expected 2 step events, got " + count);
        System.out.println("jfrEvents=" + count);
    }
}
