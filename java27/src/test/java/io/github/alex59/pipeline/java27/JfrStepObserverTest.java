package io.github.alex59.pipeline.java27;

import static org.junit.jupiter.api.Assertions.*;

import io.github.alex59.pipeline.Pipeline;
import io.github.alex59.pipeline.StepEvent;
import io.github.alex59.pipeline.StepFailure;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import jdk.jfr.Recording;
import jdk.jfr.RecordingState;
import jdk.jfr.consumer.RecordedEvent;
import jdk.jfr.consumer.RecordingFile;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

class JfrStepObserverTest {
    @TempDir Path directory;

    @Test
    void actualRecordingContainsLifecycleMetadataWithoutPayloadOrExceptionMessage() throws Exception {
        Path file = directory.resolve("pipeline.jfr");
        String payload = "synthetic-input-not-for-recording";
        String errorMessage = "synthetic-message-not-for-recording";
        try (Recording recording = new Recording()) {
            recording.enable(JfrStepObserver.EVENT_NAME).withThreshold(Duration.ZERO).withoutStackTrace();
            recording.start();
            var observer = new JfrStepObserver();
            assertEquals(7, Pipeline.<Integer>start().map("increment", input -> input + 1)
                    .observe(observer).run(6).block());
            var failing = Pipeline.<String>start().map("validate", input -> {
                throw new IllegalArgumentException(errorMessage);
            }).observe(observer);
            assertThrows(StepFailure.class, () -> failing.run(payload).block());
            assertEquals(RecordingState.RUNNING, recording.getState());
            recording.stop();
            recording.dump(file);
        }

        var events = RecordingFile.readAllEvents(file).stream()
                .filter(event -> event.getEventType().getName().equals(JfrStepObserver.EVENT_NAME)).toList();
        assertEquals(4, events.size());
        assertEquals(Set.of("STARTED", "SUCCEEDED", "FAILED"), events.stream().map(e -> e.getString("kind")).collect(Collectors.toSet()));
        RecordedEvent failure = events.stream().filter(e -> e.getString("kind").equals("FAILED")).findFirst().orElseThrow();
        assertEquals("validate", failure.getString("step"));
        assertEquals("java.lang.IllegalArgumentException", failure.getString("errorType"));
        assertEquals(1, failure.getInt("attempt"));
        assertTrue(failure.getLong("elapsedNanos") >= 0);
        assertDoesNotThrow(() -> UUID.fromString(failure.getString("executionId")));

        Set<String> customFields = failure.getEventType().getFields().stream().map(field -> field.getName())
                .filter(name -> !Set.of("startTime", "duration", "eventThread", "stackTrace").contains(name))
                .collect(Collectors.toSet());
        assertEquals(Set.of("executionId", "step", "kind", "attempt", "elapsedNanos", "errorType"), customFields);
        assertTrue(events.stream().allMatch(event -> event.getStackTrace() == null));
        assertTrue(events.stream().noneMatch(event -> event.toString().contains(payload) || event.toString().contains(errorMessage)));
    }

    @Test
    void disabledCustomEventEmitsNothingAndObserverDoesNotStartRecording() throws Exception {
        Path file = directory.resolve("disabled.jfr");
        var observer = new JfrStepObserver();
        try (Recording recording = new Recording()) {
            recording.disable(JfrStepObserver.EVENT_NAME);
            assertEquals(RecordingState.NEW, recording.getState());
            observer.onEvent(new StepEvent(UUID.randomUUID(), "before", 1, StepEvent.Kind.STARTED, Duration.ZERO, null));
            assertEquals(RecordingState.NEW, recording.getState());
            recording.start();
            observer.onEvent(new StepEvent(UUID.randomUUID(), "disabled", 1, StepEvent.Kind.SUCCEEDED, Duration.ofNanos(3), null));
            recording.stop();
            recording.dump(file);
        }
        assertTrue(RecordingFile.readAllEvents(file).stream()
                .noneMatch(event -> event.getEventType().getName().equals(JfrStepObserver.EVENT_NAME)));
    }

    @Test
    void concurrentInvocationsKeepIndependentEventFields() throws Exception {
        Path file = directory.resolve("concurrent.jfr");
        var observer = new JfrStepObserver();
        try (Recording recording = new Recording()) {
            recording.enable(JfrStepObserver.EVENT_NAME).withoutStackTrace();
            recording.start();
            Flux.range(0, 100).parallel(4).runOn(Schedulers.parallel()).doOnNext(index ->
                    observer.onEvent(new StepEvent(new UUID(0, index), "step-" + index, index,
                            StepEvent.Kind.SUCCEEDED, Duration.ofNanos(index), null)))
                    .sequential().blockLast(Duration.ofSeconds(5));
            recording.stop();
            recording.dump(file);
        }
        var events = RecordingFile.readAllEvents(file).stream()
                .filter(event -> event.getEventType().getName().equals(JfrStepObserver.EVENT_NAME)).toList();
        assertEquals(100, events.size());
        for (RecordedEvent event : events) {
            int index = event.getInt("attempt");
            assertEquals("step-" + index, event.getString("step"));
            assertEquals(new UUID(0, index).toString(), event.getString("executionId"));
            assertEquals(index, event.getLong("elapsedNanos"));
            assertEquals("", event.getString("errorType"));
        }
    }
}
