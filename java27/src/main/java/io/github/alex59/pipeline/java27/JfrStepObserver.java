package io.github.alex59.pipeline.java27;

import io.github.alex59.pipeline.StepEvent;
import io.github.alex59.pipeline.StepObserver;
import java.util.Objects;
import jdk.jfr.Category;
import jdk.jfr.Description;
import jdk.jfr.Enabled;
import jdk.jfr.Event;
import jdk.jfr.Label;
import jdk.jfr.Name;
import jdk.jfr.StackTrace;
import jdk.jfr.Timespan;

/**
 * Optional Java 27 diagnostics profile recording step metadata into an
 * application-owned JFR recording. It neither starts a recording nor changes VM
 * options. Input/output values and exception messages are never copied.
 * Java 27 startup redaction does not redact these custom event fields; use
 * static, non-sensitive step names.
 */
public final class JfrStepObserver implements StepObserver {
    /** Event name to enable in the application's Recording or JFR configuration. */
    public static final String EVENT_NAME = "io.github.alex59.pipeline.Step";

    /** Creates a stateless observer; recording lifecycle remains application-owned. */
    public JfrStepObserver() { }

    /**
     * Emits one custom event if this event type is enabled in a running recording.
     * Each call creates an independent event and can run concurrently.
     * @param source step metadata; payloads are absent from this type
     */
    @Override
    public void onEvent(StepEvent source) {
        Objects.requireNonNull(source, "source");
        StepJfrEvent event = new StepJfrEvent();
        if (!event.shouldCommit()) return;
        event.executionId = source.executionId().toString();
        event.step = source.step();
        event.kind = source.kind().name();
        event.attempt = source.attempt();
        event.elapsedNanos = source.elapsed().toNanos();
        event.errorType = source.error() == null ? "" : source.error().getClass().getName();
        event.commit();
    }

    @Name(EVENT_NAME)
    @Label("Reactive pipeline step")
    @Category("Reactive Pipeline")
    @Description("Step lifecycle metadata without business payloads or exception messages")
    @Enabled(false)
    @StackTrace(false)
    private static final class StepJfrEvent extends Event {
        @Label("Execution ID") String executionId;
        @Label("Step") String step;
        @Label("Lifecycle kind") String kind;
        @Label("Attempt") int attempt;
        @Label("Elapsed time") @Timespan(Timespan.NANOSECONDS) long elapsedNanos;
        @Label("Error class") String errorType;
    }
}
