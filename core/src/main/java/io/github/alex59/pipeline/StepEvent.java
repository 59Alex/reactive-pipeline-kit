package io.github.alex59.pipeline;

import java.time.Duration;
import java.util.UUID;

/**
 * A diagnostic event; input and output payloads are deliberately excluded.
 * @param executionId input execution identity
 * @param step stable step name
 * @param attempt one-based attempt number, or zero for a skipped condition
 * @param kind lifecycle signal
 * @param elapsed monotonic elapsed time since this attempt started
 * @param error original error for FAILED, otherwise null
 */
public record StepEvent(UUID executionId, String step, int attempt, Kind kind,
                        Duration elapsed, Throwable error) {
    /** Lifecycle of an attempt, or a conditional skip. */
    public enum Kind {
        /** The attempt has subscribed. */ STARTED,
        /** The attempt produced its result. */ SUCCEEDED,
        /** The attempt failed (it may subsequently be retried). */ FAILED,
        /** Downstream cancellation reached this attempt. */ CANCELLED,
        /** The step's predicate was false and its input was passed through. */ SKIPPED
    }
}
