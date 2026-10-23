package io.github.alex59.pipeline;

/**
 * Nonblocking, thread-safe diagnostics sink. Observer failures are sent to
 * Reactor's onErrorDropped hook and never trigger a business-operation retry.
 * If that hook also throws a nonfatal exception, it is discarded without calling
 * the hook again. Reactor-fatal errors are rethrown, not treated as diagnostics.
 */
@FunctionalInterface
public interface StepObserver {
    /** An observer with no side effects. */
    StepObserver NONE = event -> { };

    /**
     * Receives an event on the thread delivering the corresponding signal.
     * @param event lifecycle metadata without payloads
     */
    void onEvent(StepEvent event);
}
