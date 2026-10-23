package io.github.alex59.pipeline;

/** Final step failure, preserving the last original cause. */
public final class StepFailure extends RuntimeException {
    /** Stable failed-step name. */
    private final String step;
    /** Number of attempts actually started. */
    private final int attempts;

    StepFailure(String step, int attempts, Throwable cause) {
        super("Step '" + step + "' failed after " + attempts + " attempt(s)", cause);
        this.step = step;
        this.attempts = attempts;
    }

    /**
     * Identifies the failed step.
     * @return the stable name of the failed step
     */
    public String step() { return step; }

    /**
     * Describes the retry budget consumed.
     * @return number of attempts; zero means condition evaluation failed
     */
    public int attempts() { return attempts; }
}
