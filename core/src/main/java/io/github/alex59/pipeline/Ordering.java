package io.github.alex59.pipeline;

/** Output ordering for concurrently processed input records. */
public enum Ordering {
    /** Emits in input order; a slow first record delays later completed results. */
    INPUT,
    /** Emits as records finish, without preserving input order. */
    COMPLETION
}
