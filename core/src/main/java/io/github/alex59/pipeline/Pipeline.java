package io.github.alex59.pipeline;

import java.time.Duration;
import java.util.HashSet;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import org.reactivestreams.Publisher;
import reactor.core.Exceptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;
import reactor.util.retry.Retry;

/**
 * Immutable, reusable definition of a typed asynchronous pipeline.
 * Each run subscription creates its own identity and retry counters.
 * User-supplied steps and values remain the caller's responsibility.
 * @param <I> pipeline input
 * @param <O> current pipeline output
 */
public final class Pipeline<I, O> {
    private record Execution(ExecutionContext context, StepObserver observer) { }

    private final BiFunction<I, Execution, Mono<O>> flow;
    private final Set<String> names;
    private final StepObserver observer;

    private Pipeline(BiFunction<I, Execution, Mono<O>> flow, Set<String> names,
                     StepObserver observer) {
        this.flow = flow;
        this.names = names;
        this.observer = observer;
    }

    /**
     * Begins a definition with an identity transformation.
     * @param <T> input type
     * @return empty pipeline
     */
    public static <T> Pipeline<T, T> start() {
        return new Pipeline<>((value, execution) -> Mono.just(value), Set.of(), StepObserver.NONE);
    }

    /**
     * Returns a definition using this observer for all its steps.
     * @param observer nonblocking thread-safe observer
     * @return new definition
     */
    public Pipeline<I, O> observe(StepObserver observer) {
        return new Pipeline<>(flow, names, Objects.requireNonNull(observer, "observer"));
    }

    /**
     * Adds an asynchronous step using the default policy.
     * @param name unique nonblank step name
     * @param step step implementation
     * @param <N> new output type
     * @return new definition
     */
    public <N> Pipeline<I, N> then(String name, Step<O, N> step) {
        return then(name, step, StepPolicy.defaults());
    }

    /**
     * Adds an asynchronous step with isolated timeout and retry boundaries.
     * @param name unique nonblank step name
     * @param step step implementation
     * @param policy attempt policy
     * @param <N> new output type
     * @return new definition
     */
    public <N> Pipeline<I, N> then(String name, Step<O, N> step, StepPolicy policy) {
        Set<String> nextNames = addName(name);
        Objects.requireNonNull(step, "step");
        Objects.requireNonNull(policy, "policy");
        return new Pipeline<>((input, execution) -> flow.apply(input, execution)
                .flatMap(value -> execute(name, step, policy, value, execution)), nextNames, observer);
    }

    /**
     * Adds a synchronous, nonblocking transformation with the default policy.
     * @param name unique nonblank step name
     * @param mapper transformation returning a non-null value
     * @param <N> new output type
     * @return new definition
     */
    public <N> Pipeline<I, N> map(String name, Function<? super O, ? extends N> mapper) {
        Objects.requireNonNull(mapper, "mapper");
        return then(name, (value, context) -> Mono.fromCallable(() -> mapper.apply(value)));
    }

    /**
     * Runs a same-type step only when its predicate is true. False passes the
     * input through; a predicate error fails once without retrying the predicate.
     * @param name unique nonblank step name
     * @param condition nonblocking predicate evaluated once per input
     * @param step conditional transformation
     * @param policy attempt policy for the transformation
     * @return new definition
     */
    public Pipeline<I, O> thenIf(String name, Predicate<? super O> condition,
                                Step<O, O> step, StepPolicy policy) {
        Set<String> nextNames = addName(name);
        Objects.requireNonNull(condition, "condition");
        Objects.requireNonNull(step, "step");
        Objects.requireNonNull(policy, "policy");
        return new Pipeline<>((input, execution) -> flow.apply(input, execution).flatMap(value -> {
            boolean matches;
            try {
                matches = condition.test(value);
            } catch (Throwable failure) {
                Exceptions.throwIfFatal(failure);
                event(execution, name, 0, StepEvent.Kind.FAILED, 0, failure);
                return Mono.error(new StepFailure(name, 0, failure));
            }
            if (!matches) {
                event(execution, name, 0, StepEvent.Kind.SKIPPED, 0, null);
                return Mono.just(value);
            }
            return execute(name, step, policy, value, execution);
        }), nextNames, observer);
    }

    /**
     * Creates a cold execution. Each subscription starts again with a fresh
     * execution identity. The input reference is not copied.
     * @param input non-null value; prefer immutable values
     * @return one result or a named StepFailure
     */
    public Mono<O> run(I input) {
        Objects.requireNonNull(input, "input");
        return Mono.deferContextual(context -> flow.apply(input,
                new Execution(new ExecutionContext(UUID.randomUUID(), context), observer)));
    }

    /**
     * Processes a compliant source with at most concurrency active records.
     * On the first error, cancels the source and other records. One result is
     * buffered per active inner subscription; record size is not byte-limited.
     * @param source input records
     * @param concurrency positive maximum number of in-flight records
     * @param ordering output ordering contract
     * @return cold, backpressured processing stream
     */
    public Flux<O> process(Publisher<I> source, int concurrency, Ordering ordering) {
        Objects.requireNonNull(source, "source");
        Objects.requireNonNull(ordering, "ordering");
        if (concurrency < 1) throw new IllegalArgumentException("concurrency must be positive");
        return Flux.defer(() -> ordering == Ordering.INPUT
                ? Flux.from(source).flatMapSequential(this::run, concurrency, 1)
                : Flux.from(source).flatMap(this::run, concurrency, 1));
    }

    private Set<String> addName(String name) {
        Objects.requireNonNull(name, "name");
        if (name.isBlank()) throw new IllegalArgumentException("step name must not be blank");
        if (names.contains(name)) throw new IllegalArgumentException("duplicate step name: " + name);
        Set<String> next = new HashSet<>(names);
        next.add(name);
        return Set.copyOf(next);
    }

    private static <A, B> Mono<B> execute(String name, Step<A, B> step, StepPolicy policy,
                                         A input, Execution execution) {
        return Mono.defer(() -> {
            AtomicInteger attempts = new AtomicInteger();
            Mono<B> work = Mono.defer(() -> {
                int attempt = attempts.incrementAndGet();
                long started = System.nanoTime();
                AtomicBoolean terminated = new AtomicBoolean();
                event(execution, name, attempt, StepEvent.Kind.STARTED, 0, null);
                return Mono.defer(() -> Objects.requireNonNull(step.apply(input, execution.context()), "step returned null Mono"))
                        .switchIfEmpty(Mono.error(new NoSuchElementException("Step '" + name + "' completed without a value")))
                        .timeout(policy.timeout())
                        .doOnSuccess(result -> {
                            if (terminated.compareAndSet(false, true)) event(execution, name, attempt, StepEvent.Kind.SUCCEEDED, System.nanoTime() - started, null);
                        })
                        .doOnError(error -> {
                            if (terminated.compareAndSet(false, true)) event(execution, name, attempt, StepEvent.Kind.FAILED, System.nanoTime() - started, error);
                        })
                        .doOnCancel(() -> {
                            if (terminated.compareAndSet(false, true)) event(execution, name, attempt, StepEvent.Kind.CANCELLED, System.nanoTime() - started, null);
                        });
            });
            if (policy.maxRetries() > 0) {
                Retry retry = policy.retryDelay().isZero()
                        ? Retry.max(policy.maxRetries()).filter(policy.retryOn()).onRetryExhaustedThrow((spec, signal) -> signal.failure())
                        : Retry.fixedDelay(policy.maxRetries(), policy.retryDelay()).filter(policy.retryOn()).onRetryExhaustedThrow((spec, signal) -> signal.failure());
                work = work.retryWhen(retry);
            }
            return work.onErrorMap(error -> new StepFailure(name, attempts.get(), error));
        });
    }

    private static void event(Execution execution, String name, int attempt, StepEvent.Kind kind,
                               long elapsedNanos, Throwable error) {
        try {
            execution.observer().onEvent(new StepEvent(execution.context().executionId(), name,
                    attempt, kind, Duration.ofNanos(Math.max(0, elapsedNanos)), error));
        } catch (Throwable observerError) {
            Exceptions.throwIfFatal(observerError);
            try {
                Operators.onErrorDropped(observerError, reactor.util.context.Context.of(execution.context().subscriberContext()));
            } catch (Throwable hookError) {
                // The diagnostic fallback is application code too. Do not let its
                // failure retry a successful effect, or recursively report to it.
                Exceptions.throwIfFatal(hookError);
            }
        }
    }
}
