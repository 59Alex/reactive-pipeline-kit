package io.github.alex59.pipeline;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.test.StepVerifier;
import reactor.util.context.Context;

class PipelineTest {
    @Test
    void typedCompositionAndDefinitionsRemainIndependent() {
        Pipeline<String, String> start = Pipeline.start();
        Pipeline<String, Integer> length = start.map("trim", String::trim).map("length", String::length);
        assertEquals(" hi ", start.run(" hi ").block());
        assertEquals(2, length.run(" hi ").block());
    }

    @Test
    void retriesOnlyFailedStepAndPreservesExecutionIdentity() {
        AtomicInteger first = new AtomicInteger();
        AtomicInteger attempts = new AtomicInteger();
        List<UUID> ids = new ArrayList<>();
        List<StepEvent> events = new ArrayList<>();
        Pipeline<Integer, Integer> pipeline = Pipeline.<Integer>start()
                .map("first", x -> { first.incrementAndGet(); return x + 1; })
                .then("remote", (x, context) -> {
                    ids.add(context.executionId());
                    return attempts.incrementAndGet() < 3 ? Mono.error(new IOException("temporary")) : Mono.just(x * 2);
                }, new StepPolicy(Duration.ofSeconds(1), 2, Duration.ZERO, IOException.class::isInstance))
                .observe(events::add);
        StepVerifier.create(pipeline.run(4)).expectNext(10).verifyComplete();
        assertEquals(1, first.get());
        assertEquals(3, attempts.get());
        assertEquals(1, ids.stream().distinct().count());
        assertEquals(List.of(1, 2, 3), events.stream().filter(e -> e.step().equals("remote") && e.kind() == StepEvent.Kind.STARTED).map(StepEvent::attempt).toList());
    }

    @Test
    void retryExhaustionKeepsStepAndOriginalCause() {
        IOException cause = new IOException("unavailable");
        Pipeline<Integer, Integer> pipeline = Pipeline.<Integer>start().then("lookup",
                (x, context) -> Mono.error(cause),
                new StepPolicy(Duration.ofSeconds(1), 2, Duration.ZERO, error -> true));
        StepVerifier.create(pipeline.run(1)).expectErrorSatisfies(error -> {
            StepFailure failure = assertInstanceOf(StepFailure.class, error);
            assertEquals("lookup", failure.step());
            assertEquals(3, failure.attempts());
            assertSame(cause, failure.getCause());
        }).verify();
    }

    @Test
    void nonRetryableErrorDoesNotRetry() {
        AtomicInteger attempts = new AtomicInteger();
        Pipeline<Integer, Integer> pipeline = Pipeline.<Integer>start().then("validate", (x, context) -> {
            attempts.incrementAndGet();
            throw new IllegalArgumentException("bad record");
        }, new StepPolicy(Duration.ofSeconds(1), 5, Duration.ZERO, IOException.class::isInstance));
        StepVerifier.create(pipeline.run(1)).expectError(StepFailure.class).verify();
        assertEquals(1, attempts.get());
    }

    @Test
    void timeoutCancelsAttemptAndRetryDelayIsExplicit() {
        AtomicInteger cancelled = new AtomicInteger();
        AtomicInteger attempts = new AtomicInteger();
        StepVerifier.withVirtualTime(() -> Pipeline.<Integer>start().<Integer>then("wait", (x, context) -> {
            attempts.incrementAndGet();
            return Mono.<Integer>never().doOnCancel(cancelled::incrementAndGet);
        }, new StepPolicy(Duration.ofSeconds(2), 1, Duration.ofSeconds(3), TimeoutException.class::isInstance)).run(1))
                .thenAwait(Duration.ofSeconds(2))
                .then(() -> { assertEquals(1, attempts.get()); assertEquals(1, cancelled.get()); })
                .thenAwait(Duration.ofSeconds(3))
                .then(() -> assertEquals(2, attempts.get()))
                .thenAwait(Duration.ofSeconds(2))
                .expectErrorSatisfies(error -> assertInstanceOf(TimeoutException.class, error.getCause()))
                .verify();
        assertEquals(2, cancelled.get());
    }

    @Test
    void concurrencyIsBoundedAndCancellationReachesAllActiveWork() {
        AtomicInteger subscribed = new AtomicInteger();
        AtomicInteger active = new AtomicInteger();
        AtomicInteger peak = new AtomicInteger();
        AtomicInteger cancelled = new AtomicInteger();
        AtomicInteger sourceCancelled = new AtomicInteger();
        Pipeline<Integer, Integer> pipeline = Pipeline.<Integer>start().then("remote", (x, context) ->
                Mono.defer(() -> {
                    subscribed.incrementAndGet();
                    peak.accumulateAndGet(active.incrementAndGet(), Math::max);
                    return Mono.<Integer>never().doOnCancel(() -> { active.decrementAndGet(); cancelled.incrementAndGet(); });
                }));
        StepVerifier.create(pipeline.process(Flux.range(1, 100).doOnCancel(sourceCancelled::incrementAndGet), 3, Ordering.COMPLETION))
                .then(() -> assertEquals(3, active.get())).thenCancel().verify();
        assertEquals(3, peak.get());
        assertEquals(3, subscribed.get());
        assertEquals(3, cancelled.get());
        assertEquals(0, active.get());
        assertEquals(1, sourceCancelled.get());
    }

    @Test
    void oneRecordFailureCancelsSiblings() {
        Sinks.One<Integer> first = Sinks.one();
        AtomicInteger cancelled = new AtomicInteger();
        Pipeline<Integer, Integer> pipeline = Pipeline.<Integer>start().then("work", (x, ctx) -> x == 1
                ? first.asMono() : Mono.<Integer>never().doOnCancel(cancelled::incrementAndGet));
        StepVerifier.create(pipeline.process(Flux.just(1, 2, 3), 3, Ordering.COMPLETION))
                .then(() -> first.tryEmitError(new IllegalStateException("failure")))
                .expectError(StepFailure.class).verify();
        assertEquals(2, cancelled.get());
    }

    @Test
    void orderingIsAnExplicitChoice() {
        StepVerifier.withVirtualTime(() -> delayedPipeline().process(Flux.just(1, 2, 3), 3, Ordering.INPUT))
                .thenAwait(Duration.ofSeconds(3)).expectNext(1, 2, 3).verifyComplete();
        StepVerifier.withVirtualTime(() -> delayedPipeline().process(Flux.just(1, 2, 3), 3, Ordering.COMPLETION))
                .thenAwait(Duration.ofSeconds(3)).expectNext(3, 2, 1).verifyComplete();
    }

    private Pipeline<Integer, Integer> delayedPipeline() {
        return Pipeline.<Integer>start().then("delay", (x, ctx) -> Mono.delay(Duration.ofSeconds(4 - x)).thenReturn(x));
    }

    @Test
    void subscriptionsHaveIndependentIdentitiesAndRetryCountersAndReadReactorContext() {
        List<UUID> ids = Collections.synchronizedList(new ArrayList<>());
        AtomicInteger invocations = new AtomicInteger();
        Pipeline<Integer, String> pipeline = Pipeline.<Integer>start().then("context", (x, context) -> {
            ids.add(context.executionId());
            invocations.incrementAndGet();
            return Mono.just(context.subscriberContext().get("tenant"));
        });
        Mono<String> run = pipeline.run(1);
        assertEquals(0, invocations.get());
        StepVerifier.create(run.contextWrite(Context.of("tenant", "one"))).expectNext("one").verifyComplete();
        StepVerifier.create(run.contextWrite(Context.of("tenant", "two"))).expectNext("two").verifyComplete();
        assertNotEquals(ids.get(0), ids.get(1));
    }

    @Test
    void retryBudgetResetsForEachSubscription() {
        var attemptsByExecution = new ConcurrentHashMap<UUID, AtomicInteger>();
        List<StepEvent> events = new ArrayList<>();
        var pipeline = Pipeline.<Integer>start().then("transient", (input, context) -> {
            int attempt = attemptsByExecution.computeIfAbsent(context.executionId(), id -> new AtomicInteger()).incrementAndGet();
            return attempt == 1 ? Mono.error(new IOException("retry")) : Mono.just(input);
        }, new StepPolicy(Duration.ofSeconds(1), 1, Duration.ZERO, IOException.class::isInstance)).observe(events::add);
        var sameColdRun = pipeline.run(7);
        StepVerifier.create(sameColdRun).expectNext(7).verifyComplete();
        StepVerifier.create(sameColdRun).expectNext(7).verifyComplete();
        assertEquals(2, attemptsByExecution.size());
        assertTrue(attemptsByExecution.values().stream().allMatch(count -> count.get() == 2));
        assertEquals(List.of(1, 2, 1, 2), events.stream().filter(event -> event.kind() == StepEvent.Kind.STARTED).map(StepEvent::attempt).toList());
    }

    @Test
    void completingARecordReplenishesOneSlot() {
        List<Sinks.One<Integer>> gates = new ArrayList<>();
        for (int i = 0; i < 5; i++) gates.add(Sinks.one());
        AtomicInteger subscribed = new AtomicInteger();
        var pipeline = Pipeline.<Integer>start().then("gate", (input, context) -> {
            subscribed.incrementAndGet();
            return gates.get(input).asMono();
        });
        StepVerifier.create(pipeline.process(Flux.range(0, 5), 2, Ordering.COMPLETION))
                .then(() -> assertEquals(2, subscribed.get()))
                .then(() -> gates.get(1).tryEmitValue(1))
                .expectNext(1)
                .then(() -> assertEquals(3, subscribed.get()))
                .thenCancel().verify();
    }

    @Test
    void conditionSkipsWithoutExecutingStepAndFalsePreservesInput() {
        AtomicInteger calls = new AtomicInteger();
        List<StepEvent> events = new ArrayList<>();
        Pipeline<Integer, Integer> pipeline = Pipeline.<Integer>start().thenIf("positive", x -> x > 0,
                (x, context) -> { calls.incrementAndGet(); return Mono.just(x * 10); }, StepPolicy.defaults()).observe(events::add);
        assertEquals(-2, pipeline.run(-2).block());
        assertEquals(20, pipeline.run(2).block());
        assertEquals(1, calls.get());
        assertEquals(StepEvent.Kind.SKIPPED, events.get(0).kind());
    }

    @Test
    void conditionFailureDoesNotRetryOrExecuteOperation() {
        AtomicInteger predicates = new AtomicInteger();
        Pipeline<Integer, Integer> pipeline = Pipeline.<Integer>start().thenIf("condition", x -> {
            predicates.incrementAndGet(); throw new IllegalStateException("bad condition");
        }, (x, context) -> Mono.just(x), new StepPolicy(Duration.ofSeconds(1), 4, Duration.ZERO, error -> true));
        StepVerifier.create(pipeline.run(1)).expectErrorSatisfies(error -> assertEquals(0, ((StepFailure) error).attempts())).verify();
        assertEquals(1, predicates.get());
    }

    @Test
    void emptyAndNullResultsFailWithStepContext() {
        StepVerifier.create(Pipeline.<Integer>start().then("empty", (x, ctx) -> Mono.empty()).run(1))
                .expectErrorSatisfies(error -> assertEquals("empty", ((StepFailure) error).step())).verify();
        StepVerifier.create(Pipeline.<Integer>start().then("null", (x, ctx) -> null).run(1))
                .expectErrorSatisfies(error -> assertInstanceOf(NullPointerException.class, error.getCause())).verify();
    }

    @Test
    void diagnosticFailureNeverRepeatsBusinessOperation() {
        AtomicInteger calls = new AtomicInteger();
        List<Throwable> dropped = new ArrayList<>();
        Hooks.onErrorDropped(dropped::add);
        try {
            Pipeline<Integer, Integer> pipeline = Pipeline.<Integer>start().then("effect", (x, ctx) -> {
                calls.incrementAndGet(); return Mono.just(x);
            }, new StepPolicy(Duration.ofSeconds(1), 2, Duration.ZERO, error -> true))
                    .observe(event -> { throw new IllegalStateException("metrics unavailable"); });
            StepVerifier.create(pipeline.run(1)).expectNext(1).verifyComplete();
            assertEquals(1, calls.get());
            assertEquals(2, dropped.size());
        } finally {
            Hooks.resetOnErrorDropped();
        }
    }

    @Test
    void invalidConfigurationFailsBeforeExecution() {
        Pipeline<Integer, Integer> pipeline = Pipeline.<Integer>start().map("same", x -> x);
        assertThrows(IllegalArgumentException.class, () -> pipeline.map("same", x -> x));
        assertThrows(IllegalArgumentException.class, () -> pipeline.map(" ", x -> x));
        assertThrows(IllegalArgumentException.class, () -> pipeline.process(Flux.just(1), 0, Ordering.INPUT));
        assertThrows(IllegalArgumentException.class, () -> StepPolicy.once(Duration.ZERO));
        assertThrows(IllegalArgumentException.class, () -> new StepPolicy(Duration.ofSeconds(1), -1, Duration.ZERO, error -> true));
        assertThrows(IllegalArgumentException.class, () -> new StepPolicy(Duration.ofSeconds(1), 0, Duration.ofSeconds(-1), error -> true));
    }
}
