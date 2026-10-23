package io.github.alex59.pipeline.java25;

import static org.junit.jupiter.api.Assertions.*;

import io.github.alex59.pipeline.Pipeline;
import io.github.alex59.pipeline.StepFailure;
import io.github.alex59.pipeline.StepPolicy;
import java.io.IOException;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import reactor.util.context.Context;

@Timeout(15)
class ScopedBlockingStepsTest {
    @Test
    void bindsOnActualVirtualThreadAndUnbindsBeforeDownstream() {
        assertEquals(Optional.empty(), ScopedBlockingSteps.currentContext());
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var pipeline = Pipeline.<String>start()
                    .then("scoped", ScopedBlockingSteps.call(executor, input -> {
                        assertTrue(Thread.currentThread().isVirtual());
                        assertEquals("tenant-a", ScopedBlockingSteps.currentContext().orElseThrow()
                                .subscriberContext().get("tenant"));
                        assertTrue(nestedContextAvailable());
                        return input.toUpperCase(java.util.Locale.ROOT);
                    }))
                    .map("after", output -> {
                        assertEquals(Optional.empty(), ScopedBlockingSteps.currentContext());
                        return output;
                    });
            StepVerifier.create(pipeline.run("java").contextWrite(Context.of("tenant", "tenant-a")))
                    .expectNext("JAVA").verifyComplete();
            assertFalse(executor.isShutdown());
        }
        assertEquals(Optional.empty(), ScopedBlockingSteps.currentContext());
    }

    private static boolean nestedContextAvailable() {
        return ScopedBlockingSteps.currentContext().isPresent();
    }

    @Test
    void concurrentRunsDoNotShareBindingsOrPropagateToUnrelatedThreads() {
        var identities = ConcurrentHashMap.<UUID>newKeySet();
        try (var executor = Executors.newVirtualThreadPerTaskExecutor();
             var unrelated = Executors.newVirtualThreadPerTaskExecutor()) {
            var pipeline = Pipeline.<Integer>start().then("scoped", ScopedBlockingSteps.call(executor, input -> {
                var context = ScopedBlockingSteps.currentContext().orElseThrow();
                identities.add(context.executionId());
                assertEquals(input, context.subscriberContext().get("record"));
                assertTrue(unrelated.submit(ScopedBlockingSteps::currentContext).get(3, TimeUnit.SECONDS).isEmpty());
                assertTrue(Mono.fromCallable(ScopedBlockingSteps::currentContext)
                        .subscribeOn(Schedulers.parallel()).block(Duration.ofSeconds(3)).isEmpty());
                assertSame(context, ScopedBlockingSteps.currentContext().orElseThrow());
                return input;
            }));
            var results = Flux.range(0, 40).flatMap(input -> pipeline.run(input)
                    .contextWrite(Context.of("record", input)), 8).collectList().block(Duration.ofSeconds(5));
            assertNotNull(results);
            assertEquals(40, results.size());
            assertEquals(40, identities.size());
        }
    }

    @Test
    void exceptionalExitClearsBindingOnReusedWorkerBeforeErrorSignal() throws Exception {
        IOException cause = new IOException("lookup failed");
        try (var executor = Executors.newSingleThreadExecutor()) {
            var pipeline = Pipeline.<Integer>start().then("scoped", ScopedBlockingSteps.call(executor, input -> {
                assertTrue(ScopedBlockingSteps.currentContext().isPresent());
                throw cause;
            }));
            StepVerifier.create(pipeline.run(1).doOnError(error -> assertTrue(ScopedBlockingSteps.currentContext().isEmpty())))
                    .expectErrorSatisfies(error -> {
                        assertInstanceOf(StepFailure.class, error);
                        assertSame(cause, error.getCause());
                    }).verify();
            assertTrue(executor.submit(ScopedBlockingSteps::currentContext).get(3, TimeUnit.SECONDS).isEmpty());
        }
    }

    @Test
    void interruptionUsesCoreCancellationAndScopeEndsWhenOperationExits() throws Exception {
        var started = new CountDownLatch(1);
        var interrupted = new CountDownLatch(1);
        try (var executor = Executors.newSingleThreadExecutor()) {
            var pipeline = Pipeline.<Integer>start().then("scoped", ScopedBlockingSteps.call(executor, input -> {
                started.countDown();
                try {
                    new CountDownLatch(1).await();
                    return input;
                } catch (InterruptedException failure) {
                    assertTrue(ScopedBlockingSteps.currentContext().isPresent());
                    interrupted.countDown();
                    throw failure;
                }
            }));
            var subscription = pipeline.run(1).subscribe();
            assertTrue(started.await(3, TimeUnit.SECONDS));
            subscription.dispose();
            assertTrue(interrupted.await(3, TimeUnit.SECONDS));
            assertTrue(executor.submit(ScopedBlockingSteps::currentContext).get(3, TimeUnit.SECONDS).isEmpty());
        }
    }

    @Test
    void retryRebindsTheSameExecutionAndEachSubscriptionGetsNewIdentity() {
        var counts = new ConcurrentHashMap<UUID, AtomicInteger>();
        AtomicReference<UUID> previousRun = new AtomicReference<>();
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var pipeline = Pipeline.<Integer>start().then("scoped", ScopedBlockingSteps.call(executor, input -> {
                UUID id = ScopedBlockingSteps.currentContext().orElseThrow().executionId();
                previousRun.set(id);
                if (counts.computeIfAbsent(id, ignored -> new AtomicInteger()).incrementAndGet() == 1) {
                    throw new IOException("retry");
                }
                return input;
            }), new StepPolicy(Duration.ofSeconds(3), 1, Duration.ZERO, IOException.class::isInstance));
            var run = pipeline.run(7);
            StepVerifier.create(run).expectNext(7).verifyComplete();
            UUID first = previousRun.get();
            StepVerifier.create(run).expectNext(7).verifyComplete();
            assertNotEquals(first, previousRun.get());
            assertEquals(2, counts.size());
            assertTrue(counts.values().stream().allMatch(count -> count.get() == 2));
        }
    }
}
