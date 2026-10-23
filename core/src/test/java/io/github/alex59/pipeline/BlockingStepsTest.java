package io.github.alex59.pipeline;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;
import reactor.util.context.Context;

@Timeout(15)
class BlockingStepsTest {
    @Test
    void usesRealVirtualThreadsAndClearsBindingBeforeDownstreamSignals() {
        assertEquals(Optional.empty(), BlockingSteps.currentContext());
        AtomicInteger calls = new AtomicInteger();
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var pipeline = Pipeline.<String>start()
                    .then("blocking", BlockingSteps.call(executor, input -> {
                        assertTrue(Thread.currentThread().isVirtual());
                        calls.incrementAndGet();
                        assertEquals("tenant-a", BlockingSteps.currentContext().orElseThrow().subscriberContext().get("tenant"));
                        return input.toUpperCase(java.util.Locale.ROOT);
                    }))
                    .map("after", output -> {
                        assertEquals(Optional.empty(), BlockingSteps.currentContext());
                        return output;
                    });
            var cold = pipeline.run("java").contextWrite(Context.of("tenant", "tenant-a"));
            assertEquals(0, calls.get());
            StepVerifier.create(cold).expectNext("JAVA").verifyComplete();
            StepVerifier.create(cold).expectNext("JAVA").verifyComplete();
            assertEquals(2, calls.get());
            assertFalse(executor.isShutdown(), "only the application may close the executor");
        }
        assertEquals(Optional.empty(), BlockingSteps.currentContext());
    }

    @Test
    void concurrentExecutionsKeepScopedContextSeparateAndDoNotBindUnrelatedTasks() throws Exception {
        var identities = ConcurrentHashMap.<UUID>newKeySet();
        try (var executor = Executors.newVirtualThreadPerTaskExecutor();
             var unrelatedExecutor = Executors.newVirtualThreadPerTaskExecutor()) {
            var pipeline = Pipeline.<Integer>start().then("blocking", BlockingSteps.call(executor, input -> {
                assertTrue(Thread.currentThread().isVirtual());
                var context = BlockingSteps.currentContext().orElseThrow();
                identities.add(context.executionId());
                assertEquals(input, context.subscriberContext().get("record"));
                assertTrue(unrelatedExecutor.submit(BlockingSteps::currentContext).get(3, TimeUnit.SECONDS).isEmpty());
                Thread.sleep(Duration.ofMillis(1));
                assertSame(context, BlockingSteps.currentContext().orElseThrow());
                return input;
            }));
            var results = Flux.range(0, 40).flatMap(input -> pipeline.run(input)
                    .contextWrite(Context.of("record", input)), 8).collectList().block(Duration.ofSeconds(5));
            assertNotNull(results);
            assertEquals(40, results.size());
            assertEquals(40, identities.size());
            assertTrue(executor.submit(BlockingSteps::currentContext).get(3, TimeUnit.SECONDS).isEmpty());
        }
    }

    @Test
    void cancellationInterruptsCooperativeBlockingWork() throws Exception {
        var started = new CountDownLatch(1);
        var interrupted = new CountDownLatch(1);
        var completed = new AtomicInteger();
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var pipeline = Pipeline.<Integer>start().then("wait", BlockingSteps.call(executor, input -> {
                started.countDown();
                try {
                    new CountDownLatch(1).await();
                    return input;
                } catch (InterruptedException failure) {
                    interrupted.countDown();
                    throw failure;
                }
            }));
            var subscription = pipeline.run(1).subscribe(value -> completed.incrementAndGet());
            assertTrue(started.await(3, TimeUnit.SECONDS));
            subscription.dispose();
            assertTrue(interrupted.await(3, TimeUnit.SECONDS));
            assertEquals(0, completed.get());
        }
    }

    @Test
    void cancellationDuringExecutorHandoffCannotMissTheTask() throws Exception {
        AtomicInteger operations = new AtomicInteger();
        var subscriber = new BaseSubscriber<Integer>() { };
        try (var executor = new PausingExecutor()) {
            var pipeline = Pipeline.<Integer>start().then("handoff", BlockingSteps.call(executor, input -> {
                operations.incrementAndGet();
                return input;
            }));
            Thread subscriptionThread = Thread.ofVirtual().start(() -> pipeline.run(1).subscribe(subscriber));
            try {
                assertTrue(executor.handoff.await(3, TimeUnit.SECONDS));
                subscriber.dispose();
            } finally {
                executor.accept.countDown();
            }
            subscriptionThread.join(Duration.ofSeconds(3));
            assertFalse(subscriptionThread.isAlive());
            assertTrue(executor.finished.await(3, TimeUnit.SECONDS));
            assertEquals(0, operations.get());
        }
    }

    @Test
    void attemptTimeoutInterruptsBlockingTaskAndKeepsTimeoutAsCause() throws Exception {
        var started = new CountDownLatch(1);
        var interrupted = new CountDownLatch(1);
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var pipeline = Pipeline.<Integer>start().then("timeout", BlockingSteps.call(executor, input -> {
                started.countDown();
                try {
                    new CountDownLatch(1).await();
                    return input;
                } catch (InterruptedException failure) {
                    interrupted.countDown();
                    throw failure;
                }
            }), StepPolicy.once(Duration.ofSeconds(1)));
            StepVerifier.create(pipeline.run(1))
                    .then(() -> {
                        try { assertTrue(started.await(3, TimeUnit.SECONDS)); }
                        catch (InterruptedException failure) { throw new AssertionError(failure); }
                    })
                    .expectErrorSatisfies(error -> assertInstanceOf(TimeoutException.class, error.getCause()))
                    .verify(Duration.ofSeconds(5));
            assertTrue(interrupted.await(3, TimeUnit.SECONDS));
        }
    }

    @Test
    void checkedFailureIsPreservedAndScopeDoesNotLeakOnReusedWorker() throws Exception {
        IOException expected = new IOException("lookup failed");
        try (var executor = Executors.newSingleThreadExecutor()) {
            var pipeline = Pipeline.<Integer>start().then("legacy", BlockingSteps.call(executor, input -> {
                assertTrue(BlockingSteps.currentContext().isPresent());
                throw expected;
            }));
            StepVerifier.create(pipeline.run(1)).expectErrorSatisfies(error -> {
                assertEquals("legacy", ((StepFailure) error).step());
                assertSame(expected, error.getCause());
            }).verify();
            assertTrue(executor.submit(BlockingSteps::currentContext).get(3, TimeUnit.SECONDS).isEmpty());
        }
    }

    @Test
    void pipelineRetryCreatesNewTaskButDoesNotReplayPreviousSteps() {
        AtomicInteger earlier = new AtomicInteger();
        AtomicInteger attempts = new AtomicInteger();
        AtomicReference<UUID> execution = new AtomicReference<>();
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var pipeline = Pipeline.<Integer>start().map("first", input -> {
                earlier.incrementAndGet();
                return input;
            }).then("blocking", BlockingSteps.call(executor, input -> {
                UUID id = BlockingSteps.currentContext().orElseThrow().executionId();
                execution.compareAndSet(null, id);
                assertEquals(execution.get(), id);
                if (attempts.incrementAndGet() == 1) throw new IOException("transient");
                return input * 2;
            }), new StepPolicy(Duration.ofSeconds(3), 1, Duration.ZERO, IOException.class::isInstance));
            StepVerifier.create(pipeline.run(4)).expectNext(8).verifyComplete();
            assertEquals(1, earlier.get());
            assertEquals(2, attempts.get());
        }
    }

    @Test
    void virtualThreadsRespectPipelineConcurrencyAndCancellation() throws Exception {
        AtomicInteger started = new AtomicInteger();
        AtomicInteger active = new AtomicInteger();
        AtomicInteger peak = new AtomicInteger();
        var ready = new CountDownLatch(3);
        var stopped = new CountDownLatch(3);
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var pipeline = Pipeline.<Integer>start().then("wait", BlockingSteps.call(executor, input -> {
                started.incrementAndGet();
                peak.accumulateAndGet(active.incrementAndGet(), Math::max);
                ready.countDown();
                try {
                    new CountDownLatch(1).await();
                    return input;
                } finally {
                    active.decrementAndGet();
                    stopped.countDown();
                }
            }));
            var subscription = pipeline.process(Flux.range(0, 100), 3, Ordering.COMPLETION).subscribe();
            assertTrue(ready.await(3, TimeUnit.SECONDS));
            assertEquals(3, started.get());
            subscription.dispose();
            assertTrue(stopped.await(3, TimeUnit.SECONDS));
            assertEquals(3, peak.get());
            assertEquals(0, active.get());
        }
    }

    @Test
    void rejectedExecutorAndNullResultAreNamedFailures() {
        var closed = Executors.newVirtualThreadPerTaskExecutor();
        closed.close();
        StepVerifier.create(Pipeline.<Integer>start().then("closed", BlockingSteps.call(closed, input -> input)).run(1))
                .expectErrorSatisfies(error -> assertInstanceOf(RejectedExecutionException.class, error.getCause())).verify();
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            StepVerifier.create(Pipeline.<Integer>start().then("null", BlockingSteps.call(executor, input -> null)).run(1))
                    .expectErrorSatisfies(error -> assertInstanceOf(NullPointerException.class, error.getCause())).verify();
        }
    }

    /** Deliberately holds the acceptance boundary to reproduce a cancellation race. */
    private static final class PausingExecutor extends AbstractExecutorService {
        private final ExecutorService delegate = Executors.newVirtualThreadPerTaskExecutor();
        private final CountDownLatch handoff = new CountDownLatch(1);
        private final CountDownLatch accept = new CountDownLatch(1);
        private final CountDownLatch finished = new CountDownLatch(1);

        @Override public void execute(Runnable command) {
            handoff.countDown();
            try {
                if (!accept.await(3, TimeUnit.SECONDS)) throw new RejectedExecutionException("test handoff timed out");
            } catch (InterruptedException failure) {
                Thread.currentThread().interrupt();
                throw new RejectedExecutionException(failure);
            }
            delegate.execute(() -> {
                try { command.run(); } finally { finished.countDown(); }
            });
        }
        @Override public void shutdown() { delegate.shutdown(); }
        @Override public List<Runnable> shutdownNow() { return delegate.shutdownNow(); }
        @Override public boolean isShutdown() { return delegate.isShutdown(); }
        @Override public boolean isTerminated() { return delegate.isTerminated(); }
        @Override public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException { return delegate.awaitTermination(timeout, unit); }
    }
}
