import io.github.alex59.pipeline.*;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.context.Context;

public final class Example {
    public static void main(String[] args) throws Exception {
        typed();
        retry();
        timeout();
        ordering();
        http();
        blocking();
        diagnostics();
        failure();
    }

    static void typed() throws Exception {
        Pipeline<String, Integer> sizes = Pipeline.<String>start()
            .map("trim", String::trim)
            .thenIf("remove-prefix", value -> value.startsWith("id:"),
                (value, context) -> Mono.just(value.substring(3)), StepPolicy.defaults())
            .map("length", String::length);
        Integer result = sizes.run(" id:java ").block();
        check(Objects.equals(4, result), "typed result");
        System.out.println("typed=" + result);
    }

    static void retry() throws Exception {
        AtomicInteger previousStep = new AtomicInteger();
        AtomicInteger attempts = new AtomicInteger();
        StepPolicy policy = new StepPolicy(Duration.ofSeconds(2), 2,
            Duration.ofMillis(10), IOException.class::isInstance);
        var pipeline = Pipeline.<Integer>start()
            .map("prepare", value -> { previousStep.incrementAndGet(); return value + 1; })
            .then("enrich", (value, context) -> attempts.incrementAndGet() < 3
                ? Mono.error(new IOException("temporary")) : Mono.just(value * 2), policy);
        Integer result = pipeline.run(4).block();
        check(Objects.equals(10, result) && previousStep.get() == 1 && attempts.get() == 3,
            "retry boundary");
        System.out.println("retry=" + result + "; previous=" + previousStep + "; attempts=" + attempts);
    }

    static void timeout() throws Exception {
        var pipeline = Pipeline.<Integer>start().<Integer>then("slow",
            (value, context) -> Mono.never(), StepPolicy.once(Duration.ofMillis(50)));
        try {
            pipeline.run(1).block();
            throw new AssertionError("timeout expected");
        } catch (StepFailure failure) {
            check(failure.getCause() instanceof TimeoutException, "timeout cause");
            System.out.println("timeout=" + failure.step() + "; attempts=" + failure.attempts());
        }
    }

    static void ordering() throws Exception {
        var delayed = Pipeline.<Integer>start().then("delay",
            (value, context) -> Mono.delay(Duration.ofMillis((4 - value) * 20L)).thenReturn(value));
        List<Integer> ordered = delayed.process(Flux.just(1, 2, 3), 3, Ordering.INPUT)
            .collectList().block();
        List<Integer> completed = delayed.process(Flux.just(1, 2, 3), 3, Ordering.COMPLETION)
            .collectList().block();
        check(List.of(1, 2, 3).equals(ordered), "input order");
        check(completed != null && Set.copyOf(completed).equals(Set.of(1, 2, 3)), "all results");
        System.out.println("ordered=" + ordered + "; completionContainsAll=true");
    }

    static void http() throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/value", exchange -> {
            byte[] body = "try later".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(503, body.length);
            try (var output = exchange.getResponseBody()) { output.write(body); }
        });
        server.start();
        try {
            var client = HttpSteps.jdk(HttpClient.newHttpClient(), HttpResponse.BodyHandlers.ofString());
            var fetch = Pipeline.<HttpRequest>start()
                .then("fetch", client.asStep(), StepPolicy.once(Duration.ofSeconds(2)));
            URI endpoint = URI.create("http://127.0.0.1:" + server.getAddress().getPort() + "/value");
            var response = fetch.run(HttpRequest.newBuilder(endpoint).GET().build()).block();
            check(response != null && response.statusCode() == 503, "HTTP status remains data");
            System.out.println("http=" + response.statusCode() + "; body=" + response.body());
        } finally {
            server.stop(0);
        }
    }

    static void blocking() throws Exception {
        var executor = Executors.newFixedThreadPool(2);
        try {
            var lookup = Pipeline.<String>start().then("blocking", BlockingSteps.call(executor,
                (value, context) -> {
                    Thread.sleep(5);
                    return context.subscriberContext().<String>get("tenant") + ":" + value;
                }));
            String result = lookup.run("java").contextWrite(Context.of("tenant", "team")).block();
            check("team:java".equals(result), "explicit context");
            System.out.println("blocking=" + result);
        } finally {
            executor.shutdownNow();
            check(executor.awaitTermination(5, TimeUnit.SECONDS), "executor shutdown");
        }
    }

    static void diagnostics() throws Exception {
        var events = new ConcurrentLinkedQueue<StepEvent>();
        var lookup = Pipeline.<String>start()
            .then("tenant", (value, context) -> Mono.just(
                context.subscriberContext().<String>get("tenant") + ":" + value))
            .observe(events::add);
        String result = lookup.run("java").contextWrite(Context.of("tenant", "team")).block();
        check("team:java".equals(result) && events.size() == 2, "diagnostic events");
        check(events.stream().map(StepEvent::executionId).distinct().count() == 1, "execution identity");
        System.out.println("events=" + events.stream().map(StepEvent::kind).toList());
    }

    static void failure() throws Exception {
        var invalid = Pipeline.<String>start().then("lookup", (value, context) -> Mono.empty());
        try {
            invalid.run("java").block();
            throw new AssertionError("empty step must fail");
        } catch (StepFailure failure) {
            check("lookup".equals(failure.step()), "failed step name");
            check(failure.getCause() instanceof NoSuchElementException, "empty completion cause");
            System.out.println("failure=" + failure.step() + "; cause=" + failure.getCause().getClass().getSimpleName());
        }
    }

    static void check(boolean condition, String description) {
        if (!condition) throw new AssertionError(description);
    }
}
