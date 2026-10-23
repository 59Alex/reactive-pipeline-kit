package io.github.alex59.pipeline;

import static org.junit.jupiter.api.Assertions.*;

import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.net.Authenticator;
import java.net.CookieHandler;
import java.net.ProxySelector;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

class HttpStepsTest {
    @Test
    void downstreamCancellationCancelsHttpFuture() {
        PendingClient client = new PendingClient();
        var pipeline = Pipeline.<HttpRequest>start().then("http", HttpSteps.jdk(client, HttpResponse.BodyHandlers.ofString()).asStep());
        var request = HttpRequest.newBuilder(URI.create("https://example.org/value")).build();
        StepVerifier.create(pipeline.run(request)).thenCancel().verify();
        assertTrue(client.pending.isCancelled());
    }

    @Test
    void jdkAdapterPerformsRealHttpAndDoesNotTreatStatusAsTransportFailure() throws Exception {
        AtomicInteger requests = new AtomicInteger();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/value", exchange -> {
            requests.incrementAndGet();
            byte[] body = "temporarily unavailable".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(503, body.length);
            try (var output = exchange.getResponseBody()) { output.write(body); }
        });
        server.start();
        try {
            var http = HttpSteps.jdk(HttpClient.newHttpClient(), HttpResponse.BodyHandlers.ofString());
            var pipeline = Pipeline.<HttpRequest>start().then("http", http.asStep());
            var request = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + server.getAddress().getPort() + "/value")).GET().build();
            var run = pipeline.run(request);
            assertEquals(0, requests.get());
            var response = run.block(Duration.ofSeconds(5));
            assertNotNull(response);
            assertEquals(503, response.statusCode());
            assertEquals("temporarily unavailable", response.body());
            assertEquals(1, requests.get());
        } finally {
            server.stop(0);
        }
    }

    /** Transport boundary fake: verifies cancellation of the exact future supplied to Reactor. */
    private static final class PendingClient extends HttpClient {
        private final CompletableFuture<HttpResponse<String>> pending = new CompletableFuture<>();

        @Override
        @SuppressWarnings("unchecked")
        public <T> CompletableFuture<HttpResponse<T>> sendAsync(HttpRequest request, HttpResponse.BodyHandler<T> handler) {
            return (CompletableFuture<HttpResponse<T>>) (CompletableFuture<?>) pending;
        }
        @Override public <T> CompletableFuture<HttpResponse<T>> sendAsync(HttpRequest request, HttpResponse.BodyHandler<T> handler, HttpResponse.PushPromiseHandler<T> push) { throw new UnsupportedOperationException(); }
        @Override public <T> HttpResponse<T> send(HttpRequest request, HttpResponse.BodyHandler<T> handler) { throw new UnsupportedOperationException(); }
        @Override public Optional<CookieHandler> cookieHandler() { return Optional.empty(); }
        @Override public Optional<Duration> connectTimeout() { return Optional.empty(); }
        @Override public Redirect followRedirects() { return Redirect.NEVER; }
        @Override public Optional<ProxySelector> proxy() { return Optional.empty(); }
        @Override public SSLContext sslContext() { throw new UnsupportedOperationException(); }
        @Override public SSLParameters sslParameters() { return new SSLParameters(); }
        @Override public Optional<Authenticator> authenticator() { return Optional.empty(); }
        @Override public Version version() { return Version.HTTP_1_1; }
        @Override public Optional<Executor> executor() { return Optional.empty(); }
    }
}
