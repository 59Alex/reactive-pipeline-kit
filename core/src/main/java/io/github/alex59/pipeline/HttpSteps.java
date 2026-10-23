package io.github.alex59.pipeline;

import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Objects;
import reactor.core.publisher.Mono;

/** JDK HTTP adapter; application code owns and configures the shared client. */
public final class HttpSteps {
    private HttpSteps() { }

    /**
     * Sends asynchronously and propagates cancellation to the CompletableFuture.
     * HTTP status codes are returned as data; classify retryable responses in a
     * custom HttpPort if necessary. The body handler controls body buffering.
     * @param client application-owned reusable client
     * @param bodyHandler response body policy
     * @param <T> response body type
     * @return cold HTTP port
     */
    public static <T> HttpPort<HttpRequest, HttpResponse<T>> jdk(HttpClient client,
                                                              HttpResponse.BodyHandler<T> bodyHandler) {
        Objects.requireNonNull(client, "client");
        Objects.requireNonNull(bodyHandler, "bodyHandler");
        return (request, context) -> Mono.fromFuture(() -> client.sendAsync(request, bodyHandler));
    }
}
