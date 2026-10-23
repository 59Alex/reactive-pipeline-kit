package io.github.alex59.pipeline.jolt;

import com.bazaarvoice.jolt.Chainr;
import io.github.alex59.pipeline.ExecutionContext;
import io.github.alex59.pipeline.Step;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import reactor.core.publisher.Mono;

/**
 * Reusable Jolt specification with isolated input, specification and transform
 * instances for each attempt. Supports the built-in shift, default, remove,
 * cardinality and sort operations. It does not load arbitrary transform classes.
 * JSON trees are fully materialized; this is not a streaming JSON parser.
 */
public final class JoltStep implements Step<Object, Object> {
    private static final Set<String> OPERATIONS = Set.of("shift", "default", "remove", "cardinality", "sort");
    private final List<?> specification;

    private JoltStep(List<?> specification) {
        this.specification = specification;
    }

    /**
     * Snapshots and validates a specification. A fresh Chainr is compiled for
     * each execution, trading setup work for isolation without a shared lock.
     * @param specification list of Jolt operations using JSON-compatible values
     * @return reusable step
     */
    public static JoltStep fromSpec(List<?> specification) {
        Objects.requireNonNull(specification, "specification");
        List<?> snapshot = (List<?>) copy(specification, 0);
        for (Object operation : snapshot) {
            if (!(operation instanceof Map<?, ?> map)
                    || !(map.get("operation") instanceof String name)
                    || !OPERATIONS.contains(name)) {
                throw new IllegalArgumentException("Only built-in operations are supported: " + OPERATIONS);
            }
        }
        Chainr.fromSpec(copy(snapshot, 0));
        return new JoltStep(snapshot);
    }

    /**
     * Transforms a defensive copy. CPU work executes on the subscribing thread;
     * use subscribeOn with an application-chosen scheduler for large documents.
     * @param input JSON-compatible Map/List/scalar tree
     * @param context execution metadata, unused by Jolt
     * @return transformed value; a null Jolt output completes empty, which a
     *         Pipeline reports as a failed step
     */
    @Override
    public Mono<Object> apply(Object input, ExecutionContext context) {
        return Mono.fromCallable(() -> Chainr.fromSpec(copy(specification, 0)).transform(copy(input, 0)));
    }

    private static Object copy(Object value, int depth) {
        if (depth > 128) throw new IllegalArgumentException("JSON tree is cyclic or exceeds depth 128");
        return switch (value) {
            case null -> null;
            case String text -> text;
            case Boolean flag -> flag;
            case Byte number -> number;
            case Short number -> number;
            case Integer number -> number;
            case Long number -> number;
            case BigInteger number when number.getClass() == BigInteger.class -> number;
            case BigDecimal number when number.getClass() == BigDecimal.class -> number;
            case Float number when Float.isFinite(number) -> number;
            case Double number when Double.isFinite(number) -> number;
            case Map<?, ?> map -> {
                Map<String, Object> result = new LinkedHashMap<>();
                map.forEach((key, item) -> {
                    if (!(key instanceof String name)) throw new IllegalArgumentException("JSON object keys must be strings");
                    result.put(name, copy(item, depth + 1));
                });
                yield result;
            }
            case List<?> list -> {
                List<Object> result = new ArrayList<>(list.size());
                list.forEach(item -> result.add(copy(item, depth + 1)));
                yield result;
            }
            default -> throw new IllegalArgumentException("Unsupported JSON value: " + value.getClass().getName());
        };
    }
}
