package io.github.alex59.pipeline.jolt;

import static org.junit.jupiter.api.Assertions.*;

import io.github.alex59.pipeline.Pipeline;
import io.github.alex59.pipeline.Ordering;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

class JoltStepTest {
    @Test
    void transformsWithoutMutatingInputOrCapturingMutableSpec() {
        Map<String, Object> defaults = new LinkedHashMap<>(Map.of("enabled", true));
        List<Map<String, Object>> spec = new ArrayList<>();
        spec.add(Map.of("operation", "default", "spec", defaults));
        JoltStep step = JoltStep.fromSpec(spec);
        defaults.put("enabled", false);
        spec.clear();
        Map<String, Object> input = new LinkedHashMap<>(Map.of("id", 7));
        Object output = Pipeline.<Object>start().then("defaults", step).run(input).block();
        assertEquals(Map.of("id", 7, "enabled", true), output);
        assertEquals(Map.of("id", 7), input);
    }

    @Test
    void concurrentRunsHaveSeparateTreesAndTransformInstances() {
        JoltStep step = JoltStep.fromSpec(List.of(Map.of("operation", "shift", "spec", Map.of("id", "record.key"))));
        var pipeline = Pipeline.<Object>start().then("shift", (input, context) -> step.apply(input, context).subscribeOn(Schedulers.parallel()));
        var outputs = pipeline.process(Flux.range(0, 100).map(i -> (Object) Map.of("id", i)), 8, Ordering.INPUT).collectList().block();
        assertNotNull(outputs);
        for (int i = 0; i < outputs.size(); i++) {
            assertEquals(Map.of("record", Map.of("key", i)), outputs.get(i));
        }
        assertNotSame(outputs.get(0), outputs.get(1));
    }

    @Test
    void rejectsCustomClassLoadingAndNonJsonTrees() {
        assertThrows(IllegalArgumentException.class, () -> JoltStep.fromSpec(List.of(Map.of("operation", "java.lang.Runtime"))));
        assertThrows(IllegalArgumentException.class, () -> JoltStep.fromSpec(List.of(Map.of("operation", "default", "spec", Map.of("x", new Object())))));
        List<Object> cyclic = new ArrayList<>();
        cyclic.add(cyclic);
        assertThrows(IllegalArgumentException.class, () -> JoltStep.fromSpec(cyclic));
    }
}
