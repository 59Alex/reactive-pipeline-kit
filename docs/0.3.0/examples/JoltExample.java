import io.github.alex59.pipeline.Pipeline;
import io.github.alex59.pipeline.jolt.JoltStep;
import java.util.List;
import java.util.Map;

public final class JoltExample {
    public static void main(String[] args) {
        var reshape = JoltStep.fromSpec(List.of(
            Map.of("operation", "shift", "spec", Map.of("id", "record.key"))));
        var pipeline = Pipeline.<Object>start().then("reshape", reshape);
        Map<String, Object> input = Map.of("id", 7);
        Object result = pipeline.run(input).block();
        if (!Map.of("record", Map.of("key", 7)).equals(result)) throw new AssertionError(result);
        if (!Map.of("id", 7).equals(input)) throw new AssertionError("input changed");
        System.out.println("jolt=" + result + "; inputUnchanged=true");
    }
}
