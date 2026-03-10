package net.pincette.json.streams;

import static net.pincette.util.Util.tryToGetRethrow;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import javax.json.JsonObject;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

public final class TestAsserters {

  private TestAsserters() {}

  public static TestAsserter strict() {
    return List::equals;
  }

  public static TestAsserter lenient() {
    return new LenientAsserter();
  }

  private static class LenientAsserter implements TestAsserter {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public boolean test(List<JsonObject> expected, List<JsonObject> actual) {
      final var expectedNodes = asNodes().apply(expected);
      final var actualNodes = asNodes().apply(actual);

      for (JsonNode expectedNode : expectedNodes) {
        final var match =
            actualNodes.stream().anyMatch(actualNode -> matches(expectedNode, actualNode));
        if (!match) {
          return false;
        }
      }
      return true;
    }

    private static Function<List<JsonObject>, List<JsonNode>> asNodes() {
      return objects ->
          objects.stream()
              .map(LenientAsserter::toJsonNode)
              .filter(Optional::isPresent)
              .map(Optional::get)
              .toList();
    }

    private static boolean matches(JsonNode expected, JsonNode actual) {
      if (isNullNode(expected)) {
        return true;
      } else if (isNullNode(actual)) {
        return false;
      } else if (expected.isValueNode()) {
        return expected.equals(actual);
      } else if (expected.isObject()) {
        return actual.isObject() && objectsMatch(expected, actual);
      } else if (expected.isArray()) {
        return actual.isArray() && arraysMatch(expected, actual);
      }
      return true;
    }

    private static boolean isNullNode(JsonNode node) {
      return node == null || node.isNull();
    }

    private static boolean objectsMatch(JsonNode expected, JsonNode actual) {
      final var fields = expected.fieldNames();

      while (fields.hasNext()) {
        final var field = fields.next();
        if (!actual.has(field) || !matches(expected.get(field), actual.get(field))) {
          return false;
        }
      }
      return true;
    }

    private static boolean arraysMatch(JsonNode expected, JsonNode actual) {
      if (expected.size() > actual.size()) {
        return false;
      }

      for (int index = 0; index < expected.size(); index++) {
        if (!matches(expected.get(index), actual.get(index))) {
          return false;
        }
      }
      return true;
    }

    private static Optional<JsonNode> toJsonNode(JsonObject jsonObject) {
      return tryToGetRethrow(() -> MAPPER.readTree(jsonObject.toString()));
    }
  }
}
