package net.pincette.json.streams;

import java.util.List;
import java.util.function.BiPredicate;
import javax.json.JsonObject;

public interface TestAsserter extends BiPredicate<List<JsonObject>, List<JsonObject>> {}
