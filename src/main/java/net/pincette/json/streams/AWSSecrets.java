package net.pincette.json.streams;

import static com.typesafe.config.ConfigFactory.parseMap;
import static com.typesafe.config.ConfigValueType.STRING;
import static java.util.Optional.ofNullable;
import static net.pincette.json.JsonUtil.from;
import static net.pincette.json.JsonUtil.getString;
import static net.pincette.util.Collections.map;
import static net.pincette.util.Pair.pair;
import static software.amazon.awssdk.services.secretsmanager.SecretsManagerClient.builder;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigValue;
import java.util.Optional;
import net.pincette.util.Pair;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;

class AWSSecrets {
  @SuppressWarnings({"java:S6241", "java:S6242"}) // That comes from the environment.
  private static final SecretsManagerClient client = builder().build();

  private AWSSecrets() {}

  private static Object load(final ConfigValue value) {
    return Optional.of(value)
        .filter(v -> v.valueType() == STRING)
        .map(ConfigValue::unwrapped)
        .map(Object::toString)
        .filter(v -> v.startsWith("arn:aws:secretsmanager:"))
        .map(AWSSecrets::loadSecret)
        .map(Object.class::cast)
        .orElseGet(value::unwrapped);
  }

  static Config load(final Config config) {
    return parseMap(map(config.entrySet().stream().map(e -> pair(e.getKey(), load(e.getValue())))));
  }

  private static String loadSecret(final String id) {
    final Pair<String, String> split = splitId(id);
    final String secret =
        client
            .getSecretValue(GetSecretValueRequest.builder().secretId(split.first).build())
            .secretString();

    return ofNullable(split.second)
        .flatMap(field -> from(secret))
        .flatMap(s -> getString(s, "/" + split.second))
        .orElse(secret);
  }

  private static Pair<String, String> splitId(final String id) {
    return Optional.of(id)
        .map(i -> i.lastIndexOf('['))
        .filter(i -> i != -1)
        .map(i -> pair(id.substring(0, i), id.substring(i + 1, id.length() - 1)))
        .orElseGet(() -> pair(id, null));
  }
}
