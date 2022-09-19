package net.pincette.json.streams;

import static com.auth0.jwt.algorithms.Algorithm.RSA256;
import static java.lang.Math.round;
import static java.security.KeyFactory.getInstance;
import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static java.time.Instant.now;
import static java.util.Arrays.stream;
import static java.util.Base64.getDecoder;
import static java.util.Optional.ofNullable;
import static java.util.UUID.randomUUID;
import static java.util.stream.Collectors.joining;
import static javax.json.JsonValue.NULL;
import static net.pincette.json.JsonUtil.createObjectBuilder;
import static net.pincette.json.JsonUtil.getValue;
import static net.pincette.json.JsonUtil.isObject;
import static net.pincette.json.JsonUtil.stringValue;
import static net.pincette.json.JsonUtil.toNative;
import static net.pincette.json.streams.Common.SIGN_JWT;
import static net.pincette.json.streams.Logging.logStageObject;
import static net.pincette.mongo.Expression.function;
import static net.pincette.rs.Mapper.map;
import static net.pincette.rs.PassThrough.passThrough;
import static net.pincette.util.Collections.map;
import static net.pincette.util.ImmutableBuilder.create;
import static net.pincette.util.Pair.pair;
import static net.pincette.util.Util.must;
import static net.pincette.util.Util.tryToGetRethrow;

import com.auth0.jwt.JWT;
import java.security.KeyFactory;
import java.security.interfaces.RSAPrivateKey;
import java.security.spec.PKCS8EncodedKeySpec;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import javax.json.JsonObject;
import javax.json.JsonValue;
import net.pincette.mongo.streams.Stage;
import net.pincette.util.TimedCache;

class SignJwtStage {
  private static final String AS = "as";
  private static final String AUD = "aud";
  private static final int DEFAULT_TTL_SECONDS = 5;
  private static final String CLAIMS = "claims";
  private static final String ISS = "iss";
  private static final String JTI = "jti";
  private static final String KID = "kid";
  private static final String PRIVATE_KEY = "privateKey";
  private static final String SUB = "sub";
  private static final String TTL = "ttl";

  private static final KeyFactory keyFactory =
      tryToGetRethrow(() -> getInstance("RSA")).orElse(null);

  private SignJwtStage() {}

  private static Function<JsonObject, Map<String, Object>> createClaims(
      final JsonObject expression, final Context context) {
    final Map<String, Function<JsonObject, JsonValue>> fields =
        map(
            pair(AUD, field(expression, AUD, context)),
            pair(CLAIMS, field(expression, CLAIMS, context)),
            pair(ISS, field(expression, ISS, context)),
            pair(JTI, field(expression, JTI, context)),
            pair(KID, field(expression, KID, context)),
            pair(SUB, field(expression, SUB, context)));

    return json ->
        map(
            fields.entrySet().stream()
                .map(e -> pair(e.getKey(), e.getValue().apply(json)))
                .filter(pair -> pair.second != NULL)
                .map(pair -> pair(pair.first, toNative(pair.second))));
  }

  private static TimedCache<Map<String, Object>, String> createTimedCache(final Duration ttl) {
    final long cacheTtl = round(ttl.toMillis() * 0.8);

    return new TimedCache<>(ofMillis(cacheTtl), ofMillis(round(cacheTtl * 0.1)));
  }

  private static String extractKey(final String s) {
    return stream(s.split("\\n")).filter(line -> !line.startsWith("-----")).collect(joining());
  }

  private static Function<JsonObject, JsonValue> field(
      final JsonObject expression, final String field, final Context context) {
    return getValue(expression, "/" + field)
        .map(v -> function(v, context.features))
        .orElse(json -> NULL);
  }

  private static Optional<RSAPrivateKey> getPrivateKey(final JsonObject expression) {
    return Optional.ofNullable(expression.getString(PRIVATE_KEY, null))
        .map(SignJwtStage::extractKey)
        .map(key -> getDecoder().decode(key))
        .flatMap(
            key -> tryToGetRethrow(() -> keyFactory.generatePrivate(new PKCS8EncodedKeySpec(key))))
        .map(RSAPrivateKey.class::cast);
  }

  private static Function<JsonObject, String> getToken(
      final JsonObject expression, final Context context) {
    final Function<JsonObject, Map<String, Object>> claims = createClaims(expression, context);
    final RSAPrivateKey privateKey = getPrivateKey(expression).orElse(null);
    final Duration ttl = ofSeconds(expression.getInt(TTL, DEFAULT_TTL_SECONDS));
    final TimedCache<Map<String, Object>, String> cache = createTimedCache(ttl);

    return json -> {
      final Map<String, Object> c = claims.apply(json);

      return cache
          .get(c)
          .orElseGet(
              () -> {
                final String token = sign(c, privateKey, ttl);

                cache.put(c, token);

                return token;
              });
    };
  }

  private static String sign(
      final Map<String, Object> claims, final RSAPrivateKey key, final Duration ttl) {
    final Instant now = now();

    return create(JWT::create)
        .updateIf(
            () -> ofNullable(claims.get(AUD)).filter(List.class::isInstance),
            (b, v) -> b.withAudience(((List<String>) v).toArray(String[]::new)))
        .updateIf(() -> ofNullable(claims.get(ISS)), (b, v) -> b.withIssuer(v.toString()))
        .updateIf(() -> ofNullable(claims.get(KID)), (b, v) -> b.withKeyId(v.toString()))
        .updateIf(() -> ofNullable(claims.get(SUB)), (b, v) -> b.withSubject(v.toString()))
        .updateIf(
            () -> ofNullable(claims.get(CLAIMS)).filter(Map.class::isInstance),
            (b, v) -> b.withPayload((Map<String, Object>) v))
        .update(b -> b.withIssuedAt(now))
        .update(b -> b.withExpiresAt(now.plus(ttl)))
        .update(
            b ->
                b.withJWTId(
                    ofNullable(claims.get(JTI))
                        .map(Object::toString)
                        .orElseGet(() -> randomUUID().toString())))
        .build()
        .sign(RSA256(null, key));
  }

  static Stage signJwtStage(final Context context) {
    return (expression, c) -> {
      if (!isObject(expression)) {
        logStageObject(SIGN_JWT, expression);

        return passThrough();
      }

      final var expr = expression.asJsonObject();

      must(expr.containsKey(AS) && expr.containsKey(PRIVATE_KEY));

      final Function<JsonObject, JsonValue> as = field(expr, AS, context);
      final Function<JsonObject, String> token = getToken(expr, context);

      return map(
          m ->
              stringValue(as.apply(m.value))
                  .map(a -> pair(a, token.apply(m.value)))
                  .map(
                      pair ->
                          m.withValue(
                              createObjectBuilder(m.value).add(pair.first, pair.second).build()))
                  .orElse(m));
    };
  }
}
