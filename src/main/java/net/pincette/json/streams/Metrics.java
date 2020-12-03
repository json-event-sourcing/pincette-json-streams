package net.pincette.json.streams;

import static java.lang.management.ManagementFactory.getPlatformMBeanServer;
import static java.time.Instant.now;
import static java.util.Arrays.stream;
import static java.util.UUID.randomUUID;
import static java.util.stream.Collectors.toMap;
import static net.pincette.jes.util.JsonFields.TIMESTAMP;
import static net.pincette.json.JsonUtil.createObjectBuilder;
import static net.pincette.json.JsonUtil.createValue;
import static net.pincette.util.Collections.map;
import static net.pincette.util.Pair.pair;
import static net.pincette.util.Util.tryToGetRethrow;
import static net.pincette.util.Util.tryToGetSilent;

import java.lang.reflect.Array;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Timer;
import java.util.TimerTask;
import java.util.function.Supplier;
import java.util.stream.Stream;
import javax.json.JsonObject;
import javax.management.MBeanFeatureInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.openmbean.CompositeDataSupport;
import net.pincette.util.Collections;
import net.pincette.util.Pair;
import net.pincette.util.StreamUtil;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

class Metrics {
  private static final String HOSTNAME = getHostname();
  private static final String SOURCE = "_source";

  private final Duration interval;
  private final KafkaProducer<String, JsonObject> producer;
  private final Timer timer = new Timer(true);
  private final String topic;

  Metrics(
      final String topic,
      final KafkaProducer<String, JsonObject> producer,
      final Duration interval) {
    this.topic = topic;
    this.producer = producer;
    this.interval = interval;
  }

  private static Map<String, Object> convert(final CompositeDataSupport data) {
    return data.getCompositeType().keySet().stream()
        .map(k -> pair(k, toValue(data.get(k))))
        .filter(pair -> pair.second != null)
        .collect(toMap(pair -> pair.first, pair -> pair.second));
  }

  private static Map<String, Object> convert(final Map<Object, Object> data) {
    return data.entrySet().stream()
        .map(e -> pair(getKey(e.getKey()), toValue(e.getValue())))
        .filter(pair -> pair.second != null)
        .collect(toMap(pair -> pair.first, pair -> pair.second));
  }

  private static JsonObject getAttributes() {
    final MBeanServer server = getPlatformMBeanServer();

    return server.queryNames(null, null).stream()
        .map(name -> pair(name, tryToGetRethrow(() -> server.getMBeanInfo(name)).orElse(null)))
        .map(
            pair ->
                pair(
                    pair.first.getCanonicalName(),
                    getAttributeValues(pair.first, getAttributes(pair.second), server)))
        .map(Metrics::splitDomain)
        .collect(toMap(pair -> pair.first, pair -> pair.second, Collections::merge))
        .entrySet()
        .stream()
        .reduce(
            createObjectBuilder().add(TIMESTAMP, now().toString()).add(SOURCE, source()),
            (b, e) -> b.add(e.getKey(), createValue(toValue(e.getValue()))),
            (b1, b2) -> b1)
        .build();
  }

  private static Map<String, Object> getAttributeValues(
      final ObjectName name, final Stream<String> names, final MBeanServer server) {
    return names
        .map(n -> pair(n, tryToGetSilent(() -> server.getAttribute(name, n)).orElse(null)))
        .filter(pair -> pair.second != null)
        .collect(toMap(pair -> pair.first, pair -> pair.second));
  }

  private static Stream<String> getAttributes(final MBeanInfo bean) {
    return stream(bean.getAttributes())
        .map(MBeanFeatureInfo::getName)
        .filter(n -> !n.equals("LastGcInfo")); // Uses internal Sun class.
  }

  private static String getHostname() {
    return tryToGetSilent(NetworkInterface::getNetworkInterfaces)
        .map(StreamUtil::stream)
        .flatMap(s -> s.flatMap(i -> StreamUtil.stream(i.getInetAddresses())).findAny())
        .map(InetAddress::getHostName)
        .orElse(null);
  }

  private static String getKey(final Object key) {
    return key instanceof List ? ((List) key).get(0).toString() : key.toString();
  }

  private static String source() {
    return "pincette-json-streams" + (HOSTNAME != null ? ("-" + HOSTNAME) : "");
  }

  private static Pair<String, Map<String, Object>> splitDomain(
      final Pair<String, Map<String, Object>> attributes) {
    return Optional.of(attributes.first.indexOf(':'))
        .filter(i -> i != -1)
        .map(
            i ->
                pair(
                    attributes.first.substring(0, i),
                    map(pair(attributes.first.substring(i + 1), (Object) attributes.second))))
        .orElse(attributes);
  }

  private static List<Object> toList(final Object array) {
    final List<Object> result = new ArrayList<>();

    for (int i = 0; i < Array.getLength(array); ++i) {
      result.add(toValue(Array.get(array, i)));
    }

    return result;
  }

  private static Object toValue(final Object value) {
    final Supplier<Object> tryArray =
        () -> value != null && value.getClass().isArray() ? toList(value) : value;
    final Supplier<Object> tryComposite =
        () ->
            value instanceof CompositeDataSupport
                ? convert((CompositeDataSupport) value)
                : tryArray.get();

    return value instanceof Map ? convert((Map) value) : tryComposite.get();
  }

  void start() {
    timer.schedule(
        new TimerTask() {
          @Override
          public void run() {
            producer.send(new ProducerRecord<>(topic, randomUUID().toString(), getAttributes()));
          }
        },
        0,
        interval.toMillis());
  }
}
