/**
 * Copyright 2016 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package io.confluent.examples.streams;

import com.google.common.collect.Lists;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import io.confluent.examples.streams.kafka.EmbeddedSingleNodeKafkaCluster;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end integration test that demonstrates how to perform a join between a KStream and a
 * KTable (think: KStream.leftJoin(KTable)), i.e. an example of a stateful computation.
 *
 * See JoinScalaIntegrationTest for the equivalent Scala example.
 *
 * Note: This example uses lambda expressions and thus works with Java 8+ only.
 */
public class JoinLambdaIntegrationTest {

  @ClassRule
  public static final EmbeddedSingleNodeKafkaCluster CLUSTER = new EmbeddedSingleNodeKafkaCluster();

  private static final String userClicksTopic = "user-clicks";
  private static final String userRegionsTopic = "user-regions";
  private static final String outputTopic = "output-topic";

  @BeforeClass
  public static void startKafkaCluster() throws Exception {
    CLUSTER.createTopic(userClicksTopic);
    CLUSTER.createTopic(userRegionsTopic);
    CLUSTER.createTopic(outputTopic);
  }

  /**
   * Tuple for a region and its associated number of clicks.
   */
  private static final class RegionWithClicks {

    private final String region;
    private final long clicks;

    public RegionWithClicks(String region, long clicks) {
      if (region == null || region.isEmpty()) {
        throw new IllegalArgumentException("region must be set");
      }
      if (clicks < 0) {
        throw new IllegalArgumentException("clicks must not be negative");
      }
      this.region = region;
      this.clicks = clicks;
    }

    public String getRegion() {
      return region;
    }

    public long getClicks() {
      return clicks;
    }

  }

  @Test
  public void shouldCountClicksPerRegion() throws Exception {

    System.out.println(CLUSTER.zookeeperConnect());
    // Input 1: Clicks per user (multiple records allowed per user).
    List<KeyValue<String, Long>> userClicks = Arrays.asList(
        new KeyValue<>("alice", 13L),
        new KeyValue<>("alice", 7L)
    );

    // Input 2: Region per user (multiple records allowed per user).
    List<KeyValue<String, String>> userRegions = Arrays.asList(
        new KeyValue<>("alice", "asia"),   /* Alice lived in Asia originally... */
        new KeyValue<>("alice", "europe") /* ...but moved to Europe some time later. */
    );

    List<KeyValue<String, Long>> expectedClicksPerRegion = Arrays.asList(
        new KeyValue<>("asia", 13L),
        new KeyValue<>("europe", 7L)
    );

    //
    // Step 1: Configure and start the processor topology.
    //
    final Serde<String> stringSerde = Serdes.String();
    final Serde<Long> longSerde = Serdes.Long();

    Properties streamsConfiguration = new Properties();
    streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "join-lambda-integration-test");
    streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
    streamsConfiguration.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, CLUSTER.zookeeperConnect());
    streamsConfiguration.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    streamsConfiguration.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    // Explicitly place the state directory under /tmp so that we can remove it via
    // `purgeLocalStreamsState` below.  Once Streams is updated to expose the effective
    // StreamsConfig configuration (so we can retrieve whatever state directory Streams came up
    // with automatically) we don't need to set this anymore and can update `purgeLocalStreamsState`
    // accordingly.
    streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams");

    // Remove any state from previous test runs
    IntegrationTestUtils.purgeLocalStreamsState(streamsConfiguration);

    KStreamBuilder builder = new KStreamBuilder();

    // This KStream contains information such as "alice" -> 13L.
    //
    // Because this is a KStream ("record stream"), multiple records for the same user will be
    // considered as separate click-count events, each of which will be added to the total count.
    KStream<String, Long> userClicksStream = builder.stream(stringSerde, longSerde, userClicksTopic);

    // This KTable contains information such as "alice" -> "europe".
    //
    // Because this is a KTable ("changelog stream"), only the latest value (here: region) for a
    // record key will be considered at the time when a new user-click record (see above) is
    // received for the `leftJoin` below.  Any previous region values are being considered out of
    // date.  This behavior is quite different to the KStream for user clicks above.
    //
    // For example, the user "alice" will be considered to live in "europe" (although originally she
    // lived in "asia") because, at the time her first user-click record is being received and
    // subsequently processed in the `leftJoin`, the latest region update for "alice" is "europe"
    // (which overrides her previous region value of "asia").
    KTable<String, String> userRegionsTable =
        builder.table(stringSerde, stringSerde, userRegionsTopic);

    // Compute the number of clicks per region, e.g. "europe" -> 13L.
    //
    // The resulting KTable is continuously being updated as new data records are arriving in the
    // input KStream `userClicksStream` and input KTable `userRegionsTable`.
    KStream<String, String> clicksPerRegion = userClicksStream
        // Join the stream against the table.
        //
        // Null values possible: In general, null values are possible for region (i.e. the value of
        // the KTable we are joining against) so we must guard against that (here: by setting the
        // fallback region "UNKNOWN").  In this specific example this is not really needed because
        // we know, based on the test setup, that all users have appropriate region entries at the
        // time we perform the join.
        //
        // Also, we need to return a tuple of (region, clicks) for each user.  But because Java does
        // not support tuples out-of-the-box, we must use a custom class `RegionWithClicks` to
        // achieve the same effect.
        .leftJoin(userRegionsTable, (clicks, region) -> new RegionWithClicks(region == null ? "UNKNOWN" : region, clicks))
        // Change the stream from <user> -> <region, clicks> to <region> -> <clicks>
        .flatMap((user, regionWithClicks) -> {
          ArrayList<KeyValue<String, String>> objects = Lists.newArrayList();
          objects.add(new KeyValue<>(regionWithClicks.getRegion()+"_A", "S:"+regionWithClicks.getClicks()));
          objects.add(new KeyValue<>(regionWithClicks.getRegion()+"_B", "S:"+regionWithClicks.getClicks()));
          return objects;
        });
        // Compute the total per region by summing the individual click counts per region.
        //.reduceByKey(
        //    (firstClicks, secondClicks) -> firstClicks + secondClicks,
        //    stringSerde, longSerde, "ClicksPerRegionUnwindowed");

    // Write the (continuously updating) results to the output topic.
    clicksPerRegion.to(stringSerde, stringSerde, outputTopic);

    KafkaStreams streams = new KafkaStreams(builder, streamsConfiguration);
    streams.start();

    new Thread(() -> {


      //
      // Step 2: Publish user-region information.
      //
      // To keep this code example simple and easier to understand/reason about, we publish all
      // user-region records before any user-click records (cf. step 3).  In practice though,
      // data records would typically be arriving concurrently in both input streams/topics.
      Properties userRegionsProducerConfig = new Properties();
      userRegionsProducerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
      userRegionsProducerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
      userRegionsProducerConfig.put(ProducerConfig.RETRIES_CONFIG, 0);
      userRegionsProducerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
      userRegionsProducerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
      try {
        for (KeyValue<String, String> userRegion : userRegions) {
            IntegrationTestUtils.produceKeyValuesSynchronously(userRegionsTopic, Arrays.asList(userRegions.get(1)), userRegionsProducerConfig);
          System.out.println(userRegion);
        }

      } catch (ExecutionException e) {
        e.printStackTrace();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }).start();

    new Thread(() -> {
      //
      // Step 3: Publish some user click events.
      //
      Properties userClicksProducerConfig = new Properties();
      userClicksProducerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
      userClicksProducerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
      userClicksProducerConfig.put(ProducerConfig.RETRIES_CONFIG, 0);
      userClicksProducerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
      userClicksProducerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, LongSerializer.class);
      try {
        for (KeyValue<String, Long> userClick : userClicks) {
          IntegrationTestUtils.produceKeyValuesSynchronously(userClicksTopic, Arrays.asList(userClick), userClicksProducerConfig);
          System.out.println(userClick);
        }

      } catch (ExecutionException e) {
        e.printStackTrace();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }

    }).start();


      //
      // Step 4: Verify the application's output data.
      //
      Properties consumerConfig = new Properties();
      consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
      consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "join-lambda-integration-test-standard-consumer");
      consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
      consumerConfig.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30000);
      consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
      consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

      new Thread(() -> {
          while (true) {
              try {
                  Thread.sleep(50);
              } catch (InterruptedException e) {
                  e.printStackTrace();
              }
              List<KeyValue<Object, Object>> r = IntegrationTestUtils.readKeyValues(userRegionsTopic, consumerConfig);
              List<KeyValue<Object, Object>> u = IntegrationTestUtils.readKeyValues(userClicksTopic, consumerConfig);
              List<KeyValue<Object, Object>> o = IntegrationTestUtils.readKeyValues(outputTopic, consumerConfig);

              System.out.println(userRegionsTopic+": " + r);
              System.out.println(userClicksTopic+": " + u);
              System.out.println(outputTopic+": " + o);
          }


      }).start();

      List<KeyValue<String, Long>> actualClicksPerRegion = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerConfig,
              outputTopic, expectedClicksPerRegion.size());
      streams.close();
      assertThat(actualClicksPerRegion).containsExactlyElementsOf(expectedClicksPerRegion);
  }

}