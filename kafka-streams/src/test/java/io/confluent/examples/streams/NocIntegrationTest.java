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

import io.confluent.examples.streams.kafka.EmbeddedSingleNodeKafkaCluster;
import io.confluent.examples.streams.noc.*;
import org.apache.commons.lang.math.RandomUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end integration test that demonstrates how to perform a join between a KStream and a
 * KTable (think: KStream.leftJoin(KTable)), i.e. an example of a stateful computation.
 *
 * See JoinScalaIntegrationTest for the equivalent Scala example.
 *
 * Note: This example uses lambda expressions and thus works with Java 8+ only.
 */
public class NocIntegrationTest {

  @ClassRule
  public static final EmbeddedSingleNodeKafkaCluster CLUSTER = new EmbeddedSingleNodeKafkaCluster();

  private static final String trafficMessagesTopic = "traffic-messages";
  private static final String deltaMessagesTopic = "delta-messages";
  private static final String outputTopic = "output-topic";
  private static final String outputTopicAgg = "output-topic-agg";

  @BeforeClass
  public static void startKafkaCluster() throws Exception {
    CLUSTER.createTopic(trafficMessagesTopic);
    CLUSTER.createTopic(deltaMessagesTopic);
    CLUSTER.createTopic(outputTopic);
  }

  @Test
  public void shouldJoinTrafficAndDeltas() throws Exception {
    //
    // Step 1: Configure and start the processor topology.
    //
    final Serde<String> stringSerde = Serdes.String();
    final Serde<Long> longSerde = Serdes.Long();
    final Serde<Integer> intSerde = Serdes.Integer();

    final Serde<TrafficMessage> trafficMessageSerde = Serdes.serdeFrom(PojoSerde.serializer(TrafficMessage.class), PojoSerde.deserializer(TrafficMessage.class));
    final Serde<DeltaMessage> deltaMessageSerde = Serdes.serdeFrom(PojoSerde.serializer(DeltaMessage.class), PojoSerde.deserializer(DeltaMessage.class));
    final Serde<JoinMessage> joinMessageSerde = Serdes.serdeFrom(PojoSerde.serializer(JoinMessage.class), PojoSerde.deserializer(JoinMessage.class));

    Properties streamsConfiguration = new Properties();
    streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "noc-integration-test");
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

    KStream<Integer, TrafficMessage> trafficMessagesStream = builder.stream(intSerde, trafficMessageSerde, trafficMessagesTopic);

    KStream<Integer, DeltaMessage> deltaMessagesStream =
        builder.stream(intSerde, deltaMessageSerde, deltaMessagesTopic);

    KStream<Integer, JoinMessage> joinStream = trafficMessagesStream
        .join(deltaMessagesStream, (trafficMessage, deltaMessage) -> {

          JoinMessage joinMessage = new JoinMessage(trafficMessage.getSequenceId(), trafficMessage.getStatusId(), deltaMessage.getCountDelta());

          return joinMessage;
        }, JoinWindows.of("joinName").within(TimeUnit.SECONDS.toMillis(1)), intSerde, trafficMessageSerde, deltaMessageSerde);

    // Write the (continuously updating) results to the output topic.
    joinStream.to(intSerde, joinMessageSerde, outputTopic);

    KStream<Integer, JoinMessage> outStream = builder.stream(intSerde, joinMessageSerde, outputTopic);

    KTable<Integer, JoinMessage> outStreamAgg = outStream.aggregateByKey(() -> new JoinMessage(-1, -1, 0), (aggKey, value, aggregate) -> {
      return new JoinMessage(aggKey, value.getStatusId(), value.getCountDelta() + aggregate.getCountDelta());
    }, intSerde, joinMessageSerde, "join-agg");

    outStreamAgg.to(intSerde, joinMessageSerde, outputTopicAgg);

    KafkaStreams streams = new KafkaStreams(builder, streamsConfiguration);
    streams.start();

    Properties deltaMessagesProducerConfig = deltaMessagesProducerConfig();

    Properties trafficMessagesProducerConfig = trafficMessagesProducerConfig();

    List<KeyValue<Integer, TrafficMessage>> trafficData = Arrays.asList(
            new KeyValue<>(1, new TrafficMessage(1, 3))
    );

    List<KeyValue<Integer, DeltaMessage>> deltaData = Arrays.asList(
            new KeyValue<>(1, new DeltaMessage(1, 1)),
            new KeyValue<>(1, new DeltaMessage(1, 6))

    );

    List<KeyValue<Integer, JoinMessage>> expectedJoinMessages = Arrays.asList(
            new KeyValue<>(1, new JoinMessage(1, 3, 1)),
            new KeyValue<>(1, new JoinMessage(1, 3, 6))
    );

    List<KeyValue<Integer, JoinMessage>> expectedOutMessagesAgg = Arrays.asList(
            new KeyValue<>(1, new JoinMessage(1, 3, 1)),
            new KeyValue<>(1, new JoinMessage(1, 3, 7))
    );


//    IntegrationTestUtils.produceKeyValuesSynchronously(trafficMessagesTopic, Arrays.asList(
//            new KeyValue<>(1, new TrafficMessage(1, 5))
//    ), trafficMessagesProducerConfig);

    IntegrationTestUtils.produceKeyValuesSynchronously(deltaMessagesTopic, deltaData, deltaMessagesProducerConfig);

    IntegrationTestUtils.produceKeyValuesSynchronously(trafficMessagesTopic, trafficData, trafficMessagesProducerConfig);

//    IntegrationTestUtils.produceKeyValuesSynchronously(deltaMessagesTopic, Arrays.asList(
//            new KeyValue<>(1, new DeltaMessage(1, 2))
//    ), deltaMessagesProducerConfig);


    System.out.println(trafficMessagesTopic);
    List<KeyValue<Integer, TrafficMessage>> trafficInTopic = IntegrationTestUtils.readKeyValues(trafficMessagesTopic, consumerConfig(IntegerDeserializer.class, TrafficMessageDeserializer.class));
    System.out.println(trafficInTopic);


    System.out.println(deltaMessagesTopic);
    List<KeyValue<Integer, DeltaMessage>> deltasInTopic = IntegrationTestUtils.readKeyValues(deltaMessagesTopic, consumerConfig(IntegerDeserializer.class, DeltaMessageDeserializer.class));
    System.out.println(deltasInTopic);

    System.out.println(outputTopic);
    List<KeyValue<Integer, DeltaMessage>> outputInTopic = IntegrationTestUtils.readKeyValues(outputTopic, consumerConfig(IntegerDeserializer.class, JoinMessageDeserializer.class));
    System.out.println(outputInTopic);

    System.out.println(outputTopicAgg);
    List<KeyValue<Integer, DeltaMessage>> outputAggInTopic = IntegrationTestUtils.readKeyValues(outputTopicAgg, consumerConfig(IntegerDeserializer.class, JoinMessageDeserializer.class));
    System.out.println(outputAggInTopic);


    List<KeyValue<Integer, JoinMessage>> actualJoinMessages = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerConfig(IntegerDeserializer.class, JoinMessageDeserializer.class),
            outputTopic, expectedJoinMessages.size());

    List<KeyValue<Integer, JoinMessage>> actualOutMessagesAgg = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerConfig(IntegerDeserializer.class, JoinMessageDeserializer.class),
            outputTopicAgg, expectedOutMessagesAgg.size());

    streams.close();
    assertThat(actualJoinMessages).containsExactlyElementsOf(expectedJoinMessages);
    assertThat(actualOutMessagesAgg).containsExactlyElementsOf(expectedOutMessagesAgg);
  }

  private Properties consumerConfig(Class keyDeserializer, Class valueDeserializer) {
    Properties consumerConfig = new Properties();
    consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
    consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "noc-integration-test-standard-consumer-"+valueDeserializer.getSimpleName()+"-"+RandomUtils.nextInt(10000));
    consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer);
    consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer);
    return consumerConfig;
  }

  private Properties trafficMessagesProducerConfig() {
    Properties trafficMessagesProducerConfig = new Properties();
    trafficMessagesProducerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
    trafficMessagesProducerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
    trafficMessagesProducerConfig.put(ProducerConfig.RETRIES_CONFIG, 0);
    trafficMessagesProducerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
    trafficMessagesProducerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, TrafficMessageSerializer.class);
    return trafficMessagesProducerConfig;
  }

  private Properties deltaMessagesProducerConfig() {
    Properties deltaMessagesProducerConfig = new Properties();
    deltaMessagesProducerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
    deltaMessagesProducerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
    deltaMessagesProducerConfig.put(ProducerConfig.RETRIES_CONFIG, 0);
    deltaMessagesProducerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
    deltaMessagesProducerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, DeltaMessageSerializer.class);
    return deltaMessagesProducerConfig;
  }

}