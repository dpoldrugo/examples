package io.confluent.examples.streams.pt;

import io.confluent.examples.streams.IntegrationTestUtils;
import io.confluent.examples.streams.kafka.EmbeddedSingleNodeKafkaCluster;

import io.confluent.examples.streams.noc.PojoSerde;
import io.confluent.examples.streams.noc.TrafficMessage;
import io.confluent.examples.streams.noc.TrafficMessageDeserializer;
import io.confluent.examples.streams.noc.TrafficMessageSerializer;
import org.apache.commons.lang.math.RandomUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.apache.kafka.streams.processor.StateStoreSupplier;
import org.apache.kafka.streams.processor.TopologyBuilder;
import org.apache.kafka.streams.KafkaStreams;

import org.apache.kafka.streams.state.Stores;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import java.util.Date;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

/**
 * Created by lmagdic (Lmagdic72@gmail.com) on 08/09/16.
 */
public class PtInputTest {

    @ClassRule
    public static final EmbeddedSingleNodeKafkaCluster CLUSTER = new EmbeddedSingleNodeKafkaCluster();

    // input (source topics)
    private static final String ptiMessagesTopic = "pt-messages";

    // output (sink topics)
    private static final String ptoKaskada0Topic = "pt-kaskada0-topic";
    private static final String ptoDruidAgg = "pt-druidagg-topic";

    //
    // data model serdes (
    //

    // topic key is always string
    private final StringDeserializer stringDeserializer = new StringDeserializer();
    private final StringSerializer   stringSerializer   = new StringSerializer();

    // TrafficMessage (borrowed from noc package)
    private final Deserializer<TrafficMessage> trafficMessageDeserializer = PojoSerde.deserializer(TrafficMessage.class);
    private final   Serializer<TrafficMessage> trafficMessageSerializer   = PojoSerde.serializer  (TrafficMessage.class);

    // PtDruidAgg: SINKS TO DRUID
    private final Deserializer<PtDruidAgg> ptDruidAggDeserializer = PojoSerde.deserializer(PtDruidAgg.class);
    private final   Serializer<PtDruidAgg> ptDruidAggSerializer   = PojoSerde.serializer  (PtDruidAgg.class);
    private final        Serde<PtDruidAgg> ptDruidAggSerde = Serdes.serdeFrom(ptDruidAggSerializer,ptDruidAggDeserializer);


    @BeforeClass
    public static void startKafkaCluster() throws Exception {
        CLUSTER.createTopic(ptiMessagesTopic);
        CLUSTER.createTopic(ptoKaskada0Topic);
        CLUSTER.createTopic(ptoDruidAgg);
    }

    @Test
    public void testPtInput() throws Exception {

        out("testPtInput: BEGIN");

        //
        // STEP 1: Configure kafka strems
        //
        Properties automataConfig = new Properties();
        automataConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, "pt-input-test");
        automataConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        automataConfig.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, CLUSTER.zookeeperConnect());
        automataConfig.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        automataConfig.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        automataConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        //System.out.println(automataConfig); if (true) return;

        // Explicitly place the state directory under /tmp so that we can remove it via
        // `purgeLocalStreamsState` below.  Once Streams is updated to expose the effective
        // StreamsConfig configuration (so we can retrieve whatever state directory Streams came up
        // with automatically) we don't need to set this anymore and can update `purgeLocalStreamsState`
        // accordingly.
        automataConfig.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams");

        // Remove any state from previous test runs
        IntegrationTestUtils.purgeLocalStreamsState(automataConfig);


        //
        // STEP 1b: Build Topology
        //

        StateStoreSupplier druidStoreSupplier = Stores.create("druid-store")
                .withStringKeys()
                .withValues(ptDruidAggSerde)
                .persistent()
                //.windowed()
                .build();

        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder
            .addSource("SOURCE", stringDeserializer, trafficMessageDeserializer, ptiMessagesTopic)

            .addProcessor("DRUID-AGG", () -> new PtDruidAggProcessor(), "SOURCE")

            .addStateStore(druidStoreSupplier, "DRUID-AGG")

            .addSink("SINK", ptoKaskada0Topic, stringSerializer, trafficMessageSerializer, "SOURCE")
            .addSink("SINK-DRUID", ptoDruidAgg, stringSerializer, ptDruidAggSerializer, "DRUID-AGG")
        ;


        //
        // STEP 2: Execute topology
        //
        out("testPtInput: EXECUTE/START AUTOMATA");

        KafkaStreams automata = new KafkaStreams(topologyBuilder, automataConfig);
        automata.start();

        //
        // STEP 3: Generate some input
        //
        List<KeyValue<String, TrafficMessage>> trafficData = Arrays.asList(
                TRAFFIC(1,3), TRAFFIC(2,4), TRAFFIC(1,5), TRAFFIC(1,6), TRAFFIC(2,7)
        );
        IntegrationTestUtils.produceKeyValuesSynchronously(ptiMessagesTopic, trafficData
                , producerConfig(StringSerializer.class, TrafficMessageSerializer.class.getName()));

        //
        // STEP 9: Show output
        //
        Thread.sleep(1000);     // wait 1 sec

        SHOW(ptiMessagesTopic);
        SHOW(ptoKaskada0Topic);
        SHOW(ptoDruidAgg);

        System.out.println();

        // shutdown
        automata.close();
        out("testPtInput: END");
    }

    private void out(String s) {
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        Date date = new Date();
        System.out.println("[" + dateFormat.format(date) + "] " + s);
    }

    private KeyValue<String, TrafficMessage> TRAFFIC(int sequenceId, Integer statusId) {
        return new KeyValue<>(String.valueOf(sequenceId), new TrafficMessage(sequenceId, statusId));
    }

    private void SHOW (String topic) {
        System.out.println();
        System.out.println(topic);
        List<KeyValue<String, String>> list = IntegrationTestUtils.readKeyValues(topic
                , consumerConfig(StringDeserializer.class, StringDeserializer.class.getName()));
        System.out.println(list);
    }

    private void SHOW_TRAFFIC (String topic) {
        System.out.println();
        System.out.println(topic);
        List<KeyValue<String, TrafficMessage>> list = IntegrationTestUtils.readKeyValues(topic
                , consumerConfig(StringDeserializer.class, TrafficMessageDeserializer.class.getName()));
        System.out.println(list);
    }

    private void SHOW_DRUID (String topic) {
        System.out.println();
        System.out.println(topic);
        List<KeyValue<String, PtDruidAgg>> list = IntegrationTestUtils.readKeyValues(topic
                , consumerConfig(StringDeserializer.class, StringDeserializer.class.getName()));
        System.out.println(list);
    }

    private Properties producerConfig(Class keySerializer, String valueSerializer) {
        Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
        producerConfig.put(ProducerConfig.RETRIES_CONFIG, 0);
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializer);
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer);
        return producerConfig;
    }

    private Properties consumerConfig(Class keyDeserializer, String valueDeserializer) {
        Properties consumerConfig = new Properties();
        consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "pt-input-test-standard-consumer-" + valueDeserializer/*.getSimpleName()*/ + "-" + RandomUtils.nextInt(10000));
        consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer);
        consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer);
        //System.out.println(consumerConfig);
        return consumerConfig;
    }

}
