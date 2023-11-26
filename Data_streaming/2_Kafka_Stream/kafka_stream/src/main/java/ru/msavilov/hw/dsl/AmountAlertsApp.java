package ru.msavilov.hw.dsl;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * Приложение на Kafka Streams, которое будет писать в топик сообщение,
 * каждый раз, когда стоимость продаж по одному продукту за минуту будет больше, чем MAX_SUM_PER_MINUTE
 */
public class AmountAlertsApp {
    public static final String PURCHASE_TOPIC_NAME = "purchase";
    public static final String PRODUCT_TOPIC_NAME = "product";
    public static final String RESULT_TOPIC = "product_amount_alerts-dsl";
    public static final String DLQ_TOPIC = "join_dlq-dsl";
    private static final double MAX_AMOUNT_PER_MINUTE = 3000.0;

    public static void main(String[] args) throws InterruptedException {
        var client = new CachedSchemaRegistryClient("http://localhost:8090", 16);
        var serDeProps = Map.of(
                KafkaAvroSerializerConfig.AUTO_REGISTER_SCHEMAS, "true",
                KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8090"
        );

        // строим нашу топологию
        Topology topology = buildTopology(client, serDeProps);
        System.out.println(topology.describe());
        KafkaStreams kafkaStreams = new KafkaStreams(topology, getStreamsConfig());
        CountDownLatch latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread("shutdown-hook") {
            @Override
            public void run() {
                kafkaStreams.close();
                latch.countDown();
            }
        });
        kafkaStreams.setUncaughtExceptionHandler((thread, ex) -> {
            ex.printStackTrace();
            kafkaStreams.close();
            latch.countDown();
        });

        try {
            kafkaStreams.start();
            latch.await();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
        System.exit(0);
    }

    public static Properties getStreamsConfig() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "AmountAlertsAppDSL");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.STATE_DIR_CONFIG, "states");
        return props;
    }

    public static Topology buildTopology(SchemaRegistryClient client, Map<String, String> serDeConfig) {
        var builder = new StreamsBuilder();
        var avroSerde = new GenericAvroSerde(client);
        avroSerde.configure(serDeConfig, false);

        var purchasesStream = builder.stream(
                PURCHASE_TOPIC_NAME,
                Consumed.with(new Serdes.StringSerde(), avroSerde)
        );

        var productsTable = builder.globalTable(
                PRODUCT_TOPIC_NAME,
                Consumed.with(new Serdes.StringSerde(), avroSerde)
        );

        var purchaseWithJoinedProduct = purchasesStream.leftJoin(
                productsTable,
                (key, val) -> val.get("productid").toString(),
                AmountAlertsApp::joinProduct
        );

        purchaseWithJoinedProduct
                .filter((key, val) -> !val.success)
                .mapValues(val -> val.result)
                .to(DLQ_TOPIC, Produced.with(new Serdes.StringSerde(), avroSerde));

        Duration oneMinute = Duration.ofMinutes(1);

        purchaseWithJoinedProduct
                .filter((key, val) -> val.success)
                .mapValues(val -> val.result)
                .groupBy((key, val) -> val.get("product_id").toString(), Grouped.with(new Serdes.StringSerde(), avroSerde))
                .windowedBy(TimeWindows.of(oneMinute).advanceBy(oneMinute))
                .aggregate(
                        () -> 0.0,
                        (key, val, agg) -> agg +=  ((double) val.get("product_price")) * ((Long) val.get("purchase_quantity")).doubleValue(),
                        Materialized.with(new Serdes.StringSerde(), new Serdes.DoubleSerde())
                )
                .filter((key, val) -> val > MAX_AMOUNT_PER_MINUTE)
                .toStream()
                .map((key, val) -> {
                    Schema schema = SchemaBuilder.record("AmountAlert").fields()
                            .name("window_start")
                            .type(LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG)))
                            .noDefault()
                            .requiredDouble("amount")
                            .endRecord();
                    GenericRecord record = new GenericData.Record(schema);
                    record.put("window_start", key.window().start());
                    record.put("amount", (double) val);
                    return KeyValue.pair(key.key(), record);
                })
                .to(RESULT_TOPIC, Produced.with(new Serdes.StringSerde(), avroSerde));

        return builder.build();
    }

    private static JoinResult joinProduct(GenericRecord purchase, GenericRecord product) {
        try {
            Schema schema = SchemaBuilder.record("PurchaseWithProduct").fields()
                    .requiredLong("purchase_id")
                    .requiredLong("purchase_quantity")
                    .requiredLong("product_id")
                    .requiredString("product_name")
                    .requiredDouble("product_price")
                    .endRecord();
            GenericRecord result = new GenericData.Record(schema);
            result.put("purchase_id", purchase.get("id"));
            result.put("purchase_quantity", purchase.get("quantity"));
            result.put("product_id", purchase.get("productid"));
            result.put("product_name", product.get("name"));
            result.put("product_price", product.get("price"));
            return new JoinResult(true, result, null);
        } catch (Exception e) {
            return new JoinResult(false, purchase, e.getMessage());
        }
    }

    private static record JoinResult(boolean success, GenericRecord result, String errorMessage){}
}
