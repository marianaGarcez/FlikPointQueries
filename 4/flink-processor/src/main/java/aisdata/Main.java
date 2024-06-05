package aisdata;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Properties;

public class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "kafka:9092");
        properties.setProperty("group.id", "flink_consumer");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty("auto.offset.reset", "earliest");

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
            .setBootstrapServers("kafka:9092")
            .setGroupId("flink_consumer")
            .setTopics("aisdata")
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();

        DataStream<String> rawStream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source");

        rawStream.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                logger.info("Received message from Kafka: {}", value);
                return value;
            }
        });

        DataStream<AISData> source = rawStream
            .map(new MapFunction<String, AISData>() {
                @Override
                public AISData map(String value) throws Exception {
                    logger.info("Deserializing message: {}", value);
                    return new AISDataDeserializationSchema().deserialize(value.getBytes());
                }
            })
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<AISData>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                    .withTimestampAssigner(new SerializableTimestampAssigner<AISData>() {
                        @Override
                        public long extractTimestamp(AISData element, long recordTimestamp) {
                            long timestamp = element.getTimestamp();
                            logger.info("Extracted timestamp: {}", timestamp);
                            return timestamp;
                        }
                    })
                    .withIdleness(Duration.ofMinutes(1))
            );

        DataStream<Tuple2<Integer, Double>> averageSpeedByMMSI = source
            .map(new MapFunction<AISData, Tuple2<Integer, Double>>() {
                @Override
                public Tuple2<Integer, Double> map(AISData value) throws Exception {
                    logger.info("Mapping AISData to Tuple2: MMSI={}, Speed={}", value.getMmsi(), value.getSpeed());
                    return new Tuple2<>(value.getMmsi(), value.getSpeed());
                }
            })
            .keyBy(value -> value.f0)
            .window(TumblingEventTimeWindows.of(Time.seconds(10)))
            .aggregate(new AverageAggregator(), new ProcessWindowFunction<Tuple2<Integer, Double>, Tuple2<Integer, Double>, Integer, TimeWindow>() {
                @Override
                public void process(Integer key, Context context, Iterable<Tuple2<Integer, Double>> elements, Collector<Tuple2<Integer, Double>> out) throws Exception {
                    for (Tuple2<Integer, Double> element : elements) {
                        logger.info("Processing window: key={}, value={}", key, element);
                    }
                    for (Tuple2<Integer, Double> element : elements) {
                        out.collect(element);
                    }
                }
            });

        // Write results to PostgreSQL
        averageSpeedByMMSI.addSink(JdbcSink.sink(
            "INSERT INTO aissum (mmsi, speed) VALUES (?, ?) ON CONFLICT (mmsi) DO UPDATE SET speed = EXCLUDED.speed",
            (statement, tuple) -> {
                statement.setInt(1, tuple.f0);
                statement.setDouble(2, tuple.f1);
            },
            JdbcExecutionOptions.builder()
                .withBatchSize(1000)
                .withBatchIntervalMs(200)
                .withMaxRetries(5)
                .build(),
            new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl("jdbc:postgresql://docker.for.mac.host.internal:5438/postgres")
                .withDriverName("org.postgresql.Driver")
                .withUsername("postgres")
                .withPassword("postgres")
                .build()
        ));

        env.execute("Average Speed by MMSI");
        logger.info("Done");
    }
}
