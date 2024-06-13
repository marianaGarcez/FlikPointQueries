package aisdata;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
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
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Properties;

import javax.naming.Context;

import static functions.functions.*;

public class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws Exception {
        meos_initialize("UTC");

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

        rawStream.map(new LogKafkaMessagesMapFunction());

        DataStream<AISData> source = rawStream
            .map(new DeserializeAISDataMapFunction())
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<AISData>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                    .withTimestampAssigner(new AISDataTimestampAssigner())
                    .withIdleness(Duration.ofMinutes(1))
            );

        DataStream<Integer> countStBox = source
            .map(new AISDataToTupleMapFunction())
            .keyBy(value -> 1)
            .window(TumblingEventTimeWindows.of(Time.seconds(10)))
            .aggregate(new CountAggregator(), new ProcessCountWindowFunction());

        countStBox.addSink(JdbcSink.sink(
            "INSERT INTO vesselcountbyareaandtime (area, count, time) VALUES (?::stbox, ?, ?)",            
            (statement, tuple) -> {
                statement.setString(1, "STBOX XT(((3.3615, 53.964367),(16.505853, 59.24544)),[2011-01-03 00:00:00,2011-01-03 00:00:21])");               
                statement.setInt(2, tuple); 
                statement.setTimestamp(3, new java.sql.Timestamp(System.currentTimeMillis()));
            },
            JdbcExecutionOptions.builder()
                .withBatchSize(1000)
                .withBatchIntervalMs(200)
                .withMaxRetries(5)
                .build(),
            new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl("jdbc:postgresql://docker.for.mac.host.internal:5438/mobilitydb")
                .withDriverName("org.postgresql.Driver")
                .withPassword("docker")
                .withUsername("docker")
                .build()
        ));

        env.execute("count Ships in a Temporal Box");
        logger.info("Done");
        meos_finalize();
    }

    // Static nested classes to avoid serialization issues
    public static class LogKafkaMessagesMapFunction implements MapFunction<String, String> {
        private static final Logger logger = LoggerFactory.getLogger(LogKafkaMessagesMapFunction.class);

        @Override
        public String map(String value) throws Exception {
            logger.info("Received message from Kafka: {}", value);
            return value;
        }
    }

    public static class DeserializeAISDataMapFunction implements MapFunction<String, AISData> {
        private static final Logger logger = LoggerFactory.getLogger(DeserializeAISDataMapFunction.class);

        @Override
        public AISData map(String value) throws Exception {
            logger.info("Deserializing message: {}", value);
            return new AISDataDeserializationSchema().deserialize(value.getBytes());
        }
    }

    public static class AISDataTimestampAssigner implements SerializableTimestampAssigner<AISData> {
        private static final Logger logger = LoggerFactory.getLogger(AISDataTimestampAssigner.class);

        @Override
        public long extractTimestamp(AISData element, long recordTimestamp) {
            long timestamp = element.getTimestamp();
            logger.info("Extracted timestamp: {}", timestamp);
            return timestamp;
        }
    }

    public static class AISDataToTupleMapFunction implements MapFunction<AISData, Tuple3<Double, Double, Long>> {
        private static final Logger logger = LoggerFactory.getLogger(AISDataToTupleMapFunction.class);

        @Override
        public Tuple3<Double, Double, Long> map(AISData value) throws Exception {
            logger.info("Mapping AISData to Tuple3: Long={}, Latitude ={}, Timestamp={}", value.getLon(), value.getLat(), value.getTimestamp());
            return new Tuple3<>(value.getLon(), value.getLat(), value.getTimestamp());
        }
    }

    public static class ProcessCountWindowFunction extends ProcessWindowFunction<Integer, Integer, Integer, TimeWindow> {
        private static final Logger logger = LoggerFactory.getLogger(ProcessCountWindowFunction.class);

        @Override
        public void process(Integer key, Context context, Iterable<Integer> elements, Collector<Integer> out) throws Exception {
            for (Integer element : elements) {
                logger.info("Processing window value={}", element);
                out.collect(element);
            }
        }
    }
    
    public static boolean isWithinStBox(double lat, double lon, Long t_out) {
        try {
            // Check if the point is within the STBox bounds (dummy check for example)
            // Replace with actual logic to check if the point is within the bounds

            //TGeogPointInst aPointInst = createPointInst(lat, lon, t_out);
            boolean withinBounds = true;

            //LOG.info("Point ({}, {}) within bounds: {}", lat, lon, withinBounds);
            return withinBounds;
            
        } catch (Exception e) {
            logger.error("Error in isWithinStBox: ", e);
            return false;
        }
    }

}
