package aisdata;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
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


public class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);
    //public STBox stbx = new STBox("STBOX XT(((3.3615, 53.964367),(16.505853, 59.24544)),[2011-01-03 00:00:00,2011-01-03 00:00:21])");


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

        //--------------------------------------------------------------------------------    

        DataStream<Integer> countStBox = source
            .map(new MapFunction<AISData, Tuple2<Double, Double>>() {
                @Override
                public Tuple2<Double, Double> map(AISData value) throws Exception {
                    logger.info("Mapping AISData to Tuple2: Long={}, Latitude ={}, ", value.getLon(), value.getLat());
                    return new Tuple2<>(value.getLon(), value.getLat());
                }
            })
            .keyBy(value -> 1)
            .window(TumblingEventTimeWindows.of(Time.seconds(10)))
            .aggregate(new CountAggregator(), new ProcessWindowFunction<Integer, Integer, Integer, TimeWindow>() {
                @Override
                public void process(Integer key, Context context, Iterable<Integer> elements, Collector<Integer> out) throws Exception {
                    for (Integer element : elements) {
                        logger.info("Processing window value={}", element);
                        out.collect(element);
                    }
                }
            });
        // Write results to PostgreSQL
        countStBox.addSink(JdbcSink.sink(
            "INSERT INTO vesselcountbyareaandtime (area, count, time) VALUES (?::stbox, ?, ?)",            
            (statement, tuple) -> {
                statement.setString(1, "STBOX XT(((1.0,2.0),(1.0,2.0)),[2001-01-03,2001-01-03])");               
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
    }
}
