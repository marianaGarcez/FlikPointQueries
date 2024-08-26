package aisdata;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Properties;
import java.util.Map;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.api.common.typeinfo.Types;

import static functions.functions.meos_finalize;
import static functions.functions.meos_initialize;
import types.basic.tpoint.tgeom.TGeomPointInst;
import types.boxes.STBox;

import org.apache.flink.api.common.typeinfo.TypeInformation;



public class Main {

    static final Logger logger = LoggerFactory.getLogger(Main.class);
    public static void main(String[] args) throws Exception {
        //meos_initialize("UTC");
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
            .setTopics("aisdata4")
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
        

        List<GridCell> grid = List.of(
            new GridCell(53.978483, 56.605908, 3.3615, 10.68075), 
            new GridCell(53.978483, 56.605908, 10.68075, 18.0), 
            new GridCell(56.605908, 59.233333, 3.3615, 10.68075),
            new GridCell(56.605908, 59.233333, 10.68075, 18.0)
        );


        // IGnstantiate CountAggregator to keep track of the number of ships in each cell
        CountAggregator aggregator = new CountAggregator();
        
        source.map(aisData -> {
            double latitude = aisData.getLat();
            double longitude = aisData.getLon();

            // Determine which grid cell the AISData belongs to and update the count
            for (GridCell cell : grid) {
                if (cell.contains(latitude, longitude)) {
                    cell.count +=1;
                    break; // Each ship should belong to only one cell
                }
            }
            for (GridCell cell : grid) {
                logger.info("GridCell: " + cell + " has count: " + cell.count);
            }

            return aisData;
        });
        
        env.execute("Heatmap Generation");
        logger.info("Done");
                   
        //meos_finalize();
    }

    // Static nested classes to avoid serialization issues
    public static class LogKafkaMessagesMapFunction implements MapFunction<String, String> {
       final Logger logger = LoggerFactory.getLogger(LogKafkaMessagesMapFunction.class);

        @Override
        public String map(String value) throws Exception {
            logger.info("Received message from Kafka: {}", value);
            return value;
        }
    }

    public static class DeserializeAISDataMapFunction implements MapFunction<String, AISData> {
        final Logger logger = LoggerFactory.getLogger(DeserializeAISDataMapFunction.class);

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

    public static class ProcessCountWindowFunction extends ProcessWindowFunction<Integer, Integer, Integer, TimeWindow> {
        final Logger logger = LoggerFactory.getLogger(ProcessCountWindowFunction.class);

        @Override
        public void process(Integer key, Context context, Iterable<Integer> elements, Collector<Integer> out) throws Exception {
            for (Integer element : elements) {
                logger.info("Processing window value={}", element);
                out.collect(element);
            }
        }
    }

    public static String convertMillisToTimestamp(long millis) {
        Instant instant = Instant.ofEpochMilli(millis);
        LocalDateTime dateTime = LocalDateTime.ofInstant(instant, ZoneId.systemDefault());
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss");
        return dateTime.format(formatter);
    }

}
