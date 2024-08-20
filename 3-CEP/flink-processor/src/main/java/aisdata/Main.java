package aisdata;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Properties;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.IterativeCondition.Context;

import static functions.functions.meos_finalize;
import static functions.functions.meos_initialize;
import types.basic.tpoint.tgeom.TGeomPointInst;
import types.boxes.STBox;


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


        // Define the port area
        double portLatMin = 53.964367;
        double portLatMax = 59.24544;
        double portLonMin = 3.3615;
        double portLonMax = 16.505853;

        // DataStream<Integer> countStBox = source
        //     .map(new AISDataToTupleMapFunction())
        //     .keyBy(value -> 1)
        //     .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
        //     .aggregate(new CountAggregator(), new ProcessCountWindowFunction());

        
        // countStBox.print();

        // Define the CEP pattern for arrival
        Pattern<AISData, ?> arrivalPattern = Pattern.<AISData>begin("enterPort")
        .where(new IterativeCondition<AISData>() {
            @Override
            public boolean filter(AISData event, Context<AISData> ctx) throws Exception {
                return event.getLat() >= portLatMin && event.getLat() <= portLatMax
                    && event.getLon() >= portLonMin && event.getLon() <= portLonMax;
            }
        })
        .next("insidePort")
        .where(new IterativeCondition<AISData>() {
            @Override
            public boolean filter(AISData event, Context<AISData> ctx) throws Exception {
                return event.getLat() >= portLatMin && event.getLat() <= portLatMax
                    && event.getLon() >= portLonMin && event.getLon() <= portLonMax;
            }
        })
        .within(Time.minutes(10));

        // Define the CEP pattern for departure
        Pattern<AISData, ?> departurePattern = Pattern.<AISData>begin("insidePort")
        .where(new IterativeCondition<AISData>() {
            @Override
            public boolean filter(AISData event, Context<AISData> ctx) throws Exception {
                return event.getLat() >= portLatMin && event.getLat() <= portLatMax
                    && event.getLon() >= portLonMin && event.getLon() <= portLonMax;
            }
        })
        .next("leavePort")
        .where(new IterativeCondition<AISData>() {
            @Override
            public boolean filter(AISData event, Context<AISData> ctx) throws Exception {
                return event.getLat() < portLatMin || event.getLat() > portLatMax
                    || event.getLon() < portLonMin || event.getLon() > portLonMax;
            }
        })
        .within(Time.minutes(10));
        
        // Apply the pattern on the AIS data stream for arrival and departure detection
        PatternStream<AISData> arrivalPatternStream = CEP.pattern(source.keyBy(AISData::getMmsi), arrivalPattern);
        PatternStream<AISData> departurePatternStream = CEP.pattern(source.keyBy(AISData::getMmsi), departurePattern);

        DataStream<String> arrivalStream = arrivalPatternStream.select(
            (PatternSelectFunction<AISData, String>) pattern -> {
                List<AISData> enterPortList = pattern.get("enterPort");
                if (enterPortList != null && !enterPortList.isEmpty()) {
                    AISData enterPort = enterPortList.get(0);
                    return "Vessel " + enterPort.getMmsi() + " arrived at port at " + enterPort.getTimestamp();
                }
                return "No arrival event detected";
            });

    DataStream<String> departureStream = departurePatternStream.select(
        (PatternSelectFunction<AISData, String>) pattern -> {
            List<AISData> leavePortList = pattern.get("leavePort");
            if (leavePortList != null && !leavePortList.isEmpty()) {
                AISData leavePort = leavePortList.get(0);
                return "Vessel " + leavePort.getMmsi() + " departed from port at " + leavePort.getTimestamp();
            }
            return "No departure event detected";
        });

        // Print the results (or sink to a file, database, etc.)
        arrivalStream.print();
        departureStream.print();
        
            
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

    public static String convertMillisToTimestamp(long millis) {
        Instant instant = Instant.ofEpochMilli(millis);
        LocalDateTime dateTime = LocalDateTime.ofInstant(instant, ZoneId.systemDefault());
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss");
        return dateTime.format(formatter);
    }
    
    public static boolean isWithinStBox(double lat, double lon, Long t_out) {
        //logger.info("Initializing MEOS library");
        meos_initialize("UTC");
        
        STBox stbx = new STBox("STBOX XT(((3.3615, 53.964367),(16.505853, 59.24544)),[2011-01-03 00:00:00,2011-01-03 00:00:21])");
        String t = convertMillisToTimestamp(t_out);
        String str_pointbuffer = String.format("SRID=4326;POINT(%f %f)@%s", lon, lat, t);
        TGeomPointInst point = new TGeomPointInst(str_pointbuffer);
    
        logger.info("CreatePoint: {}", str_pointbuffer);
    
        boolean withinBounds = true;
        //Pointer pointPtr = point.getPointInner();
        //Pointer stboxPtr = ((STBox) stbx).get_inner();
        //withinBounds = eintersects_tpoint_geo(pointPtr, stboxPtr);
        //logger.info("Intersection check completed: {}", withinBounds);
    
        return withinBounds;
    }
    


}
