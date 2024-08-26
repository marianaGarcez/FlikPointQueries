import java.util.Objects;
import java.util.Properties;

import javax.naming.Context;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import main.java.aisdata.GridCell;

public class AISDataTimestampAssigner {

    // Grid parameters
    private static final double minLongitude = -180.0;
    private static final double maxLongitude = 180.0;
    private static final double minLatitude = -90.0;
    private static final double maxLatitude = 90.0;

    private static final double gridCellWidth = 0.1;  // Resolution of the grid
    private static final double gridCellHeight = 0.1;

    public static void main(String[] args) throws Exception {
        
        // Setup the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "flink-consumer");

        // Create a stream of vessel data from Kafka
        DataStream<String> rawStream = env.addSource(new FlinkKafkaConsumer<>("vessel_positions", new SimpleStringSchema(), properties));
        
        // Apply a deserialization schema and assign timestamps and watermarks
        DataStream<VesselData> vesselStream = rawStream
            .map(new VesselDataDeserializationSchema())
            .assignTimestampsAndWatermarks(WatermarkStrategy.<VesselData>forBoundedOutOfOrderness(Duration.ofSeconds(30))
                .withTimestampAssigner((event, timestamp) -> event.getTimestamp()));

        // Map each vessel's position to a grid cell
        DataStream<Tuple2<GridCell, Integer>> gridCells = vesselStream
            .map(vessel -> {
                int gridX = (int) ((vessel.getLon() - minLongitude) / gridCellWidth);
                int gridY = (int) ((vessel.getLat() - minLatitude) / gridCellHeight);
                return new Tuple2<>(new GridCell(gridX, gridY), 1);
            });

        // Calculate density within each grid cell using a time window
        DataStream<Tuple2<GridCell, Long>> density = gridCells
            .keyBy(tuple -> tuple.f0)
            .window(TumblingEventTimeWindows.of(Time.minutes(5)))
            .sum(1);

        // Convert the density to a format suitable for heatmap visualization
        DataStream<HeatmapData> heatmapData = density.map(densityData -> {
            return new HeatmapData(densityData.f0.getX(), densityData.f0.getY(), densityData.f1);
        });

        // Assuming you have a sink to output or visualize the heatmap data
        heatmapData.addSink(new HeatmapSink());

        env.execute("Vessel Density Heatmap");
    }
    
    // Inner class for GridCell, assuming latitude/longitude based grid
    public static class GridCell {
        private final int x;
        private final int y;

        public GridCell(int x, int y) {
            this.x = x;
            this.y = y;
        }

        public int getX() {
            return x;
        }

        public int getY() {
            return y;
        }

        @Override
        public int hashCode() {
            return Objects.hash(x, y);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null || getClass() != obj.getClass()) return false;
            GridCell gridCell = (GridCell) obj;
            return x == gridCell.x && y == gridCell.y;
        }
    }

    // Inner class for HeatmapData, assuming it contains x, y, and density count
    public static class HeatmapData {
        private final int x;
        private final int y;
        private final long density;

        public HeatmapData(int x, int y, long density) {
            this.x = x;
            this.y = y;
            this.density = density;
        }

        public int getX() {
            return x;
        }

        public int getY() {
            return y;
        }

        public long getDensity() {
            return density;
        }
    }

    // Example of VesselData class
    public static class VesselData {
        private long timestamp;
        private double latitude;
        private double longitude;

        public VesselData(long timestamp, double latitude, double longitude) {
            this.timestamp = timestamp;
            this.latitude = latitude;
            this.longitude = longitude;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public double getLatitude() {
            return latitude;
        }

        public double getLongitude() {
            return longitude;
        }
    }

    // Example of a deserialization schema
    public static class VesselDataDeserializationSchema implements org.apache.flink.api.common.functions.MapFunction<String, VesselData> {
        @Override
        public VesselData map(String value) throws Exception {
            // Implement the actual deserialization logic here
            // This is a placeholder example, assuming the value is a comma-separated string: "timestamp,latitude,longitude"
            String[] parts = value.split(",");
            long timestamp = Long.parseLong(parts[0]);
            double latitude = Double.parseDouble(parts[1]);
            double longitude = Double.parseDouble(parts[2]);
            return new VesselData(timestamp, latitude, longitude);
        }
    }

    public static class HeatmapSink extends org.apache.flink.streaming.api.functions.sink.SinkFunction<HeatmapData> {
        @Override
        public void invoke(HeatmapData value, Context context) throws Exception {
            // Implement the logic to handle the heatmap data, e.g., save to a file or a visualization tool
        }
    }
}
