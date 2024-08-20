import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Map;

public class PortArrivalDepartureDetection {

    public static void main(String[] args) throws Exception {

        // Setup Flink execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Ingest AIS data
        DataStream<AISData> aisStream = env.addSource(new AISTestSource()); // Assuming AISTestSource is provided

        // Define the port area
        double portLatMin = 50.0;
        double portLatMax = 50.5;
        double portLonMin = 3.0;
        double portLonMax = 3.5;

        // Define the CEP pattern for arrival
        Pattern<AISData, ?> arrivalPattern = Pattern.<AISData>begin("enterPort")
            .where(event -> event.getLatitude() >= portLatMin && event.getLatitude() <= portLatMax
                && event.getLongitude() >= portLonMin && event.getLongitude() <= portLonMax)
            .next("insidePort")
            .where(event -> event.getLatitude() >= portLatMin && event.getLatitude() <= portLatMax
                && event.getLongitude() >= portLonMin && event.getLongitude() <= portLonMax)
            .within(Time.minutes(10));

        // Define the CEP pattern for departure
        Pattern<AISData, ?> departurePattern = Pattern.<AISData>begin("insidePort")
            .where(event -> event.getLatitude() >= portLatMin && event.getLatitude() <= portLatMax
                && event.getLongitude() >= portLonMin && event.getLongitude() <= portLonMax)
            .next("leavePort")
            .where(event -> event.getLatitude() < portLatMin || event.getLatitude() > portLatMax
                || event.getLongitude() < portLonMin || event.getLongitude() > portLonMax)
            .within(Time.minutes(10));

        // Apply the pattern on the AIS data stream for arrival and departure detection
        PatternStream<AISData> arrivalPatternStream = CEP.pattern(aisStream.keyBy(AISData::getMMSI), arrivalPattern);
        PatternStream<AISData> departurePatternStream = CEP.pattern(aisStream.keyBy(AISData::getMMSI), departurePattern);

        // Select and output the events for arrival
        DataStream<String> arrivalStream = arrivalPatternStream.select(
            (PatternSelectFunction<AISData, String>) pattern -> {
                AISData enterPort = pattern.get("enterPort");
                return "Ve ssel " + enterPort.getMMSI() + " arrived at port at " + enterPort.getTimestamp();
            });

        // Select and output the events for departure
        DataStream<String> departureStream = departurePatternStream.select(
            (PatternSelectFunction<AISData, String>) pattern -> {
                AISData leavePort = pattern.get("leavePort");
                return "Vessel " + leavePort.getMMSI() + " departed from port at " + leavePort.getTimestamp();
            });

        // Print the results (or sink to a file, database, etc.)
        arrivalStream.print();
        departureStream.print();

        // Execute the Flink job
        env.execute("Port Arrival and Departure Detection");
    }
}
