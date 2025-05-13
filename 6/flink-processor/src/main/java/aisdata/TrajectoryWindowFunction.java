package aisdata;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import functions.*;
import types.boxes.*;
import types.basic.tpoint.tgeom.*;
import types.basic.tpoint.TPoint.*;
import types.basic.tpoint.tgeom.TGeomPointSeq;

public class TrajectoryWindowFunction extends 
ProcessWindowFunction<Tuple4<Integer, Double, Double, Long>, TGeomPointSeq, Integer, TimeWindow> {

    private static final Logger logger = LoggerFactory.getLogger(TrajectoryWindowFunction.class);
    static error_handler_fn errorHandler = new error_handler();
    private int count = 0;
    
    @Override
    public void process(Integer mmsiKey, Context context, 
                   Iterable<Tuple4<Integer, Double, Double, Long>> elements,
                   Collector<TGeomPointSeq> out) {

        List<Tuple3<Double, Double, Long>> trajectory = new ArrayList<>();
        List<Tuple4<Integer, Double, Double, Long>> sortedElements = new ArrayList<>();
        
        functions.meos_initialize("UTC", errorHandler);

        // First collect all elements
        for (Tuple4<Integer, Double, Double, Long> tuple : elements) {
            sortedElements.add(tuple);
        }
        
        // Sort elements by timestamp to ensure increasing order
        sortedElements.sort((t1, t2) -> Long.compare(t1.f3, t2.f3));
        
        StringBuilder trajbuffer = new StringBuilder();
        boolean firstPoint = true;
        
        for (Tuple4<Integer, Double, Double, Long> tuple : sortedElements) {
            // lon, lat, timestamp for the trajectory points
            trajectory.add(new Tuple3<>(tuple.f1, tuple.f2, tuple.f3));

            String t = convertMillisToTimestamp(tuple.f3);
            String str_pointbuffer = String.format("POINT(%f %f)@%s", tuple.f1, tuple.f2, t);
            //logger.info("CreatePoint: {}", str_pointbuffer);
            //TGeomPointInst point = new TGeomPointInst(str_pointbuffer);

            if (firstPoint) {
                trajbuffer.append("{" + str_pointbuffer);
                firstPoint = false;
            } else {
                trajbuffer.append("," + str_pointbuffer);
            }
        }
        trajbuffer.append("}");

        if (!trajectory.isEmpty()) {
            logger.info("MMSI={}, Window [{} - {}]: {} trajectory points", 
                mmsiKey,
                convertMillisToTimestamp(context.window().getStart()),
                convertMillisToTimestamp(context.window().getEnd()),
                trajectory.size());
        
            logger.info("trajbuffer: {}", trajbuffer);
            TGeomPointSeq trajectoryMEOS = new TGeomPointSeq(trajbuffer.toString());
            logger.info("trajectoryMEOS created");

            out.collect(trajectoryMEOS);
        }
    }
    private String convertMillisToTimestamp(long millis) {
        Instant instant = Instant.ofEpochMilli(millis);
        LocalDateTime dateTime = LocalDateTime.ofInstant(instant, ZoneId.of("UTC"));
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
        return dateTime.format(formatter);
    }
}