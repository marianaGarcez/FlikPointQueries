package aisdata;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static functions.functions.*;
import types.boxes.STBox;


public class CountAggregator implements AggregateFunction<Tuple3<Double, Double, Long>, Integer, Integer> {
    STBox stbx = new STBox("STBOX XT(((3.3615, 53.964367),(16.505853, 59.24544)),[2011-01-03 00:00:00,2011-01-03 00:00:21])");
    private static final Logger LOG = LoggerFactory.getLogger(CountAggregator.class);

    @Override
    public Integer createAccumulator() {
        return 0;
    }

    @Override
    public Integer add(Tuple3<Double, Double, Long > value, Integer accumulator) {
        if (isWithinStBox(value.f0, value.f1, value.f2)) {
            return accumulator + 1;
        } else {
            return accumulator;
        }
    }

    @Override
    public Integer getResult(Integer accumulator) {
        return accumulator;
    }

    @Override
    public Integer merge(Integer a, Integer b) {
        return a + b;
    }

    static boolean isWithinStBox(double lat, double lon, Long t_out) {
        try {
            // Check if the point is within the STBox bounds (dummy check for example)
            // Replace with actual logic to check if the point is within the bounds

            String str_pointbuffer = String.format("POINT(%f %f)@%s", lon, lat, t_out);

            boolean withinBounds = true;

            LOG.info("Point ({}, {}) within bounds: {}", lat, lon, withinBounds);
            return withinBounds;
            
        } catch (Exception e) {
            LOG.error("Error in isWithinStBox: ", e);
            return false;
        }
    }

}
