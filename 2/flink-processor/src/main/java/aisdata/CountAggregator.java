package aisdata;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CountAggregator implements AggregateFunction<Tuple2<Double, Double>, Integer, Integer> {

    private static final Logger LOG = LoggerFactory.getLogger(CountAggregator.class);

    @Override
    public Integer createAccumulator() {
        return 0;
    }

    @Override
    public Integer add(Tuple2<Double, Double> value, Integer accumulator) {
        if (isWithinStBox(value.f0, value.f1)) {
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

    static boolean isWithinStBox(double lat, double lon) {
        try {
            // Attempt to create an STBox instance from the string
            //STBox stbx = new STBox("STBOX XT(((3.3615, 53.964367),(16.505853, 59.24544)),[2011-01-03 00:00:00,2011-01-03 00:00:21])");

            // Log successful creation of the STBox
            LOG.info("STBox created successfully: {}", stbx.toString());

            // Check if the point is within the STBox bounds (dummy check for example)
            // Replace with actual logic to check if the point is within the bounds
            boolean withinBounds = checkIfWithinBounds(stbx, lat, lon);

            LOG.info("Point ({}, {}) within bounds: {}", lat, lon, withinBounds);
            return withinBounds;
        } catch (Exception e) {
            LOG.error("Error in isWithinStBox: ", e);
            return false;
        }
    }

    static boolean checkIfWithinBounds(STBox stbx, double lat, double lon) {
        return true;
    }
}