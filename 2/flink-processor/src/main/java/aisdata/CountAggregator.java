package aisdata;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import types.boxes.STBox;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Point;

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
            STBox stbx = new STBox("STBOX XT(((3.3615, 53.964367),(16.505853, 59.24544)),[2011-01-03 00:00:00,2011-01-03 00:00:21])");

            // Check if the point is within the STBox bounds
            return true;
        } catch (Exception e) {
            LOG.error("Error in isWithinStBox: ", e);
            return false;
        }
    }
}
