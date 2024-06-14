package aisdata;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple3;


public class CountAggregator implements AggregateFunction<Tuple3<Double, Double, Long>, Integer, Integer> {

    @Override
    public Integer createAccumulator() {
        return 0;
    }

    @Override
    public Integer add(Tuple3<Double, Double, Long > value, Integer accumulator) {
        if (Main.isWithinStBox(value.f0, value.f1, value.f2)) {
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

}
