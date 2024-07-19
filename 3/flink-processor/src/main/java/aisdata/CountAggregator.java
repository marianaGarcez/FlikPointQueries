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
        boolean result = Main.isWithinStBox(value.f0, value.f1, value.f2);
        // now check if it was true before, and now it is false, the ship left the box
        if (result == true) { {
            return 
        }
        // now check if it was false before, and now it is true, the ship entered the box
        } else if (result == false) {
            return accumulator + 1;
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
