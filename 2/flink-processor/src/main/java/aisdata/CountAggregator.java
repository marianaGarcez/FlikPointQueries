package aisdata;

import javax.naming.OperationNotSupportedException;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CountAggregator implements AggregateFunction<Tuple3<Double, Double, Long>, Integer, Integer> {
    private static final Logger logger = LoggerFactory.getLogger(CountAggregator.class);

    @Override
    public Integer createAccumulator() {
        return 0;
    }

    public Integer add(Tuple3<Double, Double, Long > value, Integer accumulator) {
        try {
            boolean result = Main.isWithinStBox(value.f0, value.f1, value.f2);
            if (result)  {
                return accumulator + 1;
            } else {
                return accumulator;
            }
        } catch (OperationNotSupportedException e) {
            logger.error("Operation not supported: {}", e.getMessage());
            return accumulator; // Or handle it as per your requirement
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
