package aisdata;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AverageAggregator implements AggregateFunction<Tuple2<Integer, Double>, Tuple2<Integer, Tuple2<Double, Integer>>, Tuple2<Integer, Double>> {

    private static final Logger logger = LoggerFactory.getLogger(AverageAggregator.class);

    @Override
    public Tuple2<Integer, Tuple2<Double, Integer>> createAccumulator() {
        logger.info("Creating new accumulator");
        return new Tuple2<>(0, new Tuple2<>(0.0, 0));
    }

    @Override
    public Tuple2<Integer, Tuple2<Double, Integer>> add(Tuple2<Integer, Double> value, Tuple2<Integer, Tuple2<Double, Integer>> accumulator) {
        if (!Double.isNaN(value.f1)) {
            logger.info("Adding value to accumulator: MMSI={}, Speed={}", value.f0, value.f1);
            return new Tuple2<>(value.f0, new Tuple2<>(accumulator.f1.f0 + value.f1, accumulator.f1.f1 + 1));
        } else {
            logger.warn("Transforming NaN value to 0 for MMSI={}", value.f0);
            return new Tuple2<>(value.f0, new Tuple2<>(accumulator.f1.f0 + 0, accumulator.f1.f1 + 1));
        }
    }

    @Override
    public Tuple2<Integer, Double> getResult(Tuple2<Integer, Tuple2<Double, Integer>> accumulator) {
        if (accumulator.f1.f1 == 0) {
            logger.warn("No valid speed values for MMSI={}", accumulator.f0);
            return new Tuple2<>(accumulator.f0, Double.NaN);
        } else {
            logger.info("Calculating result from accumulator: {}", accumulator);
            return new Tuple2<>(accumulator.f0, accumulator.f1.f0 / accumulator.f1.f1);
        }
    }

    @Override
    public Tuple2<Integer, Tuple2<Double, Integer>> merge(Tuple2<Integer, Tuple2<Double, Integer>> a, Tuple2<Integer, Tuple2<Double, Integer>> b) {
        logger.info("Merging accumulators: {} and {}", a, b);
        return new Tuple2<>(a.f0, new Tuple2<>(a.f1.f0 + b.f1.f0, a.f1.f1 + b.f1.f1));
    }
}
