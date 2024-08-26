package aisdata;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class AISTestSource implements SourceFunction<AISData> {

    private volatile boolean isRunning = true;

    @Override
    public void run(SourceContext<AISData> ctx) throws Exception {
        while (isRunning) {
            AISData data = new AISData();
            data.setTimestamp(System.currentTimeMillis());
            data.setMmsi(123456789);
            data.setLon(10.0);
            data.setLat(20.0);
            data.setSpeed(15.0);
            data.setCourse(90.0);
            ctx.collect(data);
            Thread.sleep(1000); // Simulate data arrival every second
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
