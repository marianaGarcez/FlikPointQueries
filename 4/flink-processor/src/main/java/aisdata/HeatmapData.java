package aisdata;

public class HeatmapData {
    private final double x;
    private final double y;
    private final long timestamp;

    public HeatmapData(double x, double y, long timestamp) {
        this.x = x;
        this.y = y;
        this.timestamp = timestamp;
    }

    // Getters for x, y, and timestamp
    public double getX() {
        return x;
    }

    public double getY() {
        return y;
    }

    public long getTimestamp() {
        return timestamp;
    }
}