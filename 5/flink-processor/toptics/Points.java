package toptics;


class Point {
    double x, y;  // Spatial coordinates
    long timestamp;  // Temporal coordinate
    double reachabilityDistance = Double.POSITIVE_INFINITY;
    double coreDistance = Double.POSITIVE_INFINITY;
    boolean processed = false;

    public Point(double x, double y, long timestamp) {
        this.x = x;
        this.y = y;
        this.timestamp = timestamp;
    }

    // Distance function combining spatial and temporal distance
    public double distance(Point other) {
        double spatialDistance = Math.sqrt(Math.pow(this.x - other.x, 2) + Math.pow(this.y - other.y, 2));
        double temporalDistance = Math.abs(this.timestamp - other.timestamp);
        return spatialDistance + temporalDistance;
    }
}