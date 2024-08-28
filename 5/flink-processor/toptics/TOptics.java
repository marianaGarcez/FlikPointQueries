package toptics;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.PriorityQueue;

class TOptics {
    private double epsilon;  // Maximum spatial/temporal distance
    private int minPoints;  // Minimum number of points to form a cluster

    public TOptics(double epsilon, int minPoints) {
        this.epsilon = epsilon;
        this.minPoints = minPoints;
    }

    public List<Point> run(List<Point> points) {
        List<Point> orderedPoints = new ArrayList<>();

        for (Point point : points) {
            if (!point.processed) {
                List<Point> neighbors = getNeighbors(point, points);

                point.processed = true;
                orderedPoints.add(point);

                if (point.coreDistance <= epsilon) {
                    PriorityQueue<Point> seeds = new PriorityQueue<>(
                        (a, b) -> Double.compare(a.reachabilityDistance, b.reachabilityDistance)
                    );
                    update(point, neighbors, seeds, points);

                    while (!seeds.isEmpty()) {
                        Point q = seeds.poll();
                        List<Point> qNeighbors = getNeighbors(q, points);
                        q.processed = true;
                        orderedPoints.add(q);

                        if (q.coreDistance <= epsilon) {
                            update(q, qNeighbors, seeds, points);
                        }
                    }
                }
            }
        }
        return orderedPoints;
    }

    private List<Point> getNeighbors(Point point, List<Point> points) {
        List<Point> neighbors = new ArrayList<>();
        for (Point other : points) {
            if (point != other && point.distance(other) <= epsilon) {
                neighbors.add(other);
            }
        }
        return neighbors;
    }

    private void update(Point point, List<Point> neighbors, PriorityQueue<Point> seeds, List<Point> points) {
        point.coreDistance = calculateCoreDistance(point, neighbors);
        for (Point neighbor : neighbors) {
            if (!neighbor.processed) {
                double newReachabilityDistance = Math.max(point.coreDistance, point.distance(neighbor));
                if (neighbor.reachabilityDistance == Double.POSITIVE_INFINITY) {
                    neighbor.reachabilityDistance = newReachabilityDistance;
                    seeds.add(neighbor);
                } else if (newReachabilityDistance < neighbor.reachabilityDistance) {
                    neighbor.reachabilityDistance = newReachabilityDistance;
                    // Re-sort the queue
                    seeds.remove(neighbor);
                    seeds.add(neighbor);
                }
            }
        }
    }

    private double calculateCoreDistance(Point point, List<Point> neighbors) {
        if (neighbors.size() < minPoints) return Double.POSITIVE_INFINITY;
        Collections.sort(neighbors, (a, b) -> Double.compare(point.distance(a), point.distance(b)));
        return point.distance(neighbors.get(minPoints - 1));
    }
}
