package toptics;

import java.util.ArrayList;
import java.util.List;

public class ClusterExtraction {
        public List<List<Point>> extractClusters(List<Point> orderedPoints, double epsilon) {
        List<List<Point>> clusters = new ArrayList<>();
        List<Point> currentCluster = new ArrayList<>();

        for (Point point : orderedPoints) {
            if (point.reachabilityDistance > epsilon) {
                if (!currentCluster.isEmpty()) {
                    clusters.add(currentCluster);
                    currentCluster = new ArrayList<>();
                }
            } else {
                currentCluster.add(point);
            }
        }
        if (!currentCluster.isEmpty()) {
            clusters.add(currentCluster);
        }
        return clusters;
    }
}
