package toptics;

public class Main {   
    public static void main(String[] args) {
        List<Point> points = new ArrayList<>();
        // Add your spatiotemporal data points here
        points.add(new Point(1.0, 2.0, 1000));
        points.add(new Point(1.1, 2.1, 1001));
        // ... more points

        TOptics optics = new TOptics(0.5, 4);
        List<Point> orderedPoints = optics.run(points);

        ClusterExtraction extractor = new ClusterExtraction();
        List<List<Point>> clusters = extractor.extractClusters(orderedPoints, 0.5);

        System.out.println("Number of clusters found: " + clusters.size());
        for (List<Point> cluster : clusters) {
            System.out.println("Cluster size: " + cluster.size());
        }
    }
}

