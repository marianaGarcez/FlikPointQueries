package aisdata;

import java.io.Serializable;

public class GridCell implements Serializable {
    private static final long serialVersionUID = 1L;  // Adding a serial version UID as best practice

    private double minLatitude;
    private double maxLatitude;
    private double minLongitude;
    private double maxLongitude;
    int count;

    public GridCell(double minLat, double maxLat, double minLon, double maxLon) {
        this.minLatitude = minLat;
        this.maxLatitude = maxLat;
        this.minLongitude = minLon;
        this.maxLongitude = maxLon;
        this.count = 0;
    }

    public boolean contains(double latitude, double longitude) {
        return latitude >= minLatitude && latitude <= maxLatitude
                && longitude >= minLongitude && longitude <= maxLongitude;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        GridCell gridCell = (GridCell) o;

        if (Double.compare(gridCell.minLatitude, minLatitude) != 0) return false;
        if (Double.compare(gridCell.maxLatitude, maxLatitude) != 0) return false;
        if (Double.compare(gridCell.minLongitude, minLongitude) != 0) return false;
        return Double.compare(gridCell.maxLongitude, maxLongitude) == 0;
    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        temp = Double.doubleToLongBits(minLatitude);
        result = (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(maxLatitude);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(minLongitude);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(maxLongitude);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "GridCell{" +
                "minLatitude=" + minLatitude +
                ", maxLatitude=" + maxLatitude +
                ", minLongitude=" + minLongitude +
                ", maxLongitude=" + maxLongitude +
                '}';
    }
}
