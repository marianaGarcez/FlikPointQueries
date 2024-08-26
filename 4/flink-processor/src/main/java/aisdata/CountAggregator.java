package aisdata;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class CountAggregator implements Serializable {
    private static final long serialVersionUID = 1L;  // Adding a serial version UID as best practice

    private Map<GridCell, Integer> gridCellCounts;

    public CountAggregator() {
        this.gridCellCounts = new HashMap<>();
    }

    public void addShipToCell(GridCell cell) {
        this.gridCellCounts.put(cell, this.gridCellCounts.getOrDefault(cell, 0) + 1);
    }

    public int getCountForCell(GridCell cell) {
        return this.gridCellCounts.getOrDefault(cell, 0);
    }

    public Map<GridCell, Integer> getAllCounts() {
        return this.gridCellCounts;
    }
}
