package org.apache.flink.training.exercises.common.datatypes;

public class DriverFaresByHour {

    public long driverId;
    public float totalFares;
    public Long endWindow;

    @SuppressWarnings("unused")
    public DriverFaresByHour() {
        // default constructor required for serialization
    }

    public DriverFaresByHour(long driverId, float totalFares) {
        this(driverId, totalFares, null);
    }

    public DriverFaresByHour(long driverId, float totalFares, Long endWindow) {
        this.driverId = driverId;
        this.totalFares = totalFares;
        this.endWindow = endWindow;
    }

    @Override
    public String toString() {
        return "DriverFaresByHour{" +
                "driverId=" + driverId +
                ", totalFares=" + totalFares +
                ", endWindow=" + endWindow +
                '}';
    }

}
