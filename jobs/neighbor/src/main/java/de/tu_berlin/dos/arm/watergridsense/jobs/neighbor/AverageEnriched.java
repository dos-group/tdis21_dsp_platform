package de.tu_berlin.dos.arm.watergridsense.jobs.neighbor;

import de.tu_berlin.dos.arm.watergridsense.jobs.utils.SensorData;

public class AverageEnriched {
    public double average;
    public SensorData data;

    public AverageEnriched(double average, SensorData sensorData) {
        this.average = average;
        this.data = sensorData;
    }
}
