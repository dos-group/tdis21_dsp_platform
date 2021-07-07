package de.tu_berlin.dos.arm.watergridsense.jobs.neighbor;

import de.tu_berlin.dos.arm.watergridsense.jobs.utils.SensorData;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

import java.util.HashMap;

public class AverageEnricher extends RichMapFunction<SensorData, AverageEnriched> {
    private static final Logger LOG = Logger.getLogger(Run.class);
    private final double change = 0.1;

    // Map types to their avg values
    private HashMap<Long, HashMap<String, Double>> avgMap;

    @Override
    public void open(final Configuration config) throws Exception {
        // configure logger
        BasicConfigurator.configure();
        avgMap = new HashMap<>();
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    @Override
    public AverageEnriched map(SensorData sensorData) {
        if (!avgMap.containsKey(sensorData.getGridCell())) {
            HashMap<String, Double> hm = new HashMap<>();
            avgMap.put(sensorData.getGridCell(), hm);
        }

        if (!avgMap.get(sensorData.getGridCell()).containsKey(sensorData.type)) {
            avgMap.get(sensorData.getGridCell()).put(sensorData.type, sensorData.rawValue);
        }

        Double historicalAvg = avgMap.get(sensorData.getGridCell()).get(sensorData.type);
        double newAvg =  historicalAvg + (sensorData.rawValue - historicalAvg) * change;

        avgMap.get(sensorData.getGridCell()).put(sensorData.type, newAvg);
        return new AverageEnriched(newAvg, sensorData);
    }
}
