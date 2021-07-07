package de.tu_berlin.dos.arm.watergridsense.jobs.enrichment;

import de.tu_berlin.dos.arm.watergridsense.jobs.utils.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.apache.commons.math3.analysis.polynomials.PolynomialSplineFunction;
import org.apache.commons.math3.analysis.interpolation.LinearInterpolator;

import com.datastax.driver.core.Cluster.Builder;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisHashCommands;

import com.uber.h3core.H3Core;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.ArrayList;
import java.util.regex.*;

public class EnrichmentFunction extends RichCoFlatMapFunction<SensorData, ParamData, SensorData> {

    /**
     *
     */
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = Logger.getLogger(EnrichmentFunction.class);
    private Map<String, ParamCache> paramState;
    private transient PolynomialSplineFunction conversionFunction;
    private transient LinearInterpolator li;
    private transient Properties props;
    private transient RedisClient redisClient;
    private transient StatefulRedisConnection<String, String> redisConnection;
    private transient RedisHashCommands<String, String> redisHashCommands;
    private transient H3Core h3;
    private int gridResolution;

    @Override
    public void open(final Configuration config) throws Exception {
        // configure logger
        BasicConfigurator.configure();
        props = FileReader.GET.read("enrichment_job.properties", Properties.class);
        // connect to redis
        //RedisURI redisUri = RedisURI.builder()
        //        .withHost(props.getProperty("redis.host"))
        //        .withPort(Integer.parseInt(props.getProperty("redis.port")))
        //        .build();
        //redisClient = RedisClient.create(redisUri);
        //redisConnection = redisClient.connect();
        //redisHashCommands = redisConnection.sync();
        paramState = new HashMap<>();
        li = new LinearInterpolator();
        h3 = H3Core.newInstance();
        gridResolution = Integer.parseInt(props.getProperty("h3.resolution"));
    }
    
    @Override
    public void close() throws Exception {
        //redisConnection.close();
        //redisClient.shutdown();
        super.close();
    }

    @Override
    public void flatMap1(SensorData sensorData, Collector<SensorData> out) throws Exception {

        String sensorid = sensorData.getSensorId();
        SensorData enrichedSensorData = new SensorData(sensorid, sensorData.timestamp, sensorData.rawValue);

        // if no local value exists, load it from redis
        if (paramState.isEmpty()){
            try {
                //Map<String, String> parameters = redisHashCommands.hgetall(sensorid);
                //for (Map.Entry<String, String> entry : parameters.entrySet()) {
                    String parameterName = "conversion";
                    String conversionValue = "[(-1000, 4.166056203999194), (0, 3.4508407298513593), (1000, 4.764646192870792)]@1624441509626";
                    String[] parameterValue = conversionValue.split("@");
                    ParamCache paramCache = new ParamCache();
                    paramCache.current = new Tuple2<>(new Date(Long.parseLong(parameterValue[1])), parameterValue[0]);
                    paramState.put(parameterName, paramCache);
                    // initialization of conversion function
                    if (parameterName.equals("conversion")) {
                        initConversion(parameterValue[0]);
                    }

                    parameterName = "geolocation";
                    String geolocationValue = "104.74207807758722,41.12000077048168@1624444844626";
                    String[] parameterValue2 = geolocationValue.split("@");
                    paramCache = new ParamCache();
                    paramCache.current = new Tuple2<>(new Date(Long.parseLong(parameterValue2[1])), parameterValue2[0]);
                    paramState.put(parameterName, paramCache);
            }
            catch (Exception e) {
                LOG.error(e);
            }
        }
        
        for (String parameter : paramState.keySet()) {
            String parameterValue = "NOPARAM";

            // check if current value of paramCache is correct
            if (paramState.get(parameter).current.f0.before(sensorData.timestamp)) {
                parameterValue = paramState.get(parameter).current.f1;
            }
            // in almost all cases, the previous one will be the correct one
            else if (paramState.get(parameter).previous != null &&
                    paramState.get(parameter).previous.f0.before(sensorData.timestamp)) {
                parameterValue = paramState.get(parameter).previous.f1;
            }
            // in the worst case, we default to the current cache value
            else {
                parameterValue = paramState.get(parameter).current.f1;
            }

            // TODO improve error handling, esp. "NOPARAM" cases
            try {
                switch (parameter) {
                    case "geolocation":
                        if (!parameterValue.equals("NOPARAM")) {
                            enrichedSensorData.location = parameterValue;
                            // compute cell for neighborhood-based calculations
                            String[] latlong = parameterValue.split(",");
                            double latitude = Double.parseDouble(latlong[0]);
                            double longitude = Double.parseDouble(latlong[1]);
                            enrichedSensorData.gridCell = h3.geoToH3(latitude, longitude, gridResolution);
                        }
                        break;
                    case "cell":
                        if (!parameterValue.equals("NOPARAM")) {
                            enrichedSensorData.gridCell = Long.parseLong(parameterValue);
                        }
                        break;
                    case "type":
                        enrichedSensorData.type = parameterValue;
                        break;
                    case "unit":
                        enrichedSensorData.unit = parameterValue;
                        break;
                    case "conversion":
                        if (!parameterValue.equals("NOPARAM")) {
                            enrichedSensorData.conValue = conversionFunction.value(enrichedSensorData.rawValue);
                        }
                        break;
                }
            }
            catch(Exception ex) {
                LOG.error("Error applying attribute \"" + parameter + "\" with value " + parameterValue);
                LOG.error(ex.getMessage());
                LOG.error("Complete sensor packet: " + sensorData.toString());
            }
        }
        out.collect(enrichedSensorData);
    }

    @Override
    public void flatMap2(ParamData paramData, Collector<SensorData> out) {
        ParamCache cachedParam = paramState.get(paramData.parameter);
        if(cachedParam == null) {
            cachedParam = new ParamCache();
        }
        // old "current" value is now "previous"
        cachedParam.previous = cachedParam.current;
        // set new "current" value to received parameter
        cachedParam.current = new Tuple2<>(paramData.timestamp, paramData.value);
        paramState.put(paramData.parameter, cachedParam);
        if (paramData.parameter.equals("conversion")) {
            initConversion(paramData.value);
        }

/*        // update redis
        try {
            redisHashCommands.hset(
                paramData.sensorId,
                paramData.parameter,
                paramData.value + "@" + paramData.timestamp.getTime());
        }
        catch (Exception e) {
            LOG.error(e);
        }*/
    }

    // this function initializes the linear interpolation for sensor data conversion
    // by creating a java PolynomialSplineFunction
    private void initConversion(String parameterValue) {
        // TODO find a much less silly way of doing this, i.e.,
        // create a separate table to store the curve points more efficiently?
        // https://stackoverflow.com/questions/64701582/how-to-store-curve-to-database
        Pattern pairPattern = Pattern.compile("-?\\d+\\.*\\d*,\\s*-?\\d+\\.*\\d*");
        Matcher pairMatcher = pairPattern.matcher(parameterValue);
        List<Double> xList = new ArrayList<Double>();
        List<Double> yList = new ArrayList<Double>();
        while (pairMatcher.find()) {
            String pair = pairMatcher.group();
            Pattern numberPattern = Pattern.compile("-?\\d+\\.*\\d*");
            Matcher numberMatcher = numberPattern.matcher(pair);
            // first match is x value
            numberMatcher.find();
            xList.add(Double.valueOf(numberMatcher.group()));
            // second match is y value
            numberMatcher.find();
            yList.add(Double.valueOf(numberMatcher.group()));
        }
        double[] x = new double[xList.size()];
        double[] y = new double[yList.size()];
        for (int i = 0; i < x.length; i++) {
            x[i] = xList.get(i);
            y[i] = yList.get(i);
        }
        conversionFunction = li.interpolate(x, y);
    }
}