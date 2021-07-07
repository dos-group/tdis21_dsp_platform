package de.tu_berlin.cit.kafka_loadgenerator;

import org.apache.kafka.clients.producer.KafkaProducer;

import org.apache.log4j.Logger;

import de.tu_berlin.cit.kafka_loadgenerator.FileReader;

import java.util.Properties;

public class Run  {

    private static final Logger LOG = Logger.getLogger(Run.class);

    public static void main(String[] args) throws Exception {

        // get properties file
        Properties configuration = FileReader.GET.read("kafka_loadgenerator.properties", Properties.class);
        
        //String clientId = System.getProperty("clientid")!=null ? System.getProperty("clientid") : configuration.getProperty("generator.id");
        String broker = System.getProperty("broker")!=null ? System.getProperty("broker") : configuration.getProperty("kafka.broker");
        String sensorTopic = System.getProperty("sensorTopic")!=null ? System.getProperty("sensorTopic") : configuration.getProperty("kafka.sensorTopic");
        String parameterTopic = System.getProperty("parameterTopic")!=null ? System.getProperty("parameterTopic") : configuration.getProperty("kafka.parameterTopic");
             
        String sensorProp = System.getProperty("sensors")!=null ? System.getProperty("sensors") : configuration.getProperty("generator.sensors");
        int sensors = Integer.parseInt(sensorProp);
        String delayProp = System.getProperty("delay")!=null ? System.getProperty("delay") : configuration.getProperty("generator.delay");
        long delay = Long.parseLong(delayProp);
        String parameterDelayProp = System.getProperty("parameterDelay")!=null ? System.getProperty("parameterDelay") : configuration.getProperty("generator.parameterDelay");
        long parameterDelay = Long.parseLong(parameterDelayProp);
        int numParams = System.getProperty("numParams")!=null ? Integer.valueOf(System.getProperty("numParams")) : Integer.valueOf(configuration.getProperty("generator.numParams"));

        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", broker);
        kafkaProps.put("acks", "all");
        kafkaProps.put("retries", 0);
        kafkaProps.put("batch.size", 16384);
        kafkaProps.put("linger.ms", 1);
        kafkaProps.put("buffer.memory", 33554432);
        kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        LOG.info("Starting to generate load on broker " + broker);
        LOG.info("Using topic " + sensorTopic + " for " + sensors + " sensors with delay " + delay);
        LOG.info("Using topic " + parameterTopic + " for parameters with delay " + parameterDelay);

        try (KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(kafkaProps)) {
            Generator.GET.generate(
                sensors,
                delay,
                parameterDelay,
                numParams,
                sensorTopic,
                parameterTopic,
                kafkaProducer);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}