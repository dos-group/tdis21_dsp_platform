package de.tu_berlin.cit.mqtt_loadgenerator;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import org.apache.log4j.Logger;

import de.tu_berlin.cit.mqtt_loadgenerator.FileReader;

import java.util.Properties;

public class Run  {

    private static final Logger LOG = Logger.getLogger(Run.class);

    public static void main(String[] args) throws Exception {

        // get properties file
        Properties configuration = FileReader.GET.read("mqtt_loadgenerator.properties", Properties.class);
        
        //String clientId = System.getProperty("clientid")!=null ? System.getProperty("clientid") : configuration.getProperty("generator.id");
        String broker = System.getProperty("broker")!=null ? System.getProperty("broker") : configuration.getProperty("rabbit.broker");
        String topic = System.getProperty("topic")!=null ? System.getProperty("topic") : configuration.getProperty("rabbit.topic");
        String parameterTopic = System.getProperty("parameterTopic")!=null ? System.getProperty("parameterTopic") : configuration.getProperty("rabbit.parameterTopic");
        
        String sensorProp = System.getProperty("sensors")!=null ? System.getProperty("sensors") : configuration.getProperty("generator.sensors");
        int sensors = Integer.parseInt(sensorProp);
        String delayProp = System.getProperty("delay")!=null ? System.getProperty("delay") : configuration.getProperty("generator.delay");
        long delay = Long.parseLong(delayProp);
        String parameterDelayProp = System.getProperty("parameterDelay")!=null ? System.getProperty("parameterDelay") : configuration.getProperty("generator.parameterDelay");
        long parameterDelay = Long.parseLong(parameterDelayProp);

        ConnectionFactory factory = new ConnectionFactory();
        factory.setUri(broker);

        try (Connection connection = factory.newConnection(); Channel channel = connection.createChannel()) {
            channel.queueDeclare(topic, true, false, false, null);
            Generator.GET.generate(
                sensors,
                delay,
                parameterDelay,
                topic,
                parameterTopic,
                channel);

        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}