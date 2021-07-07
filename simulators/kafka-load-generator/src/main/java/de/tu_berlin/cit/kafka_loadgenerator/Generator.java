package de.tu_berlin.cit.kafka_loadgenerator;

import org.apache.kafka.clients.producer.KafkaProducer;
import scala.concurrent.duration.Duration;

import akka.actor.ActorRef;

import org.apache.log4j.Logger;

import de.tu_berlin.cit.kafka_loadgenerator.Parameters.ParameterActor;
import de.tu_berlin.cit.kafka_loadgenerator.Sensors.SensorActor;

import java.util.concurrent.CountDownLatch;

public enum Generator { GET;

    private static final Logger LOG = Logger.getLogger(Generator.class);

    public void generate(int sensors, long delay, long parameterDelay, int numParams, String sensorTopic, String parameterTopic, KafkaProducer<String, String> kafkaProducer) throws Exception {

        CountDownLatch latch = new CountDownLatch(1);

        ActorRef[] actors = new ActorRef[sensors];
        // create sensor actors
        for (int id = 0; id < sensors ; id++) {
            ActorRef sensorActor = Sensors.SYSTEM.actorOf(SensorActor.props(id));
            LOG.info("Starting sensor actor " + id);
            Sensors.SYSTEM
                    .scheduler()
                    .scheduleWithFixedDelay(
                            Duration.Zero(),
                            Duration.create(delay, "millis"),
                            sensorActor,
                            new SensorActor.Emit(sensorTopic, kafkaProducer),
                            Sensors.SYSTEM.dispatcher(),
                            ActorRef.noSender());
            actors[id] = sensorActor;
        }
        // create update actor
        ActorRef parameterActor = Parameters.SYSTEM.actorOf(ParameterActor.props(sensors, numParams));
        LOG.info("Starting parameter actor");
        Parameters.SYSTEM
            .scheduler()
            .scheduleWithFixedDelay(
                Duration.Zero(),
                Duration.create(parameterDelay, "millis"),
                parameterActor, 
                new ParameterActor.Emit(parameterTopic, kafkaProducer), 
                Parameters.SYSTEM.dispatcher(),
                ActorRef.noSender());

        // don't let the thread die
        latch.await();
    }
}
