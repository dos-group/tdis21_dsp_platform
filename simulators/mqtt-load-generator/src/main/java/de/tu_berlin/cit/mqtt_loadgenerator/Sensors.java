package de.tu_berlin.cit.mqtt_loadgenerator;

import akka.actor.AbstractActor;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.actor.Status.Failure;
import com.rabbitmq.client.Channel;
import org.apache.log4j.Logger;

import java.util.*;
import java.time.Instant;

class Sensors {

    static final ActorSystem SYSTEM = ActorSystem.create("sensornode");

    public static class SensorActor extends AbstractActor {

        static final Logger LOG = Logger.getLogger(SensorActor.class);
        static final Random RAND = new Random();
        private int id;

        static Props props(int id) {
            return Props.create(SensorActor.class, id);
        }

        static final class Emit {
            String topic;
            Channel channel;

            Emit(String topic, Channel channel) {
                this.topic = topic;
                this.channel = channel;
            }
        }

        public SensorActor(int id) {
            this.id = id;
        }

        @Override
        public Receive createReceive() {

            return receiveBuilder()
                .match(Emit.class, e -> {
                    try {
                        double value = RAND.nextDouble();
                        long time = Instant.now().getEpochSecond();
                        String message = "{ \"n\" : " + id + 
                                        ", \"t \" : " + time + 
                                        ", \"v\" : " + value + "}";
                        e.channel.basicPublish("", e.topic, null, message.getBytes());
                    }
                    catch (Exception ex) {
                        getSender().tell(new Failure(ex), getSelf());
                    }
                })
                .matchAny(o -> LOG.error("received unknown message: " + o))
                .build();
        }
    }
}