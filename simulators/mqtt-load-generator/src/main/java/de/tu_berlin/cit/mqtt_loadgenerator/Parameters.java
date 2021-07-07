package de.tu_berlin.cit.mqtt_loadgenerator;

import akka.actor.AbstractActor;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.actor.Status.Failure;
import com.rabbitmq.client.Channel;
import org.apache.log4j.Logger;

import java.util.*;
import java.time.Instant;

class Parameters {

    static final ActorSystem SYSTEM = ActorSystem.create("parameterupdates");

    public static class ParameterActor extends AbstractActor {

        static final Logger LOG = Logger.getLogger(ParameterActor.class);
        static final Random RAND = new Random();
        private int maxid;

        static Props props(int id) {
            return Props.create(ParameterActor.class, id);
        }

        static final class Emit {
            String topic;
            Channel channel;

            Emit(String topic, Channel channel) {
                this.topic = topic;
                this.channel = channel;
            }
        }

        public ParameterActor(int maxid) {
            this.maxid = maxid;
        }

        @Override
        public Receive createReceive() {

            return receiveBuilder()
                .match(Emit.class, e -> {
                    try {
                        long id = (long) (RAND.nextDouble() * this.maxid);
                        double x = -180 + RAND.nextDouble() * 360;
                        double y = -90 + RAND.nextDouble() * 180;
                        String geolocation = x + "," + y;
                        long time = Instant.now().getEpochSecond();
                        String message = "{ \"n\" : " + id + 
                                        ", \"t \" : " + time +
                                        ", \"p \" : \"" + geolocation + "\"" +
                                        ", \"v\" : \"" + geolocation + "\"}";
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