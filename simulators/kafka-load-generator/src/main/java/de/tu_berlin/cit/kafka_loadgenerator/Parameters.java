package de.tu_berlin.cit.kafka_loadgenerator;

import akka.actor.AbstractActor;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.actor.Status.Failure;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;

import java.util.*;

class Parameters {

    static final ActorSystem SYSTEM = ActorSystem.create("parameterupdates");

    public static class ParameterActor extends AbstractActor {

        static final Logger LOG = Logger.getLogger(ParameterActor.class);
        static final Random RAND = new Random();
        private int maxid;
        private int numParams;

        static Props props(int id, int numParams) {
            return Props.create(ParameterActor.class, id, numParams);
        }

        static final class Emit {
            String topic;
            KafkaProducer<String, String> kafkaProducer;

            Emit(String topic, KafkaProducer<String, String> kafkaProducer) {
                this.topic = topic;
                this.kafkaProducer = kafkaProducer;
            }
        }

        public ParameterActor(int maxid, int numParams) {
            this.maxid = maxid;
            this.numParams = numParams;
        }

        @Override
        public Receive createReceive() {

            return receiveBuilder()
                .match(Emit.class, e -> {
                    try {
                        long time = System.currentTimeMillis();
                        long id = (long) (RAND.nextDouble() * this.maxid);
                        int param = (int) (RAND.nextDouble() * this.numParams);
                        String parameterName = "";
                        String parameterValue = "";
                        switch (param) {
                            case 0:
                                parameterName = "geolocation";
                                double x = -180 + RAND.nextDouble() * 360;
                                double y = -90 + RAND.nextDouble() * 180;
                                parameterValue = x + "," + y;
                                break;
                            case 1:
                                parameterName = "type";
                                parameterValue = "TMP36";
                                break;
                            case 2:
                                parameterName = "unit";
                                parameterValue = "K";
                                break;
                            case 3:
                                parameterName = "conversion";
                                parameterValue = Double.toString(RAND.nextDouble());
                                break;
                        }
                        String message = "{ \"n\" : " + id + 
                                        ", \"t\" : " + time +
                                        ", \"p\" : \"" + parameterName + "\"" +
                                        ", \"v\" : \"" + parameterValue + "\"}";
                        e.kafkaProducer.send(new ProducerRecord<String, String>(e.topic, String.valueOf(id), message));
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