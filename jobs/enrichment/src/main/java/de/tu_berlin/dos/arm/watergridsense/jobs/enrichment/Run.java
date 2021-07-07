package de.tu_berlin.dos.arm.watergridsense.jobs.enrichment;

import com.google.gson.JsonObject;
import de.tu_berlin.dos.arm.watergridsense.jobs.utils.*;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer.Semantic;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

import java.util.Properties;
import java.util.UUID;


public class Run {

    private static final Logger LOG = Logger.getLogger(Run.class);

    public static void main(String[] args) throws Exception {

        BasicConfigurator.configure(); // configure logger
        Properties props = FileReader.GET.read("enrichment_job.properties", Properties.class);
        ParameterTool parameters = ParameterTool.fromArgs(args);

        // Configure connections ****************************************************************

        // kafka configuration
        Properties kafkaConsumerProps = new Properties();
        kafkaConsumerProps.setProperty("bootstrap.servers", props.getProperty("kafka.brokers")); // Broker default host:port
        kafkaConsumerProps.setProperty("group.id", UUID.randomUUID().toString());  // Consumer group ID
        if (parameters.has("latest")) {
            kafkaConsumerProps.setProperty("auto.offset.reset", "latest");                          
        }
        else {
            kafkaConsumerProps.setProperty("auto.offset.reset", "earliest");
        }
        Properties kafkaProducerProps = new Properties();
        kafkaProducerProps.setProperty("bootstrap.servers", props.getProperty("kafka.brokers")); // Broker default host:port
        kafkaProducerProps.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,"900000");
        kafkaProducerProps.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,"5");
        kafkaProducerProps.setProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG,UUID.randomUUID().toString());
        kafkaProducerProps.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        kafkaProducerProps.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "16384");
        kafkaProducerProps.setProperty(ProducerConfig.LINGER_MS_CONFIG, "1000");
        kafkaProducerProps.setProperty(ProducerConfig.BUFFER_MEMORY_CONFIG, "33554432");

        // Configure Flink ****************************************************************

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // simplify visual presentation of execution graph
        env.disableOperatorChaining();

        // setup RocksDB state backend
        String fileBackend = props.getProperty("hdfs.host") + props.getProperty("hdfs.path");
        RocksDBStateBackend backend = new RocksDBStateBackend(fileBackend, true);
        env.setStateBackend((StateBackend) backend);

        // start a checkpoint based on configuration property
        int checkpointInterval = Integer.parseInt(props.getProperty("flink.checkpointInterval"));
        env.enableCheckpointing(checkpointInterval);

        // set mode to exactly-once (this is the default)
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        // checkpoints have to complete within one minute, or are discarded
        env.getCheckpointConfig().setCheckpointTimeout(300000);

        // enable externalized checkpoints which are deleted after job cancellation
        env.getCheckpointConfig().enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);

        // allow job recovery fallback to checkpoint when there is a more recent savepoint
        env.getCheckpointConfig().setPreferCheckpointForRecovery(true);

        // configure event-time and watermarks
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(1000L);

        // Setup input streams ****************************************************************

        DataStream<SensorData> sensorDataStream;

        // create kafka sensor measurement stream
        FlinkKafkaConsumer<JsonObject> rawConsumer =
            new FlinkKafkaConsumer<>(
                props.getProperty("kafka.topic.raw"),
                new RawDataEventSchema(),
                kafkaConsumerProps);
        //rawConsumer.assignTimestampsAndWatermarks(new SensorDataTSExtractor(Time.seconds(60)));
        sensorDataStream = env
            .addSource(rawConsumer)
            .name("Kafka raw sensor data consumer")
            .flatMap(new ParsingFunction())
            .name("Sensor data parsing function");

        // create kafka parameter update stream
        FlinkKafkaConsumer<ParamData> paramUpdateConsumer =
            new FlinkKafkaConsumer<>(
                props.getProperty("kafka.topic.updates"),
                new ParamDataEventSchema(),
                kafkaConsumerProps);
        //paramUpdateConsumer.assignTimestampsAndWatermarks(new ParamDataTSExtractor(Time.seconds(60)));
        DataStream<ParamData> paramUpdateDataStream = env
            .addSource(paramUpdateConsumer)
            .name("Kafka parameter update consumer")
            .keyBy(ParamData::getSensorId);

        // Enrichment ****************************************************************

        // connect and enrich sensor data with parameter data
        DataStream<SensorData> enrichedSensorDataStream = sensorDataStream
            .keyBy(SensorData::getSensorId)
            .connect(paramUpdateDataStream)
            .flatMap(new EnrichmentFunction())
            .name("Enrichment Function");

        // Write to sinks ****************************************************************
        
        FlinkKafkaProducer<SensorData> richProducer =
            new FlinkKafkaProducer<>(
                props.getProperty("kafka.topic.rich"),
                (KafkaSerializationSchema<SensorData>) (sensorData, aLong) -> {
                    return new ProducerRecord<>(props.getProperty("kafka.topic.rich"), sensorData.toString().getBytes());
                },
                kafkaProducerProps,
                Semantic.EXACTLY_ONCE);

        // write back to kafka output topic
        enrichedSensorDataStream
            .addSink(richProducer)
            .name("Kafka enriched sensor data sink");

        env.execute("WGS4_EnrichmentJob");
    }
}
