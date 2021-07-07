package de.tu_berlin.dos.arm.watergridsense.jobs.neighbor;

import de.tu_berlin.dos.arm.watergridsense.jobs.utils.*;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer.Semantic;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

public class Run {

    private static final Logger LOG = Logger.getLogger(Run.class);

    public static void main(String[] args) throws Exception {

        BasicConfigurator.configure(); // configure logger
        Properties props = FileReader.GET.read("neighbor_job.properties", Properties.class);
        ParameterTool parameters = ParameterTool.fromArgs(args);

        // Connections configuration ****************************************************************************************

        // kafka configuration
        Properties kafkaConsumerProps = new Properties();
        kafkaConsumerProps.setProperty("bootstrap.servers", props.getProperty("kafka.brokers")); // Broker default host:port
        kafkaConsumerProps.setProperty("group.id", props.getProperty("kafka.consumer.group"));   // Consumer group ID
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

        // Flink configurations ****************************************************************************************

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

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

        // Setup input streams ******************************************************************************************

        // kafka stream for raw measurements
        FlinkKafkaConsumer<SensorData> richConsumer =
            new FlinkKafkaConsumer<>(
                props.getProperty("kafka.topic.rich"),
                new SensorDataEventSchema(),
                kafkaConsumerProps);

        // assign a timestamp extractor to the consumer
        richConsumer.assignTimestampsAndWatermarks(new SensorDataTSExtractor(Time.seconds(60)));

        // Continue to look for suspicious behaviour on the fully enhanced data
        DataStream<AverageEnriched> averageAnnotatedStream = env
            .addSource(richConsumer)
            .partitionCustom(new H3Partitioner(), SensorData::getGridCell)
            .keyBy(SensorData::getGridCell)
            // We want to enrich the data with its rolling average here
            .map(new AverageEnricher())
            .name("Neighborhood-based fault detection");


        // The items in the averageAnnotatedStream now carry their cell-appropriate data with them, so
        // we can stop keying.
        Pattern<AverageEnriched, ?> warningPattern = Pattern.<AverageEnriched>begin("first")
                .where(new SimpleCondition<AverageEnriched>() {
                    @Override
                    public boolean filter(AverageEnriched value) throws Exception {
                        return phishyValue(value);
                    }
                })
                .times(2)
                .within(Time.minutes(15));

        PatternStream<AverageEnriched> warningPatternStream = CEP.pattern(averageAnnotatedStream.keyBy(value -> value.data.getSensorId()), warningPattern);
        DataStream<DataAlert> alertStream = warningPatternStream.process(new PatternProcessFunction<AverageEnriched, DataAlert>() {
            @Override
            public void processMatch(Map<String, List<AverageEnriched>> map, Context context, Collector<DataAlert> collector) throws Exception {
                List<AverageEnriched> l = map.get("first");
                AverageEnriched first = l.get(0);
                AverageEnriched second = l.get(1);

                // If the magnitude of the mean deviation rises we emit an alert.
                if (Math.abs(second.average - second.data.rawValue) > Math.abs(first.average - first.data.rawValue)) {
                    collector.collect(new DataAlert(first.data.getSensorId(), first.data.getTimestamp(), first.data.rawValue, first.average - 10, first.average + 10));
                }
            }
        }).name("Alert stream");

        FlinkKafkaProducer<DataAlert> alertProducer =
            new FlinkKafkaProducer<>(
                props.getProperty("kafka.topic.alarms"),
                (KafkaSerializationSchema<DataAlert>) (dataAlert, aLong) -> new ProducerRecord<>(props.getProperty("kafka.topic.alarms"), dataAlert.toString().getBytes()),
                kafkaProducerProps,
                Semantic.EXACTLY_ONCE);

        // write alerts to appropriate topic
        alertStream.addSink(alertProducer);

        env.execute("WGS4_NeighborJob");
    }
    
    private static class H3Partitioner implements Partitioner<Long> {
        // TODO reconsider implementing a native geocoord structure instead of doing the string conversion
		@Override
		public int partition(final Long gridCell, final int numPartitions) {
            long partitions = Long.valueOf(numPartitions);
			return Math.toIntExact(gridCell % partitions);
		}
	}

    private static boolean phishyValue(AverageEnriched tuple) {
        return Math.abs(tuple.data.rawValue - tuple.average) > 10;
    }
}
