Kafka load generator
====================

This kafka load generator uses akka to spawn a number of sensor agents that produce randomized measurements with a given delay between each message.

### Building

To build the package, use maven:

```
mvn clean compile package
```

or just make

```
make
```

after which you will find the compiled artifact in the `target/` directory.

### Running

The kafka load generator is used as follows:

```
java -Dsensors=12 -Ddelay=1000 -Dbroker=130.149.249.40:31774 -Dtopic=wgs-events-raw -jar path/to/load-generator.jar
```

where any parameter that is not passed through the command line is read from `src/main/resources/kafka_loadgenerator.properties`

The following parameters are supported:

- `-Dsensors=10` defines the number of sensors to produce data for
- `-Ddelay=2000` defines the delay between measurements produced by each sensor in ms (i.e. sensors=10, delay=1000 results in 10 messages per second, one from each sensor)
- `-DparameterDelay=2000` defines the delay between parameter updates in ms (i.e. parameterDelay=2000 results in one update being produced for a random sensor every two seconds.)
- `-DnumParams=1` defines the number of different parameters to generate updates for
- `-Dbroker=130.140.249.40:31090` sets the connection string for the broker
- `-DsensorTopic=wgs-events-raw` sets the topic to send the sensor messages to
- `-DparameterTopic=wgs-updates` sets the topic to send the update messages to