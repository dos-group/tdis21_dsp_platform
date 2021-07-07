MQTT load generator
===================

This MQTT load generator uses akka to spawn a number of sensor agents that produce randomized measurements with a given delay between each message.

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

The MQTT load generator is used as follows:

```
java -Dsensors=12 -Ddelay=1000 -Dbroker=tcp://130.149.249.40:31774 -Dclientid=wally030 -Dtopic=wgs/events/raw -jar path/to/load-generator.jar
```

where any parameter that is not passed through the command line is read from `src/main/resources/mqtt_loadgenerator.properties`