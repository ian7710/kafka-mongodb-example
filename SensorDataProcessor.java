import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

import java.util.Properties;

public class SensorDataProcessor {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "sensor-data-processor");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> sensorDataStream = builder.stream("sensor-data-topic");

        // Parse and process sensor data
        sensorDataStream
                .mapValues(value -> {
                    String[] parts = value.split(",");
                    return new SensorData(parts[0], Long.parseLong(parts[1]), Double.parseDouble(parts[2]), Double.parseDouble(parts[3]));
                })
                .groupBy((key, sensorData) -> sensorData.getSensorId(), Serialized.with(Serdes.String(), new JsonSerde<>(SensorData.class)))
                .windowedBy(TimeWindows.of(300000).advanceBy(60000)) // 5-minute window, advanced every minute
                .aggregate(
                        () -> new SensorAggregate(),
                        (key, sensorData, aggregate) -> aggregate.add(sensorData),
                        Materialized.with(Serdes.String(), new JsonSerde<>(SensorAggregate.class))
                )
                .toStream()
                .map((key, aggregate) -> new KeyValue<>(key.key(), aggregate.getAverage()))
                .to("processed-data-topic", Produced.with(Serdes.String(), Serdes.Double()));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
