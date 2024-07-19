import org.apache.kafka.clients.producer.*;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class SensorDataProducer {

    private static final String TOPIC_NAME = "sensor-data-topic";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);

        try {
            // Simulate sending sensor data every minute
            for (int i = 1; i <= 100; i++) {
                String sensorId = "sensor-" + i;
                long timestamp = System.currentTimeMillis();
                double temperature = Math.random() * 50 + 10; // random temperature between 10 to 60
                double humidity = Math.random() * 100; // random humidity between 0 to 100

                String sensorData = sensorId + "," + timestamp + "," + temperature + "," + humidity;
                ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, sensorId, sensorData);

                // Send data asynchronously
                producer.send(record, (metadata, exception) -> {
                    if (exception != null) {
                        exception.printStackTrace();
                    } else {
                        System.out.printf("Sent record for sensor %s to topic %s%n", sensorId, TOPIC_NAME);
                    }
                });

                Thread.sleep(60000); // sleep for 1 minute
            }
        } finally {
            producer.flush();
            producer.close();
        }
    }
}

