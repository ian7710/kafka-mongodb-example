import com.mongodb.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.Properties;

public class MongoDBConsumer {

    private static final String TOPIC_NAME = "processed-data-topic";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String MONGO_URI = "mongodb://localhost:27017";
    private static final String DATABASE_NAME = "iot_data";
    private static final String COLLECTION_NAME = "sensor_data";

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "mongodb-consumer-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        Consumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(TOPIC_NAME));

        MongoClient mongoClient = new MongoClient(new MongoClientURI(MONGO_URI));
        MongoDatabase database = mongoClient.getDatabase(DATABASE_NAME);
        MongoCollection<Document> collection = database.getCollection(COLLECTION_NAME);

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> record : records) {
                    String key = record.key();
                    String value = record.value();

                    // Store processed data in MongoDB
                    Document document = Document.parse(value);
                    collection.insertOne(document);

                    System.out.printf("Stored processed data for key=%s in MongoDB%n", key);
                }
            }
        } finally {
            consumer.close();
            mongoClient.close();
        }
    }
}
