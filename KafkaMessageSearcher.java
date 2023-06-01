import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.*;
import java.util.*;

public class KafkaMessageSearcher {
    public static void main(String[] args) {
        String bootstrapServers = "your_kafka_bootstrap_servers";
        String topic = "your_topic_name";
        String keyword = "your_keyword_to_search";

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "message-searcher");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singleton(topic));

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, String> record : records) {
                    if (record.value().contains(keyword)) {
                        System.out.println("Found matching message:");
                        System.out.println("Topic: " + record.topic());
                        System.out.println("Partition: " + record.partition());
                        System.out.println("Offset: " + record.offset());
                        System.out.println("Key: " + record.key());
                        System.out.println("Value: " + record.value());
                    }
                }
            }
        } catch (WakeupException e) {
            // Ignore
        } finally {
            consumer.close();
        }
    }
}
