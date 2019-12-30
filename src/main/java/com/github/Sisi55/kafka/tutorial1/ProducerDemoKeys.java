package com.github.Sisi55.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoKeys {

    public static void main(String[] args) {

        final Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);

        String bootstrapServers = "127.0.0.1:9092";

        // create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create Producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        try{
            for (int i = 1; i < 10; i++) {
                // create a producer record
                String topic = "first_topic";
                String value = "hello world key" + Integer.toString(i);
                String key = "id_" + Integer.toString(i);

                ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

                logger.info("Key: " + key); // log the key

                // send data - asynchronous
                producer.send(record, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if (e == null) {
                            logger.info("Received new metadata. \n" +
                                    "Topic:" + recordMetadata.topic() + "\n" +
                                    "Partition:" + recordMetadata.partition() + "\n" +
                                    "Offset:" + recordMetadata.offset() + "\n" +
                                    "Timestamp:" + recordMetadata.timestamp() + "\n");
                        } else {
                            logger.error("Error while producing", e);
                        }
                    }
                }).get(); // block the .send() to make ti synchronous - don't do this in production
            }

        }catch (Exception e){
            logger.error("kafka send error",e);
        }

        // flush data
        producer.flush();

        // flush and close producer
        producer.close();

    }
}
