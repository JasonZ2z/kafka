package com.xinzhe.demo.kafka.tutorial1;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * same key going to the same partition
 */
public class ProducerDemoKeys {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Logger log = LoggerFactory.getLogger(ProducerDemoKeys.class);
        // create Producer properties
        Properties properties = new Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        //create producer record


        for (int i = 10; i < 20; ++i) {
            String topic = "first_topic";
            String value = "hello world" + i;
            String key = "id_" + i;
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
            //send data

            log.info("Key: " +  key);
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if(e == null){
                        log.info("\n" + "Receive new metadata. \n"
                                + "Topic: " + recordMetadata.topic() + " \n"
                                + "Partition: " + recordMetadata.partition() + " \n"
                                + "Offset：" + recordMetadata.offset() + " \n"
                                + "TimeStamps: " +recordMetadata.timestamp());
                    } else {
                        log.error("Error while producing", e);
                    }
                }
            }).get();
        }

        //flush data and close
        producer.flush();
        producer.close();

    }
}
