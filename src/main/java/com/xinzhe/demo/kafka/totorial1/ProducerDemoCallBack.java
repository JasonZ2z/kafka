package com.xinzhe.demo.kafka.totorial1;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoCallBack {
    public static void main(String[] args) {
        Logger log = LoggerFactory.getLogger(ProducerDemoCallBack.class);
        // create Producer properties
        Properties properties = new Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        //create producer record
        for (int i = 0; i < 10; ++i) {
            ProducerRecord<String, String> record = new ProducerRecord<>("first_topic", "hello world" + i);
            //send data
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
            });
        }

        //flush data and close
        producer.flush();
        producer.close();

    }
}
