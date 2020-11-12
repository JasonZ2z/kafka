package tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThread {
    Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class.getName());
    public ConsumerDemoWithThread() {
    }

    public void run(){
        CountDownLatch latch = new CountDownLatch(1);
        String topic = "first_topic";
        logger.info("Creating the consumer threadd");
        ConsumerThread myConsumerThread = new ConsumerThread(latch, topic);
        new Thread(myConsumerThread).start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                    logger.info("Caught shotdown hook");
                    myConsumerThread.shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info("APP has exited");
        }
        ));
        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("App got interrupted", e);
        } finally {
            logger.info("App is closing");
        }

    }

    public class ConsumerThread implements Runnable {
        private final CountDownLatch latch;
        private final KafkaConsumer<String, String> consumer;
        public ConsumerThread(CountDownLatch latch, String topic) {
            this.latch = latch;
            Properties properties = new Properties();
            String group = "my-sixth-app";
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, group);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            this.consumer = new KafkaConsumer<>(properties);
            consumer.subscribe(Collections.singletonList(topic));
        }

        @Override
        public void run() {
            //poll for new data
            try{
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> record : records) {
                        logger.info("Key: " + record.key() + ", Value: " + record.value());
                        logger.info("Partition: " + record.partition() + ", Offset: " + record.offset());
                    }
                }
            } catch (WakeupException e) {
                logger.info("Received shutdown signal!");
            } finally {
                consumer.close();
                latch.countDown();
            }

        }

        public void shutdown(){
            consumer.wakeup();
        }
    }

    public static void main(String[] args) {
        new ConsumerDemoWithThread().run();
    }
}
