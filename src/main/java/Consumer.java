import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RebalanceInProgressException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static java.time.Duration.ofMillis;
import static java.util.Collections.singleton;

public class Consumer implements ConsumerRebalanceListener, OffsetCommitCallback {
    // Constants for configuration
    private static final String BOOTSTRAP_SERVERS = "127.0.0.1:9092";
    private static final String GROUP_ID = "my-group";
    private static final long POLL_TIMEOUT_MS = 1_000L;
    private static final String TOPIC_NAME = "random-quotes";
    private static final long NUM_MESSAGES = 1000;
    private static final long PROCESSING_DELAY_MS = 1_000L;

    private KafkaConsumer<Long, String> kafkaConsumer;
    protected AtomicLong messageCount = new AtomicLong(0);
    private Map<TopicPartition, OffsetAndMetadata> pendingOffsets = new HashMap<>();

    public static void main(String[] args) {
        new Consumer().run();
    }

    public void run() {
        System.out.println("Running consumer");

        // Create a Kafka consumer instance
        // This consumer receives messages from the Kafka topic asynchronously
        try (var consumer = createKafkaConsumer()) {
            kafkaConsumer = consumer;
            consumer.subscribe(singleton(TOPIC_NAME), this);
            System.out.printf("Subscribed to %s%n", TOPIC_NAME);
            while (messageCount.get() < NUM_MESSAGES) {
                try {
                    // Poll for new records from Kafka
                    ConsumerRecords<Long, String> records = consumer.poll(ofMillis(POLL_TIMEOUT_MS));
                    if (!records.isEmpty()) {
                        for (ConsumerRecord<Long, String> record : records) {
                            sleep(PROCESSING_DELAY_MS);

//                            System.out.printf("Record fetched from %s-%d with offset %d%n",
//                            record.topic(), record.partition(), record.offset());

                            System.out.printf("Record new record: \n" +
                                    "Key: " + record.key() + ", " +
                                    "Value: " + record.value() + ", " +
                                    "Topic: " + record.topic() + ", " +
                                    "Partition: " + record.partition() + ", " +
                                    "Offst: " + record.offset() + "\n");

                            // Track pending offsets for commit
                            pendingOffsets.put(new TopicPartition(record.topic(), record.partition()),
                                    new OffsetAndMetadata(record.offset() + 1, null));
                            if (messageCount.incrementAndGet() == NUM_MESSAGES) {
                                break;
                            }
                        }
                        // Commit pending offsets asynchronously
                        consumer.commitAsync(pendingOffsets, this);
                        pendingOffsets.clear();
                    }
                } catch (OffsetOutOfRangeException | NoOffsetForPartitionException e) {
                    // Handle invalid offset or no offset found errors when auto.reset.policy is not set
                    System.out.println("Invalid or no offset found, and auto.reset.policy unset, using latest");
                    consumer.seekToEnd(e.partitions());
                    consumer.commitSync();
                } catch (Exception e) {
                    // Handle other exceptions, including retriable ones
                    System.err.println(e.getMessage());
                    if (!retriable(e)) {
                        e.printStackTrace();
                        System.exit(1);
                    }
                }
            }
        }
    }

    private KafkaConsumer<Long, String> createKafkaConsumer() {
        // Create properties for the Kafka consumer
        Properties props = new Properties();

        // Configure the connection to Kafka brokers
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);

        // Set a unique client ID for tracking
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "client-" + UUID.randomUUID());

        // Set a consumer group ID for the consumer
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);

        // Configure deserializers for keys and values
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        // Disable automatic offset committing
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        // Set the offset reset behavior to start consuming from the earliest available offset
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return new KafkaConsumer<>(props);
    }

    private void sleep(long ms) {
        try {
            TimeUnit.MILLISECONDS.sleep(ms);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private boolean retriable(Exception e) {
        if (e == null) {
            return false;
        } else if (e instanceof IllegalArgumentException
                || e instanceof UnsupportedOperationException
                || !(e instanceof RebalanceInProgressException)
                || !(e instanceof RetriableException)) {
            return false;
        } else {
            return true;
        }
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        System.out.printf("Assigned partitions: %s%n", partitions);
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        System.out.printf("Revoked partitions: %s%n", partitions);
        kafkaConsumer.commitSync(pendingOffsets);
        pendingOffsets.clear();
    }

    @Override
    public void onPartitionsLost(Collection<TopicPartition> partitions) {
        System.out.printf("Lost partitions: {}", partitions);
    }

    @Override
    public void onComplete(Map<TopicPartition, OffsetAndMetadata> map, Exception e) {
        if (e != null) {
            System.err.println("Failed to commit offsets");
            if (!retriable(e)) {
                e.printStackTrace();
                System.exit(1);
            }
        }
    }
}