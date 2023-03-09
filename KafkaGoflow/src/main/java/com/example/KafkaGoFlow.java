import java.time.Instant;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

public class KafkaGoFlow {

    public static void main(String[] args) {
        // Set up Kafka Streams properties
        Properties config = new Properties();
        config.put("bootstrap.servers", System.getenv("KAFKA_BOOTSTRAP_SERVERS"));
        config.put("application.id", "kafkagoflow");

        // Create a StreamsBuilder instance
        StreamsBuilder builder = new StreamsBuilder();

        // Create a KStream instance by subscribing to the input topic
        KStream<String, String> inputStream = builder.stream(System.getenv("KAFKA_INPUT_TOPIC"), Consumed.with(Serdes.String(), Serdes.String()));

        // Modify the values of the input stream and publish to the output topic
        KStream<String, String> outputStream = inputStream
                .mapValues(value -> {
                    String[] parts = value.split(",");
                    String outputValue = String.format("sensor_id=%s,temperature=%s,humidity=%s,timestamp=%s", parts[0], parts[1], parts[2], Instant.now().toEpochMilli());
                    return outputValue;
                });
        
        // Publish the output stream to the output topic
        outputStream.to(System.getenv("KAFKA_OUTPUT_TOPIC"), Produced.with(Serdes.String(), Serdes.String()));

        // Create a KafkaStreams instance and start it
        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        streams.start();
    }
}

