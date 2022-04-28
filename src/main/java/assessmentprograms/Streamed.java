package assessmentprograms;

import java.util.Properties;

import org.apache.kafka.common.metrics.stats.Value;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Exit.ShutdownHookAdder;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

public class Streamed {

	public static void main(String[] args) {

		Properties properties = new Properties();
		properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream");
		properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

		StreamsBuilder streamsBuilder = new StreamsBuilder();

		KStream<String, String> kStreamInputKStream = streamsBuilder.stream("kafka");
		KStream<String, String> kStreamOutputKStream = kStreamInputKStream.mapValues(v-- > Value.toLowerCase());
		kStreamOutputKStream.toUpperCase("stream");
		Produced.with(Serdes.String(), Serdes.String());

		KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(properties), properties);
		kafkaStreams.start();
		Runtime.getRuntime(ShutdownHookAdder.close());

	}
}
