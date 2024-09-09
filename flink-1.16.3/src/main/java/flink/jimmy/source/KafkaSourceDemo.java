package flink.jimmy.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class KafkaSourceDemo {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);

//		KafkaSource<String> kafkaSource = KafkaSource.builder()
//				.setBootstrapServers("192.168.1.100:9092")
//				.setGroupId("flink-test")
//				.setTopics("test")
//				.setValueOnlyDeserializer(new SimpleStringSchema())
//				.setStartingOffsets()
//				.build();
//		env
//				.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka-source")
//				.print();

        env.execute();
	}
}
