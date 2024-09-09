package flink.jimmy.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

public class CollectionDemo {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStreamSource<Integer> source = env.fromCollection(Arrays.asList(1, 22, 3));
		env.setParallelism(1);
		source.print();
		env.execute();
	}
}
