package flink.jimmy.split;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SplitFilterDemo {
	public static void main(String[] args) {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(2);


	}
}
