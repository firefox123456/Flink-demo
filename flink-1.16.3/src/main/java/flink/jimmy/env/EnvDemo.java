package flink.jimmy.env;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class EnvDemo {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set(RestOptions.BIND_PORT, "8083");
		conf.set(RestOptions.BIND_ADDRESS, "127.0.0.1");
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
		env
				.readTextFile("input/word.txt")
				.map(line -> line.toLowerCase())
				.print();
		env.setRuntimeMode(RuntimeExecutionMode.BATCH);
		env.execute();
	}
}
