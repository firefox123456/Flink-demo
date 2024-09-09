package flink.jimmy.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DataGeneratorDemo {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// 设置并行度
		env.setParallelism(2);

		GeneratorFunction<Long, String> generatorFunction = index -> "Number: " + index;
		long numberOfRecords = 100;

		DataGeneratorSource<String> source =
				new DataGeneratorSource<>(generatorFunction, numberOfRecords, RateLimiterStrategy.perSecond(1), Types.STRING);

		env
				.fromSource(source, WatermarkStrategy.noWatermarks(), "data-generator")
				.print();

		env.execute();

	}
}
