package flink.jimmy.wordcout;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class WordCountDataStream {

	public static void main(String[] args) throws Exception {
		// 1.创建执行环境
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
//		env.setParallelism(1);
		// 2.读取数据
		DataStream<String> lineDs = env.readTextFile("./input/word.txt");
		System.out.println("------------1----------");
		lineDs.print();
		System.out.println("------------2----------");
		lineDs.filter(new FilterFunction<String>() {
			@Override
			public boolean filter(String s) throws Exception {
				return s.contains("jimmy");
			}
		}).print();
		// 3.切分、转换
		DataStream<Tuple2<String, Integer>> wordAndOne = lineDs
				.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
					@Override
					public void flatMap(String input, Collector<Tuple2<String, Integer>> collector) throws Exception {
						String[] words = input.split(" ");
						for (String word : words) {
							collector.collect(new Tuple2<>(word, 1));
						}
					}
				});

		// 4.分组
		DataStream<Tuple2<String, Integer>> grouped = wordAndOne
				.keyBy(0) // 根据元组的第一个字段（即单词）进行分组
				.sum(1); // 对每个分组的第二个字段（即计数）进行求和
		System.out.println("------------3----------");
		// 5.输出
		grouped.print();

		// 6.执行程序
		env.execute("Word Count Demo");
		System.out.println("------------4----------");
	}
}