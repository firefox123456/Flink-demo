package flink.jimmy.wordcout;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FilterOperator;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @author jimmy
 * dataSource版本处理
 */
public class WordCountDemo {

	public static void main(String[] args) throws Exception {
		// 1.创建执行环境
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		//2.读取数据
		DataSource<String> lineDs = env.readTextFile("./input/word.txt");
        lineDs.print();
        System.out.println("------------1----------");
		//3.切分、转换
		FilterOperator<String> jimmyData = lineDs.filter(new FilterFunction<String>() {
			@Override
			public boolean filter(String s) throws Exception {
				return s.contains("jimmy");
			}
		});
		jimmyData.print();
		System.out.println("------------2----------");
		FlatMapOperator<String, Tuple2<String, Integer>> wordAndOne = jimmyData.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
			@Override
			public void flatMap(String input, Collector<Tuple2<String, Integer>> collector) throws Exception {
				String[] words = input.split(" ");
				for (String word : words) {
					Tuple2<String, Integer> wordTuple2 = Tuple2.of(word, 1);
					collector.collect(wordTuple2);
				}
			}
		});
		wordAndOne.print();
		System.out.println("------------3----------");
		//4.分组
		UnsortedGrouping<Tuple2<String, Integer>> wordAndGroupByWord = wordAndOne.groupBy(0);
		//5.统计数据
		AggregateOperator<Tuple2<String, Integer>> sum = wordAndGroupByWord.sum(1);
		//6.输出
		sum.print();
	}
}
