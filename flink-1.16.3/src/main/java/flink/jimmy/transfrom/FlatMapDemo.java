package flink.jimmy.transfrom;

import flink.jimmy.bean.WaterSensor;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class FlatMapDemo {

	public static void main(String[] args) {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);

		DataStreamSource<WaterSensor> sensorDs = env.fromElements(
				new WaterSensor("sensor_1", 1546300800000L, 15),
				new WaterSensor("sensor_1", 1546300800000L, 20),
				new WaterSensor("sensor_2", 1546300860000L, 35),
				new WaterSensor("sensor_3", 1546300920000L, 45)
		);

		SingleOutputStreamOperator<String> flatMap = sensorDs.flatMap(new FlatMapFunction<WaterSensor, String>() {
			@Override
			public void flatMap(WaterSensor waterSensor, Collector<String> collector) throws Exception {
				if ("sensor_1".equals(waterSensor.getId())) {
					collector.collect(waterSensor.getVc().toString());
				} else if ("sensor_2".equals(waterSensor.getId())) {
					collector.collect(waterSensor.getVc().toString());
					collector.collect(waterSensor.getTs().toString());
				}
			}
		});

		flatMap.print();

		try {
			env.execute();
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}


	}
}
