package flink.jimmy.transfrom;

import flink.jimmy.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MapDemo {

	public static void main(String[] args) {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);

		DataStreamSource<WaterSensor> sensorDs = env.fromElements(
				new WaterSensor("sensor_1", 1546300800000L, 15),
				new WaterSensor("sensor_2", 1546300860000L, 35),
				new WaterSensor("sensor_3", 1546300920000L, 45)
		);

		//方式一：匿名内部类
		SingleOutputStreamOperator<String> map = sensorDs.map(new MapFunction<WaterSensor, String>() {
			@Override
			public String map(WaterSensor waterSensor) throws Exception {
				return waterSensor.getId();
			}
		});

		//方式二：lambda表达式
		SingleOutputStreamOperator<Integer> map1 = sensorDs.map(
				sensorDs1 -> sensorDs1.getVc()
		);

		//方式三：继承MapFunction接口
		SingleOutputStreamOperator<Long> map2 = sensorDs.map(new MyMapFunction());

		map.print();
		map1.print();
		map2.print();

		try {
			env.execute();
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}


	}
}
