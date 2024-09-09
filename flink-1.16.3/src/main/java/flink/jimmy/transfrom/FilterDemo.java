package flink.jimmy.transfrom;

import flink.jimmy.bean.WaterSensor;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FilterDemo {

	public static void main(String[] args) {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);

		DataStreamSource<WaterSensor> sensorDs = env.fromElements(
				new WaterSensor("sensor_1", 1546300800000L, 15),
				new WaterSensor("sensor_2", 1546300860000L, 35),
				new WaterSensor("sensor_3", 1546300920000L, 45)
		);
		SingleOutputStreamOperator<WaterSensor> filter = sensorDs.filter(new FilterFunction<WaterSensor>() {
			@Override
			public boolean filter(WaterSensor waterSensor) throws Exception {
				return waterSensor.getId().equals("sensor_1") ? true : false;
			}
		});

		filter.print();

		try {
			env.execute();
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}


	}
}
