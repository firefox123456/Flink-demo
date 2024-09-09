package flink.jimmy.aggreagte;

import flink.jimmy.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SimpleAggreagteDemo {

	public static void main(String[] args) {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);

		DataStreamSource<WaterSensor> sensorDs = env.fromElements(
				new WaterSensor("sensor_1", 1546300800000L, 15),
				new WaterSensor("sensor_1", 1546300800001L, 20),
				new WaterSensor("sensor_2", 1546300860000L, 35),
				new WaterSensor("sensor_2", 1546300860001L, 99),
				new WaterSensor("sensor_3", 1546300920000L, 45),
				new WaterSensor("sensor_3", 1546300920001L, 111)
		);

		KeyedStream<WaterSensor, String> keyByStream = sensorDs.keyBy(waterSensor -> waterSensor.getId());

		SingleOutputStreamOperator<WaterSensor> result = keyByStream.maxBy("vc");

		result.print();

		try {
			env.execute();
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}


	}
}
