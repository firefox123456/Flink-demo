package flink.jimmy.transfrom;

import flink.jimmy.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;

public class MyMapFunction implements MapFunction<WaterSensor, Long> {
	@Override
	public Long map(WaterSensor waterSensor) throws Exception {
		return waterSensor.getTs();
	}
}
