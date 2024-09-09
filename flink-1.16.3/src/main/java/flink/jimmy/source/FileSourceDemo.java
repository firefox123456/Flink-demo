package flink.jimmy.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FileSourceDemo {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//		从文件读新架构
		FileSource<String> fileSource = FileSource
				.forRecordStreamFormat(new TextLineInputFormat(), new Path("input/word.txt"))
				.build();

		env
				.fromSource(fileSource, WatermarkStrategy.noWatermarks(), "fileSource")
				.print();

		env.execute();

	}
}
