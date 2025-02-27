package it.polito.bigdata.hadoop.lab;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper first job
 */
class MapperBigData1 extends Mapper<LongWritable, // Input key type
		Text, // Input value type
		Text, // Output key type
		Text> { // Output value type

	protected void map(LongWritable key, // Input key type
			Text value, // Input value type
			Context context) throws IOException, InterruptedException {

				String[] valStrings = value.toString().split(",");
				String country = valStrings[3];
				String plan = valStrings[4];
				context.write(new Text(country), new Text(plan));
	}
}
