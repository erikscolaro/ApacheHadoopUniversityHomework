package it.polito.bigdata.hadoop.lab;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper first job
 */
class MapperBigData1 extends Mapper<LongWritable, // Input key type
		Text, // Input value type
		Text, // Output key type
		IntWritable> { // Output value type


	protected void map(LongWritable key, // Input key type
			org.w3c.dom.Text value, // Input value type
			Context context) throws IOException, InterruptedException {
				String[] fields = value.toString().split(",");
				String date = fields[1];
				if (date.compareTo("2021/07/04")>=0 && date.compareTo("2022/07/03")<=0){
					context.write(new Text(fields[2]), new IntWritable(1));
				} 

	}

	
}
