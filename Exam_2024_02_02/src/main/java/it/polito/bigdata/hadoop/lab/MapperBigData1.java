package it.polito.bigdata.hadoop.lab;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper first job
 */
class MapperBigData1 extends Mapper<LongWritable, // Input key type
		Text, // Input value type
		Text, // Output key type
		IntWritable> { // Output value type

	protected void map(LongWritable key, // Input key type
			Text value, // Input value type
			Context context) throws IOException, InterruptedException {

				String[] fields = value.toString().split(",");

				if (fields[3].compareTo("Game")==0){
					if (Integer.parseInt(fields[2])==0){ // free app
						context.write(new Text(fields[4]), new IntWritable(1));
					} else { // not free app
						context.write(new Text(fields[4]), new IntWritable(-1));
					}
				}
	}

}
