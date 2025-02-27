package it.polito.bigdata.hadoop.lab;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper second job
 */
class MapperBigData2 extends Mapper<
                    Text,  // Input key type
                    Text, 		  // Input value type
                    Text,         // Output key type
                    IntWritable> {		  // Output value type
	
	protected void setup(Context conf){

	}

	protected void map(LongWritable key, // Input key type
			Text value, // Input value type
			Context context) throws IOException, InterruptedException {
	}

	
	protected void cleanup(Context conf){
		
	}
}
