package it.polito.bigdata.hadoop.lab;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reducer second job
 */
class ReducerBigData2 extends Reducer<Text, // Input key type
		NullWritable, // Input value type
		Text, // Output key type
		IntWritable> { // Output value type

	protected void setup(Context context){

	}

	@Override
	protected void reduce(Text key, // Input key type
			Iterable<NullWritable> values, // Input value type
			Context context) throws IOException, InterruptedException {

	}

	protected void cleanup(Context context){
		
	}

}
