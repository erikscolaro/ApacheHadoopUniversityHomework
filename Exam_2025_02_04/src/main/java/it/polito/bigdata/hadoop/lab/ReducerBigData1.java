package it.polito.bigdata.hadoop.lab;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reducer first job
 */
class ReducerBigData1 extends Reducer<Text, // Input key type
		Text, // Input value type
		Text, // Output key type
		IntWritable> { // Output value type

	@Override
	protected void reduce(Text key, // Input key type
			Iterable<Text> values, // Input value type
			Context context) throws IOException, InterruptedException {
				int cc = 0;
				for (Text val: values){
					String valString = val.toString();
					if (valString.compareTo("free")==0){
						cc++;
					} else {
						return;
					}
				}
				context.write(key, new IntWritable(cc));
	}

}
