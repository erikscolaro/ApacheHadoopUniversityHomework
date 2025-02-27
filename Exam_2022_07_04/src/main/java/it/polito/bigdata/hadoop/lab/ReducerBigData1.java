package it.polito.bigdata.hadoop.lab;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reducer first job
 */
class ReducerBigData1 extends Reducer<Text, // Input key type
		IntWritable, // Input value type
		Text, // Output key type
		IntWritable> { // Output value type

			Text bestOs;
			IntWritable patchCount;
	protected void setup(Context context){
	
	}

	@Override
	protected void reduce(Text key, // Input key type
			Iterable<IntWritable> values, // Input value type
			Context context) throws IOException, InterruptedException {
				
				int sum =0;
				
				for (IntWritable v: values){
					sum ++;
				}

				if (sum>patchCount.get()){
					bestOs=key;
					patchCount=new IntWritable(sum);
				}


	}

	protected void cleanup(Context context) throws IOException, InterruptedException{
		context.write(bestOs, patchCount);
	}

}
