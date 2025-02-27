package it.polito.bigdata.hadoop.lab;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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

	private List<Text> cat = new ArrayList<>();
	private List<IntWritable> counts = new ArrayList<>();
	private int max=0;

	@Override
	protected void reduce(Text key, // Input key type
			Iterable<IntWritable> values, // Input value type
			Context context) throws IOException, InterruptedException {
				int sum = 0;
				for (IntWritable v: values){
					sum+=v.get();
				}
				cat.add(key);
				counts.add(new IntWritable(sum));
				if (sum>max) max=sum;
	}

	protected void cleanup(Context context)throws IOException, InterruptedException {
		for (int i=0; i<cat.size(); i++){
			if (counts.get(i).get()!=0 && counts.get(i).get()==max){
				context.write(cat.get(i), new IntWritable(max));
			}
		}
	}

}
