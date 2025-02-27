package it.polito.bigdata.hadoop.lab;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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
		Text> { // Output value type

	private List<String> cat = new ArrayList<>();
	private List<Integer> counts = new ArrayList<>();

	protected void map(LongWritable key, // Input key type
			Text value, // Input value type
			Context context) throws IOException, InterruptedException {

				String[] fields = value.toString().split(",");
				
				String catString = fields[2];
				int index = cat.indexOf(catString);
				if (index==-1){
					cat.add(index, catString);
					counts.add(index, 1);
				} else {
					counts.set(index, counts.get(index));
				}
	}

	
	protected void cleanup(Context context){
		for (int i=0; i<cat.size(); i++){
			if (counts.get(i)!=0){
				context.write(new Text(cat.get(i)), new IntWritable(counts.get(i)));
			}
		}
	}
}
