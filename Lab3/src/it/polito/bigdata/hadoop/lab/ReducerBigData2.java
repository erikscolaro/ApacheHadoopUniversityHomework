package it.polito.bigdata.hadoop.lab;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Lab - Reducer
 */

/* Set the proper data types for the (key,value) pairs */
class ReducerBigData2 extends Reducer<
                Text,           // Input key type
                IntWritable,    // Input value type
                Text,           // Output key type
                IntWritable> {  // Output value type
    
    TopKVector<WordCountWritable> local;

    @Override
    protected void setup(Context context){
        int k = 100;
        local= new TopKVector<>(k);
    }
    
    protected void reduce(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {
                local.updateWithNewElement(new WordCountWritable(key.toString(), Integer.parseInt(value.toString())));
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException{
        for (WordCountWritable top: local.getLocalTopK()){
            context.write(new Text(top.getWord()), new IntWritable(top.getCount()));
        }
    }
}
