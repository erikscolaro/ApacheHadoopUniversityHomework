package it.polito.bigdata.hadoop.lab;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Lab  - Mapper
 */

/* Set the proper data types for the (key,value) pairs */
class MapperBigData2 extends Mapper<
                    LongWritable, // Input key type
                    Text,         // Input value type
                    Text,         // Output key type
                    IntWritable> {// Output value type

    TopKVector<WordCountWritable> local;

    @Override
    protected void setup(Context context){
        int k = 100;
        local= new TopKVector<>(k);
    }
    
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

                String[] s = value.toString().split("\t");

                local.updateWithNewElement(new WordCountWritable(s[0],Integer.parseInt(s[1])));
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException{
        for (WordCountWritable top: local.getLocalTopK()){
            context.write(new Text(top.getWord()), new IntWritable(top.getCount()));
        }
    }
}
