package it.polito.bigdata.hadoop.lab;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Lab - Reducer
 */

/* Set the proper data types for the (key,value) pairs */
class ReducerBigData2 extends Reducer<
                Text,           // Input key type
                DoubleWritable,    // Input value type
                Text,           // Output key type
                DoubleWritable> {  // Output value type
    
                    @Override
                    protected void reduce(
                        Text key, // Input key type
                        Iterable<DoubleWritable> values, // Input value type
                        Context context) throws IOException, InterruptedException {
                            double sum = 0;
                            int n = 0;
                            for (DoubleWritable val: values){
                                sum += val.get();
                                n++;
                            }

                            context.write(key, new DoubleWritable(sum/n));
                        
                    }
}
