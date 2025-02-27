package it.polito.bigdata.hadoop.lab;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Lab - Reducer
 */

/* Set the proper data types for the (key,value) pairs */
class ReducerBigData1 extends Reducer<
                Text,           // Input key type
                Text,    // Input value type
                Text,           // Output key type
                DoubleWritable> {  // Output value type
    
    @Override
    protected void reduce(
        Text key, // Input key type
        Iterable<Text> values, // Input value type
        Context context) throws IOException, InterruptedException {

            Map<String, Double> recensioniMap = new HashMap<>();
            Double sum = 0.0;
            Integer num = 0;

            for (Text val: values){
                String productId = val.toString().split(",")[0];
                Double score = Double.parseDouble(val.toString().split(",")[1]);
                sum += score;
                num ++;
                recensioniMap.put(productId, score);
            }

            Double average = sum/num;
            for (Map.Entry<String, Double> entry: recensioniMap.entrySet()){
                context.write(new Text(entry.getKey()), new DoubleWritable(entry.getValue()-average));
            }    	
    }
}
