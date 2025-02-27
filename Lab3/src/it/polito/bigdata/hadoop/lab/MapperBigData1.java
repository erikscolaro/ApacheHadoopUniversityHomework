package it.polito.bigdata.hadoop.lab;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Lab  - Mapper
 */

/* Set the proper data types for the (key,value) pairs */
class MapperBigData1 extends Mapper<
                    LongWritable, // Input key type
                    Text,         // Input value type
                    Text,         // Output key type
                    IntWritable> {// Output value type
    
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {
                
                List<String> prodotti = Stream.of(value.toString().split(",")).filter(x->!x.startsWith("A")).sorted().collect(Collectors.toList());

                if (prodotti.size()<=1) return;
                for (int first=1; first<prodotti.size()-1; first++){
                    for (int second=first+1; second<prodotti.size(); second++){
                        context.write(new Text(prodotti.get(first).concat(",").concat(prodotti.get(second))), new IntWritable(1));
                    }
                }
    }
}
