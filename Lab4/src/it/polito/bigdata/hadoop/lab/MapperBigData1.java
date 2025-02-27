package it.polito.bigdata.hadoop.lab;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
                    Text> {// Output value type
    
                        @Override
                        protected void map(
                                LongWritable key,   // Input key type
                                Text value,         // Input value type
                                Context context) throws IOException, InterruptedException {

                                    List<String> recensione = Stream.of(value.toString().split(",")).collect(Collectors.toList());
                                    String Id = recensione.get(0).trim();
                                    String ProductId = recensione.get(1).trim();
                                    String UserId = recensione.get(2).trim();
                                    String Score = recensione.get(6).trim();

                                    if (!Id.equals("Id") && 
                                        !ProductId.isEmpty() && !Score.isEmpty() && !UserId.isEmpty() &&
                                        !Score.equals("Score")) {
                                        
                                        String outputKey = UserId;
                                        String outputValue = ProductId + "," + Score;
                                        context.write(new Text(outputKey), new Text(outputValue));
                                    }
    }
}
