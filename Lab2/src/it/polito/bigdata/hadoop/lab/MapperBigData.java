package it.polito.bigdata.hadoop.lab;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper
 */
class MapperBigData extends Mapper<Text, // Input key type
		Text, // Input value type
		Text, // Output key type
		Text> {// Output value type

	protected void map(Text key, // Input key type
			Text value, // Input value type
			Context context) throws IOException, InterruptedException {
		
                int occ = Integer.parseInt(value.toString());
                Text group;
                if (occ>=500){
                    group = new Text("Group5");
                } else if (occ >=400){
                    group = new Text("Group4");
                } else if (occ >=300){
                    group = new Text("Group3");
                } else if (occ >=200){
                    group = new Text("Group2");
                } else if (occ >=100){
                    group = new Text("Group1");
                } else {
                    group = new Text("Group0");
                }

                context.write(group, value);
	}

}
