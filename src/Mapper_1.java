import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by Michael Oertel and Aldo D'Eramo on 03/06/16.
 */

public class Mapper_1 extends Mapper<LongWritable,Text,Text,MapWritable> {
    private MapWritable occurrenceMap = new MapWritable();
    private MapWritable count_B = new MapWritable();
    private Text word = new Text();
    private IntWritable ONE = new IntWritable(1);
    private ArrayList<String> sameValues = new ArrayList<String>();


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //System.out.println("----------------------------------MAPPER1---------------------------------------");

        String[] tokens = value.toString().split("\\s+");
        if (tokens.length > 0) {
            for (int i = 0; i < tokens.length; i++) {
                word.set(tokens[i]);
                occurrenceMap.clear();
                count_B.clear();
                if (sameValues.contains(tokens[i]))
                {
                    continue;
                }
                else
                {
                    sameValues.add(tokens[i]);
                }
                for (int j = 0; j <= tokens.length - 1; j++) {
                    if (tokens[i].toString().compareTo(tokens[j].toString()) == 0)
                    {
                        continue;
                    }
                    Text neighbor = new Text(tokens[j]);
                    
                    if(!occurrenceMap.containsKey(neighbor))
                    {
                        occurrenceMap.put(neighbor,new IntWritable(1));
                    }
                }

                count_B.put(new Text("*"),new IntWritable(1));

                context.write(word,count_B);
                context.write(word,occurrenceMap);
            }
            sameValues.clear();
        }
    }
}
