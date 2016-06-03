/**
 * Created by MichaelOertel on 01/06/16.
 */
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;

public class StripesOccurrenceMapper extends Mapper<LongWritable,Text,Text,MapWritable> {
    private MapWritable occurrenceMap = new MapWritable();
    private MapWritable count_B = new MapWritable();
    private Text word = new Text();
    private IntWritable ONE = new IntWritable(1);
    private ArrayList<String> sameValues = new ArrayList<String>();


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        System.out.println("----------------------------------MAPPER---------------------------------------");

        String[] tokens = value.toString().split("\\s+");
        if (tokens.length > 1) {
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
                if (tokens[j].toString().compareTo(tokens[i].toString()) == 0)
                {
                    continue;
                }
                    Text neighbor = new Text(tokens[j]);
                    if(occurrenceMap.containsKey(neighbor)){
                        IntWritable count = (IntWritable)occurrenceMap.get(neighbor);
                        count.set(count.get()+1);
                    }else{
                        occurrenceMap.put(neighbor,new IntWritable(1));
                    }
                }

                count_B.put(new Text("*"),new IntWritable(1));

                for (Writable x: occurrenceMap.keySet())
                    System.out.println("Word: "+word.toString()+" "+x.toString()+" "+occurrenceMap.get(x).toString());
                for (Writable y: count_B.keySet())
                    System.out.println("Word: "+word.toString()+" "+y.toString()+" "+count_B.get(y).toString());

                context.write(word,count_B);
                context.write(word,occurrenceMap);
            }
            sameValues.clear();
        }
    }
}
