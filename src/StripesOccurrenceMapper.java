/**
 * Created by MichaelOertel on 01/06/16.
 */
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class StripesOccurrenceMapper extends Mapper<LongWritable,Text,Text,MapWritable> {
    private MapWritable occurrenceMap = new MapWritable();
    private Text word = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split("\\s+");
        if (tokens.length > 1) {
            for (int i = 0; i < tokens.length; i++) {
                word.set(tokens[i]);
                occurrenceMap.clear();

                for (int j = 0; j <= tokens.length - 1; j++) {
                if (j == i) continue;
                    Text neighbor = new Text(tokens[j]);
                    if(occurrenceMap.containsKey(neighbor)){
                        IntWritable count = (IntWritable)occurrenceMap.get(neighbor);
                        count.set(count.get()+1);
                    }else{
                        occurrenceMap.put(neighbor,new IntWritable(1));
                    }
                }
                context.write(word,occurrenceMap);
            }
        }
    }
}
