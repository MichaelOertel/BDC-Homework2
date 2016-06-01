/**
 * Created by MichaelOertel on 01/06/16.
 */
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class PairsReducer extends Reducer<WordPair,IntWritable,WordPair,IntWritable> {
    private IntWritable totalCount = new IntWritable();

    @Override
    protected void reduce(WordPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        System.out.println("----------------------------------COMBINER---------------------------------------");

        int count = 0;
        for (IntWritable value : values) {
            System.out.print(value.toString()+",");
            count += value.get();
        }
        totalCount.set(count);
        System.out.println("Key: "+key.toString()+" Total Count: "+totalCount);
        context.write(key,totalCount);
    }
}
