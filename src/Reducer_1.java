import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Set;

/**
 * Created by Michael Oertel and Aldo D'Eramo on 03/06/16.
 */

public class Reducer_1 extends Reducer<Text,MapWritable,WordPair,IntWritable> {
    private Text flag = new Text("*");
    private MapWritable incrementingAsterixMap = new MapWritable();

    @Override
    protected void reduce(Text word, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
        //System.out.println("----------------------------------REDUCER1---------------------------------------");

        incrementingAsterixMap.clear();

        for (MapWritable value : values)
            addAsterix(value);
        for(Writable key: incrementingAsterixMap.keySet())
        {

            WordPair p = new WordPair(new Text(key.toString()),word);
            context.write(p,(IntWritable)incrementingAsterixMap.get(key));
        }
    }

    private void addAsterix(MapWritable mapWritable) {
        Set<Writable> keys = mapWritable.keySet();
        for (Writable key : keys) {
            IntWritable fromCount = (IntWritable) mapWritable.get(key);
            if (incrementingAsterixMap.containsKey(key))
            {
                IntWritable count = (IntWritable) incrementingAsterixMap.get(key);
                count.set(count.get() + fromCount.get());
                incrementingAsterixMap.replace(key,count);
            }
            else
            {
                incrementingAsterixMap.put(key, fromCount);
            }
        }
    }
}

