import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Set;

/**
 * Created by MichaelOertel on 01/06/16.
 */
public class StripesCombiner extends Reducer<Text,MapWritable,Text,MapWritable> {
    private Text flag = new Text("*");
    private MapWritable incrementingAsterixMap = new MapWritable();

    @Override
    protected void reduce(Text word, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
        System.out.println("----------------------------------COMBINER---------------------------------------");

        incrementingAsterixMap.clear();

        for (MapWritable value : values)
        {
            addAsterix(word,value);
        }
        for (Writable x : incrementingAsterixMap.keySet())
        {
            System.out.println("<("+word+","+x.toString()+"),"+ incrementingAsterixMap.get(x).toString()+">;");
        }
        context.write(word, incrementingAsterixMap);
    }

    private void addAsterix(Text word,MapWritable mapWritable) {
        Set<Writable> keys = mapWritable.keySet();
        //System.out.println("Word: "+ word+" All Keys of Map"+keys.toString());
        for (Writable key : keys) {
            IntWritable fromCount = (IntWritable) mapWritable.get(key);
            if (incrementingAsterixMap.containsKey(key))
            {
                //if(key.toString().compareTo(flag.toString()) == 0)
                //{
                    IntWritable count = (IntWritable) incrementingAsterixMap.get(key);
                    count.set(count.get() + fromCount.get());
                    //System.out.println("Key: "+key.toString()+" Count value: "+count.toString());
                    incrementingAsterixMap.replace(key,count);
                //}
            }
            else
            {
                //System.out.println("Word: "+word+" Key: "+key.toString()+" Count value: "+fromCount.toString());
                incrementingAsterixMap.put(key, fromCount);
            }
        }
    }
}
