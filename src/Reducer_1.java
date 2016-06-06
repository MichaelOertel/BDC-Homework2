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

public class Reducer_1 extends Reducer<Text,MapWritable,WordPair,IntWritable/*Text,MapWritable*/> {
    private Text flag = new Text("*");
    private MapWritable incrementingAsterixMap = new MapWritable();

    @Override
    protected void reduce(Text word, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
        //System.out.println("----------------------------------REDUCER1---------------------------------------");

        incrementingAsterixMap.clear();

        for (MapWritable value : values)
            addAsterix(/*word,*/value);
        for(Writable key: incrementingAsterixMap.keySet())
        {

            WordPair p = new WordPair(new Text(key.toString()),word);
            //System.out.println("<("+p.toString()+"),"+ (IntWritable)incrementingAsterixMap.get(key)+">;");
            context.write(/*new WordPair(word,new Text(key.toString()))*/p,(IntWritable)incrementingAsterixMap.get(key));
        }
        /*for (Writable x : incrementingAsterixMap.keySet())
        {
            System.out.println("<("+word+","+x.toString()+"),"+ incrementingAsterixMap.get(x).toString()+">;");
        }
        context.write(word, incrementingAsterixMap);*/
    }

    private void addAsterix(/*Text word,*/MapWritable mapWritable) {
        Set<Writable> keys = mapWritable.keySet();
        //System.out.println("Word: "+ word+" All Keys of Map"+keys.toString());
        for (Writable key : keys) {
            IntWritable fromCount = (IntWritable) mapWritable.get(key);
            if (incrementingAsterixMap.containsKey(key))
            {
                IntWritable count = (IntWritable) incrementingAsterixMap.get(key);
                count.set(count.get() + fromCount.get());
                //System.out.println("Key: "+key.toString()+" Count value: "+count.toString());
                ((IntWritable) incrementingAsterixMap.get(key)).set(count.get());
            }
            else
            {
                //System.out.println("Word: "+word+" Key: "+key.toString()+" Count value: "+fromCount.toString());
                incrementingAsterixMap.put(key, fromCount);
            }
        }
    }
}

