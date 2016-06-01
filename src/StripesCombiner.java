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
    private IntWritable totalCount = new IntWritable();
    private Text flag = new Text("*");

    @Override
    protected void reduce(Text word, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
        System.out.println("----------------------------------COMBINER---------------------------------------");
        for (MapWritable value : values)
        {
            for(Writable asterix : value.keySet())
            {
                if (asterix.equals(flag))
                {
                    System.out.println("Word: "+word.toString()+" * "+value.get(asterix).toString());
                }
            }
            context.write(word,value);
        }
    }

}
