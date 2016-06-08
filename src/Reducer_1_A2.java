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

public class Reducer_1_A2 extends Reducer<Text, IntWritable, Text, IntWritable> {

	private IntWritable count = new IntWritable(0);
    
    @Override
    protected void cleanup(Context context) throws IOException,
            InterruptedException {
        // TODO Auto-generated method stub
        super.cleanup(context);
    }

    @Override
    protected void reduce(Text word, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        //System.out.println("----------------------------------REDUCER1---------------------------------------");

    	/* Scorre i valori ed aggiorna la somma dei valori relativi alla chiave k */
        for (IntWritable value: values) {
            count.set(count.get() + value.get());
        }

        context.write(word, count);
        count.set(0);
    }

    @Override
    public void run(Context arg0) throws IOException, InterruptedException {
        // TODO Auto-generated method stub
        super.run(arg0);
    }

    @Override
    protected void setup(Context context) throws IOException,
            InterruptedException {
        // TODO Auto-generated method stub
        super.setup(context);
    }
}
