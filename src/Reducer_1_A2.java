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

public class Reducer_1_A2 extends Reducer<Text, MapWritable, WordTriple, IntWritable> {

    /* Array associativo finale */
    private MapWritable incrementingAsterixMap = new MapWritable();

    @Override
    protected void cleanup(Context context) throws IOException,
            InterruptedException {
        // TODO Auto-generated method stub
        super.cleanup(context);
    }

    @Override
    protected void reduce(Text word, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
        //System.out.println("----------------------------------REDUCER1---------------------------------------");

        incrementingAsterixMap.clear();

        /* Crea l'array associativo facendo il merge tra tutti quelli presi in input,
        * aggiungendo o aggiornando all'occorrenza il valore di un item letto */
        for (MapWritable value : values) {
            //System.out.println("key: "+word);
            addAsterix(value);
        }

        /* Scorre l'array associativo appena aggiornato ed emette in output la coppia (key,value) seguita da value */
        for (Writable key : incrementingAsterixMap.keySet()) {

            //System.out.println("key: "+key.toString());
            WordTriple t = new WordTriple(word, (WordPair)key);
            context.write(t, (IntWritable) incrementingAsterixMap.get(key));
            //System.out.println(word+" "+((WordPair) key).getWord()+" "+((WordPair) key).getNeighbor()+" "+(IntWritable) incrementingAsterixMap.get(key));
        }
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

    private void addAsterix(MapWritable mapWritable) {

        Set<Writable> keys = mapWritable.keySet();

        for (Writable key : keys) {

            IntWritable fromCount = (IntWritable) mapWritable.get(key);

            if (incrementingAsterixMap.containsKey(key)) {

                IntWritable count = (IntWritable) incrementingAsterixMap.get(key);
                count.set(count.get() + fromCount.get());
                //System.out.println("Key present: "+key.toString()+" Count value: "+count.toString());
                ((IntWritable) incrementingAsterixMap.get(key)).set(count.get());

            } else {
                //System.out.println("Key not present: "+key.toString()+" Count value: "+fromCount.toString());
                incrementingAsterixMap.put(key, fromCount);
            }
        }
    }
}
