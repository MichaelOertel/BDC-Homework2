import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by Michael Oertel and Aldo D'Eramo on 03/06/16.
 */

public class Mapper_1_A2 extends Mapper<LongWritable, Text, Text, MapWritable> {

    /* Associative Array per contenere i valori delle coppie (B,C),count(ABC) */
    private MapWritable occurrenceMap = new MapWritable();

    private Text word = new Text();
    private IntWritable ONE = new IntWritable(1);

    /* Lista contenente i valori visitati, se si incontra un valore visitato precedentemente
    * si salta affinchè il conteggio non venga ripetuto più di una volta */
    private ArrayList<String> sameValues = new ArrayList<String>();

    @Override
    protected void cleanup(Context context) throws IOException,
            InterruptedException {
        // TODO Auto-generated method stub
        super.cleanup(context);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //System.out.println("----------------------------------MAPPER1---------------------------------------");

        String[] tokens = value.toString().split("\\s+");
        if (tokens.length > 2) {
            for (int i = 0; i < tokens.length; i++) {

                word.set(tokens[i]);
                occurrenceMap.clear();

                if (sameValues.contains(tokens[i])) continue;
                else sameValues.add(tokens[i]);

                for (int j = 0; j < tokens.length; j++) {

                    /* Se siamo sullo stesso elemento andiamo avanti... */
                    if (tokens[j].toString().compareTo(tokens[i].toString()) == 0) continue;

                    for (int k = 0; k < tokens.length; k++) {

                        /* Se siamo sullo stesso elemento andiamo avanti... */
                        if (tokens[k].toString().compareTo(tokens[j].toString()) == 0) continue;
                        if (tokens[k].toString().compareTo(tokens[i].toString()) == 0) continue;

                        occurrenceMap.put(new WordPair(new Text(tokens[j]), new Text(tokens[k])), ONE);

                        WordPair pair = new WordPair(new Text(tokens[j]), new Text(tokens[k]));

                        /* Se la stripe contiene già l'elemento pair lo inseriamo dandogli come valore 1 */
                        if (!occurrenceMap.containsKey(pair)) {
                            occurrenceMap.put(pair, new IntWritable(1));
                        }
                    }
                }
                context.write(word, occurrenceMap);
            }
            sameValues.clear();
        }
    }

    @Override
    public void run(Context context) throws IOException,
            InterruptedException {
        // TODO Auto-generated method stub
        super.run(context);
    }

    @Override
    protected void setup(Context context) throws IOException,
            InterruptedException {
        // TODO Auto-generated method stub
        super.setup(context);
    }
}
