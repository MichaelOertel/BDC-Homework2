import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by Michael Oertel and Aldo D'Eramo on 03/06/16.
 */

public class Mapper_2_A2 extends Mapper<LongWritable, Text, WordPair, WordPair>{

    private final static String flag = new String("*");

    @Override
    protected void cleanup(Context context) throws IOException,
            InterruptedException {
        // TODO Auto-generated method stub
        super.cleanup(context);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        //System.out.println("----------------------------------MAPPER2----------------------------------------");

        String[] tokens = value.toString().split("\\s+");

        /* input da intermediate_output del primo algo. Contiene (B C),count(BC) */
        if (tokens.length == 3) {
            /* Saltiamo se incontriamo un '*', non serve nel conteggio di count(BC) */
            if(!tokens[0].equals(flag)) {
                context.write(new WordPair(tokens[0], tokens[1]), new WordPair("$", tokens[2]));
            }
        } /*input da intermediate_output2. Contiene (A B C),count(ABC) */
        else if (tokens.length == 4) {
            context.write(new WordPair(tokens[1], tokens[2]), new WordPair(tokens[0], tokens[3]));
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
