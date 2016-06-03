import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by Michael Oertel and Aldo D'Eramo on 03/06/16.
 */

public class Reducer_2 extends Reducer<Text, WordPair, WordPair, Text> {

    private final static Text flag = new Text("*");

    @Override
    protected void cleanup(Context context) throws IOException,
            InterruptedException {
        // TODO Auto-generated method stub
        super.cleanup(context);
    }

    @Override
    protected void reduce(Text key, Iterable<WordPair> values, Context context)
            throws IOException, InterruptedException {
        //System.out.println("----------------------------------REDUCER2----------------------------------------");

        int countB = 0;

        List<WordPair> pairList = new LinkedList<WordPair>();

        //System.out.print("key: "+key+" - ");

        for (WordPair value : values) {
            //System.out.print("("+value+") ");

            pairList.add(new WordPair(new Text(value.getWord()), new Text(value.getNeighbor())));

            if (value.getWord().equals(flag)) {
                countB = Integer.parseInt(value.getNeighbor().toString().trim());
            }

        }

        //System.out.println("count: "+countB);

        //System.out.println(pairList.toString());

        for (WordPair item : pairList) {
            //System.out.println("item: "+item.toString());

            if (!item.getWord().equals(flag)) {

                float countAB = (float) Integer.parseInt(item.getNeighbor().toString().trim());

                context.write(new WordPair(item.getWord(), key), new Text(item.getNeighbor() + " " + countAB / countB));
                //System.out.println("("+item.getWord().toString()+" "+key.toString()+") "+new Text(item.getNeighbor()+" "+countAB/countB).toString());
            }
        }
    }
}