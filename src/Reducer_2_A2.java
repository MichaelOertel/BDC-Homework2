import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by Michael Oertel and Aldo D'Eramo on 03/06/16.
 */

public class Reducer_2_A2 extends Reducer<WordPair, WordPair, WordTriple, Text> {

    private final static Text flag = new Text("$");

    @Override
    protected void cleanup(Context context) throws IOException,
            InterruptedException {
        // TODO Auto-generated method stub
        super.cleanup(context);
    }

    @Override
    protected void reduce(WordPair key, Iterable<WordPair> values, Context context)
            throws IOException, InterruptedException {
        //System.out.println("----------------------------------REDUCER2----------------------------------------");

        int countBC = 0;

        List<WordPair> pairList = new LinkedList<WordPair>();

        //System.out.print("key: "+key+" - ");

        for (WordPair value : values) {
            //System.out.print("("+value+") ");

            pairList.add(new WordPair(new Text(value.getWord()), new Text(value.getNeighbor())));

            if (value.getWord().equals(flag)) {
                countBC = Integer.parseInt(value.getNeighbor().toString().trim());
            }

        }

        //System.out.println("count: "+countB);

        //System.out.println(pairList.toString());

        for (WordPair item : pairList) {
            //System.out.println("item: "+item.toString());

            if (!item.getWord().equals(flag)) {

                float countABC = (float) Integer.parseInt(item.getNeighbor().toString().trim());

                context.write(new WordTriple(item.getWord(), key), new Text(item.getNeighbor() + " " + countABC / countBC));
                //System.out.println("("+item.getWord().toString()+" "+key.toString()+") "+new Text(item.getNeighbor()+" "+countABC/countBC).toString());
            }
        }
    }
}