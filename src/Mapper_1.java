import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Michael Oertel and Aldo D'Eramo on 03/06/16.
 */

public class Mapper_1 extends Mapper<LongWritable, Text, Text, MapWritable> {
    private MapWritable count_B = new MapWritable();
    private IntWritable ONE = new IntWritable(1);
    private ArrayList<String> sameValues = new ArrayList<String>();

    private Map<Text, MapWritable> map = new HashMap<Text, MapWritable>();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //System.out.println("----------------------------------MAPPER1---------------------------------------");

        String[] tokens = value.toString().split("\\s+");
        if (tokens.length > 1) {


            for (int i = 0; i < tokens.length; i++) {

                Text word = new Text(tokens[i]);
                count_B.clear();

                if (sameValues.contains(tokens[i])) continue;
                else sameValues.add(tokens[i]);

                for (int j = i + 1; j < tokens.length; j++) {

                    Text b = new Text(tokens[j]);

                    if (tokens[i].toString().compareTo(tokens[j].toString()) == 0) continue;

                    if (map.containsKey(word)) {
                        if (!map.get(word).containsKey(b)) {
                            MapWritable stripe = new MapWritable();
                            map.get(word).put(b, new IntWritable(1));
                        }
                    } else {
                        MapWritable mapi = new MapWritable();
                        mapi.put(b, new IntWritable(1));
                        map.put(word, mapi);
                    }

                    if (map.containsKey(b)) {
                        if (map.get(b).isEmpty() || !map.get(b).containsKey(word)) {
                            map.get(b).put(word, new IntWritable(1));
                        }
                    } else {
                        MapWritable stripe = new MapWritable();
                        stripe.put(word, new IntWritable(1));
                        map.put(b, stripe);
                    }
                }

                count_B.put(new Text("*"), new IntWritable(1));

                context.write(word, count_B);
            }
            sameValues.clear();
        }
        for (Text keymap : map.keySet()) {
            context.write(keymap, map.get(keymap));
        }

        map.clear();
    }
}
