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

public class Reducer_1 extends Reducer<Text, MapWritable, WordPair, IntWritable> {

	private Text flag = new Text("*");

	private MapWritable incrementingAsterixMap = new MapWritable();

	private WordPair pair = new WordPair();

	@Override
	protected void reduce(Text word, Iterable<MapWritable> values, Context context)
			throws IOException, InterruptedException {
		// System.out.println("----------------------------------REDUCER1---------------------------------------");

		incrementingAsterixMap.clear();
		/*
		 * Crea la stripe finale, unendo tutte quelle ricevute per una
		 * determinata chiave
		 */
		for (MapWritable value : values) {
			addAsterix(value);
		}

		/*
		 * Scorre la stripe finale contenente
		 * <A;(B,count(AB),(C,count(AC),...,(*,count(A))> ed emette coppie
		 * <(B,A),count(BA)>...<(*,A),count(A)>
		 */
		for (Writable key : incrementingAsterixMap.keySet()) {
			pair.setWord(key.toString());
			pair.setNeighbor(word);
			context.write(pair, (IntWritable) incrementingAsterixMap.get(key));
		}
	}

	private void addAsterix(MapWritable mapWritable) {
		Set<Writable> keys = mapWritable.keySet();
		for (Writable key : keys) {
			IntWritable fromCount = (IntWritable) mapWritable.get(key);
			if (incrementingAsterixMap.containsKey(key)) {
				IntWritable count = (IntWritable) incrementingAsterixMap.get(key);
				count.set(count.get() + fromCount.get());
				incrementingAsterixMap.replace(key, count);
			} else {
				incrementingAsterixMap.put(key, fromCount);
			}
		}
	}
}
