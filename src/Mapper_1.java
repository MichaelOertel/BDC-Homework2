import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by Michael Oertel and Aldo D'Eramo on 03/06/16.
 */

public class Mapper_1 extends Mapper<LongWritable, Text, Text, MapWritable> {

	/* Associative Array per contenere i valori delle coppie (A,B),count(AB) */
	private MapWritable occurrenceMap = new MapWritable();
	/* Associative Array per contenere le occorrenze dell'item */
	private MapWritable count_B = new MapWritable();

	private IntWritable ONE = new IntWritable(1);
	private Text flag = new Text("*");

	/*
	 * Lista contenente i valori visitati, se si incontra un valore visitato
	 * precedentemente si salta affinchè il conteggio non venga ripetuto più di
	 * una volta
	 */
	private ArrayList<String> sameValues = new ArrayList<String>();

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.cleanup(context);
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// System.out.println("----------------------------------MAPPER1---------------------------------------");

		String[] tokens = value.toString().split("\\s+");
		if (tokens.length > 0) {

			for (int i = 0; i < tokens.length; i++) {

				Text a = new Text(tokens[i]);

				if (sameValues.contains(tokens[i])) continue;
				else sameValues.add(tokens[i]);

				for (int j = 0; j < tokens.length; j++) {

					Text b = new Text(tokens[j]);
					
					/* Se siamo allo stesso elemento andiamo avanti... */
					if (a.equals(b)) continue;

					/*
					 * Se la stripe non contiene l'elemento neighbor lo
					 * inseriamo dandogli come valore 1
					 */
					if (!occurrenceMap.containsKey(b)) { occurrenceMap.put(b, ONE); }
				}
				count_B.put(flag, ONE);

				context.write(a, count_B);
				context.write(a, occurrenceMap);

				occurrenceMap.clear();
				count_B.clear();
			}
			sameValues.clear();
		}
	}

	@Override
	public void run(Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.run(context);
	}

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.setup(context);
	}
}
