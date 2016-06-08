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
	
	private List<WordPair> pairList = new LinkedList<WordPair>();
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.cleanup(context);
	}

	@Override
	protected void reduce(Text key, Iterable<WordPair> values, Context context)
			throws IOException, InterruptedException {
		// System.out.println("----------------------------------REDUCER2----------------------------------------");

		int countB = 0;

		for (WordPair value : values) {
			/*
			 * Scorre la lista di valori del tipo
			 * <B,(A,count(AB)><B,(C,count(CB)>...<B,(*,count(B))> Quando trova
			 * che il primo item della coppia è un '*', ha trovato il valore di
			 * count(B). Quindi salva i rimanenti item (contenenti le coppie
			 * (A,count(AB) in una lista ausiliaria.
			 */
			if (value.getWord().equals(flag)) {
				countB = Integer.parseInt(value.getNeighbor().toString().trim());
			} else {
				pairList.add(new WordPair(new Text(value.getWord()), new Text(value.getNeighbor())));
			}
		}
		for (WordPair item : pairList) {
			/*
			 * Scorre la lista dei valori dove la chiave relativa ad essi è B,
			 * e i valori sono (A,count(AB). Per ogni valore prende l'item
			 * count(AB), e calcola la probabilità condizionata
			 * count(AB)/count(B) per ogni coppia (AB) ed emette in output
			 * (AB), count(AB), Pr(A|B)
			 */
			float countAB = (float) Integer.parseInt(item.getNeighbor().toString().trim());
			context.write(new WordPair(item.getWord(), key), new Text(item.getNeighbor() + " " + countAB / countB));
		}
		pairList.clear();
	}
}