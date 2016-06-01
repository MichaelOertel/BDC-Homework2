/**
 * Created by MichaelOertel on 01/06/16.
 */
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class WordPairPartitioner extends Partitioner<WordPair,IntWritable> {

    @Override
    public int getPartition(WordPair wordPair, IntWritable intWritable, int numPartitions) {
        return wordPair.getWord().hashCode() % numPartitions;
    }
}
