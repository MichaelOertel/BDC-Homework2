/**
 * Created by MichaelOertel on 01/06/16.
 */
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class WordTriple implements Writable,WritableComparable<WordTriple> {

    private Text word;
    private Text neighbor;
    private Text neighbor2;

    public WordTriple(Text word, Text neighbor, Text neighbor2) {
        this.word = word;
        this.neighbor = neighbor;
        this.neighbor2 = neighbor2;
    }

    public WordTriple(Text word, WordPair neighbor) {
        this.word = word;
        this.neighbor = neighbor.getWord();
        this.neighbor2 = neighbor.getNeighbor();
    }

    public WordTriple(String word, String neighbor, String neighbor2) {
        this(new Text(word),new Text(neighbor), new Text(neighbor2));
    }

    public WordTriple() {
        this.word = new Text();
        this.neighbor = new Text();
        this.neighbor2 = new Text();
    }

    @Override
    public int compareTo(WordTriple other) {
        int returnVal = this.word.compareTo(other.getWord());
        if(returnVal != 0){
            return returnVal;
        }
        if(this.neighbor.toString().equals("*")){
            return -1;
        }else if(other.getNeighbor().toString().equals("*")){
            return 1;
        }
        return this.neighbor.compareTo(other.getNeighbor());
    }

    public static WordTriple read(DataInput in) throws IOException {
        WordTriple wordTriple = new WordTriple();
        wordTriple.readFields(in);
        return wordTriple;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        word.write(out);
        neighbor.write(out);
        neighbor2.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        word.readFields(in);
        neighbor.readFields(in);
        neighbor2.readFields(in);
    }

    @Override
    public String toString() {
        return word+" "+neighbor+" "+neighbor2;

    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        WordTriple wordTriple = (WordTriple) o;

        if (neighbor2 != null ? !neighbor2.equals(wordTriple.neighbor2) : wordTriple.neighbor2 != null) return false;
        if (neighbor != null ? !neighbor.equals(wordTriple.neighbor) : wordTriple.neighbor != null) return false;
        if (word != null ? !word.equals(wordTriple.word) : wordTriple.word != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = word != null ? word.hashCode() : 0;
        result = 163 * result + (neighbor != null ? neighbor.hashCode() : 0);
        return result;
    }

    public void setWord(String word){
        this.word.set(word);
    }
    public void setNeighbor(String neighbor){
        this.neighbor.set(neighbor);
    }
    public void setNeighbor2(String neighbor2) { this.neighbor2.set(neighbor2); }

    public Text getWord() {
        return word;
    }

    public Text getNeighbor() {
        return neighbor;
    }

    public Text getNeighbor2() {
        return neighbor2;
    }
}
