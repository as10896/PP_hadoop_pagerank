package pageRank;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.Text;


public class SortPair implements WritableComparable{
	private Text word;
	private double PR;

	public SortPair() {
		word = new Text();
		PR = 0.0;
	}

	public SortPair(Text word, double PR) {
		//TODO: constructor
		this.word = word;
		this.PR = PR;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		word.write(out);
		out.writeDouble(PR);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		word.readFields(in);
		PR = in.readDouble();
	}

	public Text getWord() {
		return word;
	}

	public double getPR() {
		return PR;
	}

	@Override
	public int compareTo(Object o) {

		double thisPR = this.getPR();
		double thatPR = ((SortPair)o).getPR();

		Text thisWord = this.getWord();
		Text thatWord = ((SortPair)o).getWord();

		// Compare between two objects
		// First order by average, and then sort them lexicographically in ascending order
			
		if(thisPR > thatPR)
			return -1;
		else if(thisPR < thatPR)
			return 1;
		else{
			if(str_compare(thisWord.toString(), thatWord.toString()) < 0)
				return -1;
			else
				return 1;
		}		
	
	}

	private int str_compare(String s1, String s2){
		return s1.compareTo(s2);
	}

} 
