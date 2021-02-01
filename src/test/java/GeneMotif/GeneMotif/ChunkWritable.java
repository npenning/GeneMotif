package GeneMotif.GeneMotif;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class ChunkWritable implements Writable{

	private Text match;
	private IntWritable seqID;
	private IntWritable distance;
	private IntWritable index;
	private IntWritable totalDistance;
	
	//Constructor accepts non-writable data types, for ease of use
	public ChunkWritable(String match, int seqID, int distance, int index){
		this.match = new Text(match);
		this.seqID = new IntWritable(seqID);
		this.distance = new IntWritable(distance);
		this.index = new IntWritable(index);
		//totalDistance is always initialized to 0 and set later
		totalDistance = new IntWritable(0);
	}
	
	public ChunkWritable(){
		match = new Text("");
		seqID = new IntWritable(0);
		distance = new IntWritable(0);
		index = new IntWritable(0);
		totalDistance = new IntWritable(0);
	}
	
	public ChunkWritable clone(){
		return new ChunkWritable(match.toString(), seqID.get(), distance.get(), index.get());
	}
	
	//Writable related methods for compatibility
	public void readFields(DataInput in) throws IOException {
		match.readFields(in);
		seqID.readFields(in);
		distance.readFields(in);
		index.readFields(in);
		totalDistance.readFields(in);
	}

	public void write(DataOutput out) throws IOException {
		match.write(out);
		seqID.write(out);
		distance.write(out);
		index.write(out);
		totalDistance.write(out);
	}
	
	//custom .toString() for final output
	@Override
	public String toString(){
		return match.toString() + "\t" + seqID.toString() + "\t\t" + distance.toString() + "\t\t" + index.toString() + "\t\t" + totalDistance.toString();
	}

	public Text getMatch() {
		return match;
	}

	public void setMatch(Text match) {
		this.match = match;
	}

	public IntWritable getSeqID() {
		return seqID;
	}

	public void setSeqID(IntWritable seqID) {
		this.seqID = seqID;
	}

	public IntWritable getDistance() {
		return distance;
	}

	public void setDistance(IntWritable distance) {
		this.distance = distance;
	}

	public IntWritable getIndex() {
		return index;
	}

	public void setIndex(IntWritable index) {
		this.index = index;
	}
	
	public IntWritable getTotalDistance() {
		return index;
	}

	public void setTotalDistance(IntWritable totalDistance) {
		this.totalDistance = totalDistance;
	}
	

}
