package GeneMotif.GeneMotif;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class GeneMotif {
	
	public static class GeneMapper extends Mapper<LongWritable, Text, Text, ChunkWritable>{
		
		private byte[] candidateArr = {0, 0, 0, 0, 0, 0, 0, 0};
		private boolean done = false;
		
		public void map(LongWritable inKey, Text inValue, Context context) throws IOException, InterruptedException {
			String geneString = inValue.toString();
			Text outKey = new Text();
			long lineNum = inKey.get();
			
			String candidate = "gggggggg";
			String geneSegment = null, bestMatch = "";
			int bestScore, score, bestIndex = 0;
			
			//for each candidate
			do{
				//for each 8-mer in line of gene sequence
				bestScore = 9;
				for(int i = 0; i < geneString.length() - 7; i++) {
					score = 0;
					geneSegment = geneString.substring(i, i+8);
					
					//calculate hamming distance
					for(int j = 0; j < candidate.length(); j++) {
						if(candidate.charAt(j) != geneSegment.charAt(j)) {
							score++;
						}
					}
					
					//set winning values if lowest score is current best
					if(score < bestScore) {
						bestScore = score;
						bestMatch = geneSegment;
						bestIndex = i;
					}
					
				}
				
				//export candidate token
				context.write(new Text(candidate), new ChunkWritable(bestMatch, (int)(lineNum/(geneString.length()+2)), bestScore, bestIndex));
				
				//increment candidate
				candidate = incrementGene(candidate);
			}while(!done);
			done = false;
			
		}
		
		//gene candidate incrementer
		private String incrementGene(String s) {
			//if final 8-mer has been reached, set flags to end loop
			if(s.equals("tttttttt")) {
				done = true;
				return "";
			}
			else {
				//increment byte array on mod 4
				StringBuilder lmer = new StringBuilder();
				//increment LSB
				candidateArr[0] = (byte)((candidateArr[0] + 1) % 4);
				for(int i = 1; i < 8; i++) {
					//if previous byte just rolled over, increment next byte
					if(candidateArr[i-1] == 0) {
						candidateArr[i] = (byte)((candidateArr[i] + 1) % 4);
					}
					else
						break;
				}
				
				//build 8-mer string from byte data
				for(int i = 7; i >= 0; i--) {
					switch(candidateArr[i]) {
						case 0: lmer.append("g");
						break;
						case 1: lmer.append("c");
						break;
						case 2: lmer.append("a");
						break;
						case 3: lmer.append("t");
						break;
					}
				}
				
				return lmer.toString();
			}
			
		}
		
	}
	
	public static class ScoreReducer extends Reducer<Text, ChunkWritable, Text, ChunkWritable>{
		
		private String winner = "";
		private ArrayList<ChunkWritable> winData = new ArrayList<ChunkWritable>();
		private int winScore = -1;
		
		public void reduce(Text candidate, Iterable<ChunkWritable> scores, Context context) throws IOException, InterruptedException {
			int finalScore = 0;
			Iterator<ChunkWritable> scoreIterator = scores.iterator();
			ArrayList<ChunkWritable> candidateData = new ArrayList<ChunkWritable>();
			
			//copy candidate data to cache (candidateData) and sum line scores
			while(scoreIterator.hasNext()) {
				ChunkWritable tempChunk = scoreIterator.next().clone();
				candidateData.add(tempChunk);
				finalScore += tempChunk.getDistance().get();
			}
			
			//if summed score is lowest among all scores, set winning values
			if(finalScore < winScore || winScore == -1){
				winScore = finalScore;
				winner = candidate.toString();
				
				winData = candidateData;
				//debug: each time a new winner is found, print score and 8-mer
				//System.out.println("Score Update: " + winScore + "\t" + winner.toString());
			}
		}
		
		public void cleanup(Context context) throws IOException, InterruptedException{
			//write header to final file
			context.write(new Text("Motif\t\tMatchMotif\tSeqID\tDis\t\tIndex\ttotalDis"), null);
			
			//for each data chunk under winning key
			for(ChunkWritable currentChunk : winData) {
				//set winning score
				currentChunk.setTotalDistance(new IntWritable(winScore));
				//write data chunk
				context.write(new Text(winner), currentChunk);
			}
		}
		
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		Configuration conf = new Configuration();
		//setup mapreduce options parser
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		//give error if there are not 2 arguments
		if(otherArgs.length != 2) {
			System.err.println("Usage: input_file output_path");
			System.exit(2);
		}
		
		//create mapreduce job
		Job job = Job.getInstance(conf, "Job 1");
		
		//configure mapreduce job
		
		//set main class, mapper class, reducer class
		job.setJarByClass(GeneMotif.class);
		job.setMapperClass(GeneMapper.class);
		job.setReducerClass(ScoreReducer.class);
		//set number of reduce processes. Breaks if not '1' (see write-up)
		job.setNumReduceTasks(1);
		//set mapper/reducer output key & value data types
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(ChunkWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(ChunkWritable.class);
		
		//create subdirectory which does not already exist under given output directory following the format 'GeneMotif_Output_X' 
		String outputPath;
		for(int i = 0; true; i++){
			outputPath = otherArgs[1] + File.separator + "GeneMotif_Output_" + i;
			if(!(new File(outputPath).exists())){
				break;
			}
		}
		
		//set final input path and output directory
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		
		//pause java execution until job completes
		boolean status = job.waitForCompletion(true);
		
		//exit using appropriate error code
		if(status) {
			System.exit(0);
		}
		else {
			System.exit(1);
		}
		
	}

}