package project;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Vector;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



/*
  Program to preprocess data,form song pairs, compute similarity of each pair. Output is Similarity list
 
  
 Cosine Similarity: job2.setMapperClass( cosine_map .class);
 job2.setReducerClass( cosine_redice.class);
 
 Pearson Similarity: job2.setMapperClass( pearson_map.class);
 job2.setReducerClass( pearson_reduce.class);
  	
 
 */

public class Similarity extends Configured implements Tool {

	public static void main( String[] args) throws  Exception {

		long start = new Date().getTime();

		int res  = ToolRunner .run( new Similarity(), args);

		long end = new Date().getTime();
		System.out.println("______SIMILARITY RUN time _____	" + (end-start) + "  ms_____");

		System .exit(res);
	}

	public int run( String[] args) throws  Exception {

		Configuration c1 = new Configuration();

		String intermediate = "intermediate";
		Path out_path = new Path(args[1]);
		Path inter_path = new Path(intermediate);
		FileSystem hdfs = FileSystem.get(c1);

		try {
			if(hdfs.exists(out_path)){
				hdfs.delete(out_path, true);
			} 
			if(hdfs.exists(inter_path)){
				hdfs.delete(inter_path, true);
			} 
		} catch (IOException e) {
			e.printStackTrace();
		}

		Job job1  = Job .getInstance(c1, "Preprocess");  //job 1 to preprocess data
		job1.setJarByClass(Similarity.class);          

		Path inter1 = new Path(inter_path, "intermed1");

		FileInputFormat.addInputPaths(job1,  args[0]); //defining i/o paths
		FileOutputFormat.setOutputPath(job1, inter1);

		job1.setMapperClass( preprocess_map .class);  //defining map and reduce classes
		job1.setReducerClass( preprocess_reduce .class);

		job1.setOutputKeyClass( Text .class);
		job1.setOutputValueClass( Text .class);

		int j1 =  job1.waitForCompletion( true)  ? 0 : 1;

		int j2 = 1;
		int j3 = 1;
		if(j1 == 0){

			Configuration c2 = new Configuration();

			Job job2  = Job .getInstance(c2, "CalculateSimilarity"); //job2 to compute similarty
			job2.setJarByClass(Similarity.class);

			Path inter2 = new Path(inter_path, "intermed2");

			FileInputFormat.addInputPath(job2,  inter1);
			FileOutputFormat.setOutputPath(job2, inter2); //defining i/o paths

			job2.setMapperClass( cosine_map .class);
			job2.setReducerClass( cosine_reduce .class);   //defining map and reduce classes

			job2.setOutputKeyClass( Text .class);
			job2.setOutputValueClass( Text .class);

			j2 =  job2.waitForCompletion( true)  ? 0 : 1;


			if(j2 == 0){
				Configuration c3 = new Configuration();

				Job job3  = Job .getInstance(c3, "Similarity_list");
				job3.setJarByClass(Similarity.class);

				FileInputFormat.addInputPath(job3,  inter2);
				FileOutputFormat.setOutputPath(job3, out_path); //defining i/o paths

				job3.setMapperClass( similarity_map.class);
				job3.setReducerClass( similarity_reduce.class);  //defining map and reduce classes

				job3.setOutputKeyClass( Text .class);
				job3.setOutputValueClass( Text .class);

				j3 =  job3.waitForCompletion( true)  ? 0 : 1;
			}
		}      
		return j3;
	}

	/*
	 Mapper 1 : Preprocess
	 input: offset and every line in the input file
	 output:  <userID	songID=rating>
	 
	 */

	public static class preprocess_map extends Mapper<LongWritable ,  Text ,  Text ,  Text > {

		public void map( LongWritable offset,  Text lineText,  Context context)
				throws  IOException,  InterruptedException {

			String line  = lineText.toString();

			String[] segment = StringUtils.split(line); //splitting line
			if(segment.length == 3){
				String userID = segment[0];
				String songID = segment[1];
				String rating = segment[2];

				context.write(new Text(userID), new Text(songID + "=" + rating));
			}
		}
	}

	/*
	 Reducer 1: preprocess
	 input:  <userID	[list of songID=rating]>
	 output:  <userID songID=rating_List>	
	 
	 */

	public static class preprocess_reduce extends Reducer<Text ,  Text ,  Text ,  Text > {
		@Override 
		public void reduce( Text users,  Iterable<Text > song_rating,  Context context)
				throws IOException,  InterruptedException {

			String songID_rating = "";
			for ( Text sr  : song_rating) {
				songID_rating = songID_rating + "," + sr.toString(); //iterate through each song rating pair and separate by ,
			}

			context.write(users,  new Text(songID_rating));
		}
	}
	/*
	  Mapper 2 : Cosine Similarity
	  input: offset & content (lineText) of every line in the input file: <userID songID=rating_List>
	  output: <song1$$song2	rating1@@rating2>
	 
	 */

	public static class cosine_map extends Mapper<LongWritable ,  Text ,  Text ,  Text > {

		public void map( LongWritable offset,  Text lineText,  Context context)
				throws  IOException,  InterruptedException {

			ArrayList<Integer> song = new ArrayList<Integer>();
			ArrayList<Double> rating = new ArrayList<Double>();

			String line  = lineText.toString();

			String full_list = StringUtils.substringAfter(line, "	");   //separate username      
			String[] sr_list = StringUtils.split(full_list, ",");  //split song and rating

			if(sr_list.length >1){
				for(String sr : sr_list){

					String[] parts = StringUtils.split(sr, "=");
					song.add(Integer.parseInt(parts[0]));
					rating.add(Double.parseDouble(parts[1]));//separate song and rating
				}

				int length = song.size();

				for(int i=0; i<length-1; i++){
					for(int j=i+1; j<length; j++){

						String songPair = song.get(i) + "$$" + song.get(j); //save song pair in this format
						String ratingPair = rating.get(i) + "@@" + rating.get(j); //saving rating pair in this form

						context.write(new Text(songPair), new Text(ratingPair));
						System.out.println(songPair + "  >>>  " +  ratingPair);        		 
					}
				}
			}	         	 
		}
	}

	/*
	 Reducer 2: Cosine similarity
	 input:  <song1$$song2	[list rating1@@rating2] >
	 output: <song1$$song2 similarity>
	 
	 */

	public static class cosine_reduce extends Reducer<Text ,  Text ,  Text ,  DoubleWritable > {
		@Override 
		public void reduce( Text songPair,  Iterable<Text > ratingPair_list,  Context context)
				throws IOException,  InterruptedException {

			Vector<Double> rating1 = new Vector<Double>();
			Vector<Double> rating2 = new Vector<Double>();

			double SummationProduct = 0.0;
			double normalize1 = 0.0;
			double normalize2 = 0.0;
			double cos_sim = 0.0;

			for(Text rl : ratingPair_list){

				String ratings = rl.toString();

				
				String[] rating = StringUtils.split(ratings, "@@");

				rating1.addElement(Double.parseDouble(rating[0])); //splitting rating 1 and 2
				rating2.addElement(Double.parseDouble(rating[1]));
			}

		

			if(rating1.size() == rating2.size() && rating1.size() > 1){
				for(int i=0; i<rating1.size(); i++){

					SummationProduct  = SummationProduct + rating1.get(i) * rating2.get(i); //numerator of cosine similarity

					normalize1 = normalize1 + Math.pow(rating1.get(i), 2);
					normalize2 = normalize2 + Math.pow(rating2.get(i), 2); 
				}

				cos_sim = SummationProduct / (Math.sqrt(normalize1) * Math.sqrt(normalize2)); //denominator of cosine similarity

				if(cos_sim > 0.5){
					context.write(songPair, new DoubleWritable(cos_sim)); //output
					System.out.println(songPair + "	>>>	" + cos_sim);
				}
			}
		}
	}

	/*
	 * Mapper function 3:pearson similarity
	  input: offset & content (lineText) of every line in the input file: <userID songID=rating_List>
	  output: <key, value> pairs -> <song1$$song2	rating1@@rating2>
	 
	 */

	public static class pearson_map extends Mapper<LongWritable ,  Text ,  Text ,  Text > {

		public void map( LongWritable offset,  Text lineText,  Context context)
				throws  IOException,  InterruptedException {

			ArrayList<Integer> song = new ArrayList<Integer>();
			ArrayList<Double> rating = new ArrayList<Double>();

			String line  = lineText.toString();

			String full_list = StringUtils.substringAfter(line, "	");         
			String[] sr_list = StringUtils.split(full_list, ",");

			if(sr_list.length >1){
				for(String sr : sr_list){

					String[] segment = StringUtils.split(sr, "=");
					song.add(Integer.parseInt(segment[0]));
					rating.add(Double.parseDouble(segment[1]));
				}

				int length = song.size();

				for(int i=0; i<length-1; i++){
					for(int j=i+1; j<length; j++){

						String songPair = song.get(i) + "$$" + song.get(j);
						String ratingPair = rating.get(i) + "@@" + rating.get(j);

						context.write(new Text(songPair), new Text(ratingPair));
						System.out.println(songPair + "  >>>  " +  ratingPair);        		 
					}
				}
			} 
		}
	}

	/*
	 Reducer function 3: pearson similarity
	 input: output <key, value> pairs from Mapper3 -> <song1$$song2	[list of rating1@@rating2] >
	 output: <key, value> pairs -> <song1$$song2 similarity_value>
	  
	 */

	public static class pearson_reduce extends Reducer<Text ,  Text ,  Text ,  DoubleWritable > {
		@Override 
		public void reduce( Text songPair,  Iterable<Text > ratingPair_list,  Context context)
				throws IOException,  InterruptedException {

			Vector<Double> rating1 = new Vector<Double>();
			Vector<Double> rating2 = new Vector<Double>();

			double sum1 = 0.0;
			double sum2 = 0.0;

			for(Text rl : ratingPair_list){

				String ratings = rl.toString();

				String[] rating = StringUtils.split(ratings, "@@"); //splitting rating pair in iteration

				rating1.addElement(Double.parseDouble(rating[0]));
				rating2.addElement(Double.parseDouble(rating[1]));
			}

			

			if(rating1.size() == rating2.size() && rating1.size() > 1){

				int length= rating1.size();
				

				for(int i=0; i<length; i++){

					sum1 = sum1 + rating1.get(i);
					sum2 = sum2 + rating2.get(i);    			  
				}

				double average1 = sum1 / length;	//average of ratings in each list    	  
				double average2 = sum2 / length;				
				double denominator1 = 0.0;
				double denominator2 = 0.0;

				for(int i=0; i<length; i++){

					denominator1 = denominator1 + Math.pow(rating1.get(i)-average1, 2); //part ofdenominator of pearson similarity
					denominator2 = denominator2 + Math.pow(rating2.get(i)-average2, 2);
				}

				double denominator = Math.sqrt(denominator1 * denominator2);	//denominator	
				double pear_sim = 0.0;

				for(int i=0; i<length; i++){

					double numerator = (rating1.get(i) - average1) * (rating2.get(i) - average2); //numerator of pearson similarity
					pear_sim = pear_sim  + (numerator/denominator);
				}

				if(Double.isNaN(pear_sim )){
					pear_sim = 0.0;
				} 
				if(pear_sim  > 0){
					context.write(songPair, new DoubleWritable(pear_sim ));
					
				}
			}
		}
	}

	
	   /*
	  Mapper function 4: Similarity
	  input:  <song1$$song2 similarity_value>
	  output: <key, value> pairs -> <song1	song2=similarity>
	
	 */

	public static class similarity_map extends Mapper<LongWritable ,  Text ,  Text ,  Text > {

		public void map( LongWritable offset,  Text lineText,  Context context)
				throws  IOException,  InterruptedException {

			String line  = lineText.toString();

			String[] segment = StringUtils.split(line); //splitting song pair
			if(segment.length == 2){
				String songPair = segment[0];
				String sim = segment[1];

				String s1 = StringUtils.substringBefore(songPair, "$$");
				String s2 = StringUtils.substringAfter(songPair, "$$");

				context.write(new Text(s1), new Text(s2 + "=" + sim));
				System.out.println(s1 + ">>>>" + s2 + "=" + sim);
			}
		}
	}

	/*
	 Reducer function 4:Similarity
	  input: <song1	[list of song2=similarity]>
	  output:<song1	song2=similarity_for_songs_similar_to_song1>
	 */

	public static class similarity_reduce extends Reducer<Text ,  Text ,  Text ,  Text > {
		@Override 
		public void reduce( Text song1,  Iterable<Text > song2rating_list,  Context context)
				throws IOException,  InterruptedException {

			System.out.println("SongID: " + song1);

			String similarity = "";

			for(Text item : song2rating_list){
				System.out.println("Item: " + item);
				similarity = similarity + item.toString() + ","; //generating , separated values for song=similarity
			}

			similarity = similarity.substring(0, similarity.length()-1);
			context.write(song1, new Text(similarity));  //output of similarity
			System.out.println("RECOMM: >>>" + similarity);
		}
	}
}
