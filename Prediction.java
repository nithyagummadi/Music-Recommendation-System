package project;
import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Enumeration;
import java.util.Vector;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.HashSet;
import java.util.Set;
import java.util.Map;
import java.util.HashMap;
import java.io.FileNotFoundException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
@SuppressWarnings("unused")
public class Prediction extends Configured implements Tool {
      public static void main( String[] args) throws  Exception {
      int result  = ToolRunner .run( new Prediction(), args);
      System .exit(result);
   	}
public int run( String[] args) throws  Exception {	   
	   	Configuration conf = new Configuration();	   
		FileSystem hdfs = FileSystem.get(conf);
	   	String int_path = "int_path";
	   	Path output_path = new Path(args[3]);//setting output path
	   	Path intermediate_path = new Path(int_path);//setting new intermediate file path
	   	try {
		   	if(hdfs.exists(output_path)){
			   hdfs.delete(output_path, true);//to delete the output path if it exists already
		   } if(hdfs.exists(intermediate_path)){
			   hdfs.delete(intermediate_path, true);//to delete intermediate path if it exits already
		   }				
		} catch (IOException e) {
				e.printStackTrace();
		}	   
	   	conf.set("similarity_path", args[1]);//taking similarity file path fom arguments
	   	conf.set("testdata_path", args[2]);//taking the input file path from arguments
      	Job job = Job .getInstance(conf, "Prediction");
      	job.setJarByClass(Prediction.class);
	Path intermed1 = new Path(intermediate_path, "intermed1");     
      	FileInputFormat.addInputPaths(job,  args[0]);
      	FileOutputFormat.setOutputPath(job, intermed1);      
      	job.setMapperClass( PredictionMap .class);//setting first mapper class
      	job.setMapOutputKeyClass(Text.class);
      	job.setMapOutputValueClass(Text.class);      	
      	job.setReducerClass( PredictionReduce .class);//setting first reducer class
		job.setNumReduceTasks(1);  
      	int success =  job.waitForCompletion( true)  ? 0 : 1;
		if(success == 0){    	  
    	  	Configuration conf2 = new Configuration();    	  
         	Job job2  = Job .getInstance(conf2, "Accuracy");
         	job2.setJarByClass(Prediction.class);         
         	Path intermed1_output = new Path(intermed1, "part-r-00000");
         	FileInputFormat.addInputPath(job2,  intermed1_output);
         	FileOutputFormat.setOutputPath(job2, output_path);         
         	job2.setMapperClass( AccuracyMapper .class);//setting intermediate file as input to mapper class
         	job2.setMapOutputKeyClass(Text.class);
         	job2.setMapOutputValueClass(IntWritable.class);
      		job2.setReducerClass( AccuracyReducer .class);//setting final reducer class
		int success2 =  job2.waitForCompletion( true)  ? 0 : 1;
	}
	return 0;
	}
//input given to this mapper is train_0 data i.e., the input file of the form <uid		sid		rating>
   public static class PredictionMap extends Mapper<LongWritable ,  Text ,  Text ,Text   > {	   
      public void map( LongWritable offset,  Text lineText,  Context context)
        throws  IOException,  InterruptedException {    	    
         String inputline  = lineText.toString();
         String[] parts = StringUtils.split(inputline);
			if(parts.length == 3){
				String userID = parts[0];//storing user id
				String songID = parts[1];//storing song id
				String rating = parts[2];//storing rating
				context.write(new Text(userID), new Text(songID + "," + rating));//converts input data in the form of <uid		sid,rating>
         }
      }
   }
 //input for reduce function is of the form <uid		sid,rating>
	public static class PredictionReduce extends Reducer<Text, Text, Text, Text> {
		private Map<Integer, Map<Integer, Float>> SimilarityMap = new HashMap<Integer, Map<Integer, Float>>();//declaring a hashmap to store similarity list values
		private Map<String, List<String>> inputfile_list = new HashMap<String, List<String>>();
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {			
			Map<Integer,Integer> u_list = new HashMap<Integer,Integer>();//declaring a hashmap to store list of songs user has listened to from input data			
			for (Text value : values){
				int i = Integer.parseInt(value.toString().split(",")[0].trim());//to store song id
				int r = Integer.parseInt(value.toString().split(",")[1].trim());//to store rating value
				u_list.put(i,r);
			}
			for (String val : inputfile_list.get(key.toString())) {			
				String itemId = val.split(",")[0];
				String rating = val.split(",")[1];			
				int p = Math.round(predictRating(Integer.parseInt(itemId.trim()), u_list));
				Text Okey = new Text(key.toString() + "\t" + itemId);
				Text Ovalue = new Text(rating + "\t" + Integer.toString(p));
				System.out.print(Okey+"\t"+Ovalue+"\n");
				context.write(Okey, Ovalue); //writing the output in the form of <uid	songid	actual_rating	predicted_rating>				
			}
		}
		@Override
		protected void setup(Context context) throws IOException,InterruptedException {
			Configuration confR = context.getConfiguration();
			String simFileName = confR.get("similarity_path");
			String testFilePath = confR.get("testdata_path");
			loading_inputdata(testFilePath);
			loading_simdata(simFileName);
		}	
		private float predictRating(int itemId, Map<Integer,Integer> userlist){//writing a function to predict ratings based on similarity list
			float sim_rate_sum = 0, similarity_sum = 0;
			for (Integer item : userlist.keySet()) {
				float similarity=0;
				int smallerId = item;
				int largerId = itemId;
				int rating = userlist.get(item);			
				if( itemId < smallerId){
					int t = largerId;
					largerId = smallerId;
					smallerId = t;
				}
				if (SimilarityMap.containsKey(smallerId) && SimilarityMap.get(smallerId).containsKey(largerId)) {
					similarity = SimilarityMap.get(smallerId).get(largerId);
				} 
				else {
					similarity = 0; 
				}			
				sim_rate_sum += similarity * rating;
				similarity_sum += similarity;
			}
			float predicted_rating = 0;
			if(similarity_sum > 0)
				predicted_rating = sim_rate_sum/similarity_sum;
			return predicted_rating;
		}	
		private void loading_simdata(String simFileName) throws FileNotFoundException, IOException {
			//loading similarity data
			Path pt=new Path(simFileName);
            FileSystem fs = FileSystem.get(new Configuration());
            BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
			String inputline = null;
			while ((inputline = br.readLine()) != null) {
				String[] tokens = inputline.split("\t");
				int item1 = Integer.parseInt(tokens[0]);
				String[] item_sim_list = tokens[1].split(",");
				int i = item_sim_list.length;
				Map<Integer, Float> temp = new HashMap<Integer, Float>();
				for( int m =0; m<i;m++)
				{
					String item_2 = item_sim_list[m].split("=")[0];
					Float sim = Float.parseFloat(item_sim_list[m].split("=")[1]);
					int item2 = Integer.parseInt(item_2);
					temp.put(item2, sim);	
				}
				SimilarityMap.put(item1, temp);
			}
			br.close();
		}
		private void loading_inputdata(String testFilePath) throws FileNotFoundException, IOException {
			//loading input file data
			Path pt=new Path(testFilePath);
            FileSystem fs = FileSystem.get(new Configuration());
            BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
			String inputline = null;	
			while ((inputline = br.readLine()) != null) {
				String[] tokens = inputline.split("\t");
				String userId = tokens[0];
				String itemId = tokens[1];
				String rating = tokens[2];
				String song_rating = itemId + "," + rating;		
				if (inputfile_list.containsKey(userId)) {
					inputfile_list.get(userId).add(song_rating);
				} 
				else {
					List<String> temp = new ArrayList<String>();
					temp.add(song_rating);
					inputfile_list.put(userId, temp);
				}
			}
			br.close();
		}
		
	}

public static class AccuracyMapper extends Mapper<LongWritable ,  Text ,  Text ,  IntWritable > {	   
      public void map( LongWritable offset,  Text lineText,  Context context)
        throws  IOException,  InterruptedException {    	    
         String inputline  = lineText.toString();
         String[] parts = StringUtils.split(inputline);
         if(parts.length == 4){
        	 String key = parts[0]+parts[1];
        	 int actual_rating = Integer.parseInt(parts[2].trim());//actual rating
        	 int pred_rating = Integer.parseInt(parts[3].trim());//predicted rating        	 
        	 context.write(new Text("Error_Count"), new IntWritable(Math.abs(actual_rating - pred_rating)));
         }
      }
   }
//reducer function to calculate accuracy
	public static class AccuracyReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {			
			int total = 0;
			double sum = 0;
			for (IntWritable val : values) {
				total++;
				sum += val.get();//absolute mean error 
			}
			double accuracy = sum/total;
				context.write(new Text("Accuracy : "), new DoubleWritable(accuracy)); 
			}
		}

}
