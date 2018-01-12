package project;
import java.io.*;
import java.util.*;
import java.io.BufferedReader;
import java.util.Set;
import java.util.Map;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map.Entry;
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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class Recommendation extends Configured implements Tool {
      public static void main( String[] args) throws  Exception {
      		int result  = ToolRunner .run( new Recommendation(), args);
      		System .exit(result);
   	}
   	@SuppressWarnings("unused")
	public int run( String[] args) throws  Exception {	  
	   	Configuration conf = new Configuration();
	  	Path output = new Path(args[2]);//setting output path
 		conf.set("similaritypath", args[1]);//setting simlist path
		FileSystem hdfs = FileSystem.get(conf);	   
	   	try {
		   	if(hdfs.exists(output)){
			   	hdfs.delete(output, true);// to delete the output file if it exists before
		   	} 
		}
		catch (IOException e) {
				e.printStackTrace();
		}	   
      	Job job = Job .getInstance(conf, "Recommendation");//initializing first job instance
      	job.setJarByClass(Recommendation.class);      
      	FileInputFormat.addInputPaths(job,  args[0]);//taking input from arguments which is input data set file
      	FileOutputFormat.setOutputPath(job, output);      
      	job.setMapperClass( Map_Recommendation .class);//setting first mapper class
      	job.setReducerClass( Reduce_Recommendation .class);//setting corresponding first reducer class      
      	job.setOutputKeyClass( Text .class);
      	job.setOutputValueClass( Text .class);
      	int success =  job.waitForCompletion( true)  ? 0 : 1;
	return 0;
	}
   	//input for mapper class is of the form <userId		songId		rating>
   public static class Map_Recommendation extends Mapper<LongWritable ,  Text ,  Text ,  Text > {	   
      public void map( LongWritable offset,  Text lineText,  Context context)
        throws  IOException,  InterruptedException {    	    
         String inputline  = lineText.toString();
         String[] parts = StringUtils.split(inputline);
         if(parts.length == 3){
        	 String uid = parts[0];//storing the user id
        	 String sid = parts[1];//storing the song id
        	 String rating = parts[2];//storing the rating        	 
        	 context.write(new Text(uid), new Text(sid + "," + rating));//converts input data in the form of <uid		sid,rating>	 
         }
      }
   }
	//input for reduce function is of the form <uid		sid,rating>
    public static class Reduce_Recommendation extends Reducer<Text, Text, Text, Text> {
		private Map<Integer, String> SimilarityMap = new HashMap<Integer, String>();//declaring a hashmap to store similarity list values
		@SuppressWarnings("unused")
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {			
			Map<Integer,Integer> u_list = new HashMap<Integer,Integer>();//declaring a hashmap to store list of songs user has listened to from input data
			Map<Integer,Float> recom_hashmap = new HashMap<Integer,Float>();
			String sid_rating = "";	//declaring a string to store all the songs liked a particular user							
			for (Text value : values){
				String sid=value.toString().split(",")[0].trim();//split the <songId,rating> pair to store song id			
				if(sid_rating.equals(""))
					sid_rating = sid;
				else					
					sid_rating = sid_rating + "," + sid;
				int i = Integer.parseInt(sid);
				int r = Integer.parseInt(value.toString().split(",")[1].trim());//storing the rating value
				u_list.put(i,r);
			}				
			for (int itemId : u_list.keySet()) {				
				int user_song = itemId;
				boolean flag = false;
				String sim_list = "";
				if( SimilarityMap.containsKey(user_song)){
					sim_list = SimilarityMap.get(user_song);//adding list of similar songs to the recomm
					flag = true;
				}
				if(flag){
					String[] a = sim_list.split(","); 				
    					Arrays.sort(a, new Comparator<String>() { //sorting the similarity list based on the similarity coefficient in descenting order
     						 public int compare(String string1, String string2) {
          					 String s1 = string1.substring(string1.lastIndexOf("=")+1);
         					 String s2 = string2.substring(string2.lastIndexOf("=")+1);
          					return Double.valueOf(s2).compareTo(Double.valueOf(s1));}});					
					for(int i=0; i< a.length ; i++)
					{
						int song = Integer.parseInt(a[i].split("=")[0]);
						float sim = Float.parseFloat(a[i].split("=")[1]);
						float p = predict_rating(song, u_list);//predicting the rating for similar song
						recom_hashmap.put(song,p);
						if( SimilarityMap.containsKey(song))
						{	
							if( p >= recom_hashmap.get(song))
								recom_hashmap.put(song,p);
						}
					}
				}
				recom_hashmap.remove(user_song);//to remove the song user has already rated.
			}
		
			Set<Entry<Integer, Float>> set = recom_hashmap.entrySet();
    		List<Entry<Integer, Float>> list = new ArrayList<Entry<Integer, Float>>(set);
    		Collections.sort( list, new Comparator<Map.Entry<Integer, Float>>()
        	{
					public int compare( Map.Entry<Integer, Float> o1, Map.Entry<Integer, Float> o2 )
					{
						return (o2.getValue()).compareTo( o1.getValue() );
					}} );
			String recommendations = "";
			int limit = 0;
			for(Map.Entry<Integer, Float> entry:list){//storing top 10 recommendations
				limit++;
				recommendations += Integer.toString(entry.getKey()) + ",";
				if( limit > 10)
					break;
    		} 
			/* removing all the songs that user had rated from recommendations*/
			String[] temp=sid_rating.split(",");
			ArrayList<String> a2=new ArrayList<String>();
			for(String str: temp){
				a2.add(str);
				
			}			
			recommendations = recommendations.substring(0, recommendations.length()-1);
			ArrayList<String> a1=new ArrayList<String>();
			String[] temp1=recommendations.split(",");
			for(String str1: temp1){
				a1.add(str1);				
			}
			a1.removeAll(a2);			
			System.out.println("final:"+a1);		
			if(a1.size()>0)
			context.write(key, new Text(a1.toString()));
		}
		@Override
		protected void setup(Context context) throws IOException,InterruptedException {
			Configuration confR = context.getConfiguration();
			String simFileName = confR.get("similaritypath");
			loading_simdata(simFileName);
		}	
		private float predict_rating(int itemId, Map<Integer,Integer> userlist){//writing a function to predict ratings based on similarity list
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
				if (SimilarityMap.containsKey(smallerId)){
					String[] t = SimilarityMap.get(smallerId).split(",");
    					Map<Integer, Float> smap = new HashMap<Integer, Float>();
    					for( String str:t){
      						int i = Integer.parseInt(str.split("=")[0]);
      						float f = Float.parseFloat(str.split("=")[1]);
      						smap.put(i,f);
					}
					if(smap.containsKey(largerId))
						similarity = smap.get(largerId);				 
					else 
						similarity = 0;
				}				
				sim_rate_sum += similarity * rating;
				similarity_sum += similarity;
			}
			float predicted_rating = 0;
			if(similarity_sum > 0)
				predicted_rating = sim_rate_sum/similarity_sum;//predicted rating 
			return predicted_rating;
		}
	private void loading_simdata( String simFileName) throws FileNotFoundException, IOException {			
			Path pt=new Path(simFileName);//loading similarity list into a hash map
            FileSystem fs = FileSystem.get(new Configuration());
            BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
			String line = null;
			while ((line = br.readLine()) != null) {
				String[] tokens = line.split("\t");
				int item1 = Integer.parseInt(tokens[0]);		
				SimilarityMap.put(item1, tokens[1]);
			}
			br.close();
		}
	}
}