import java.util.*;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.io.NullWritable;

public class TopTweets {
	public static int tweetCount=0;
	public static int tweetCount1=0;
	
	public static class MapMaker {

	      public static Map<Integer, String> transformCSVToMap(String csv) {

	         Map<Integer, String> map = new HashMap<Integer, String>();
	         try {
	               boolean flag = true;
	               for (int i = 0; i < csv.length(); i++) {
	                     if (csv.charAt(i) == '\"') {
	                        flag = !flag;
	                     }
	                     else if ((csv.charAt(i) == ',') && (flag == false)) {
	                        StringBuilder sb = new StringBuilder(csv);
	                        sb.setCharAt(i, '%');
	                        csv = sb.toString();
	                     }
	                }
	              String[] tokens = csv.trim().split(",");
	              int key = 0;
	              String value = new String();
	              for (int j = 0; j < tokens.length -1; j++) {
	                 key = j;
	                 if (tokens[j] == null || tokens[j].isEmpty())
	                     tokens[j] = "0";
	                 	if(tokens[j].equals("Favs"))
	                 		tokens[j]="0";
	                 		
	                 value = tokens[j];
	                 map.put(key, value);
	              }
	           }  catch (StringIndexOutOfBoundsException e) {
	                                System.err.println("OutOfBoundsException");
	              }
	              return map;
	        }
	   }
	  
	
	//Joining the files. Adding the Biography and all the information
	
	public static class CommentJoinMapper extends Mapper<Object, Text, Text, Text> {
		private Text outUserId = new Text();
		private Text outValue = new Text();

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		
			String s = value.toString();
			String parsed[] = s.split("\",");

			String skip[] = new String[1];
			skip[0] = "Handle,Tweet,Favs,RTs,Latitude,Longitude";
			if(parsed[0].equals(skip[0]))
				return;

			String userId = parsed[0].replace("\"", "").toLowerCase();
			String tweets = parsed[1].replace("\"", "");

			if(userId == null || tweets == null || tweets.isEmpty()) {
				return;
			}

			
				outUserId.set(userId);
				outValue.set("A" + tweets);
				context.write(outUserId, outValue);

		}

	}
	
//mapper for passing the list of the people
	public static class UserJoinMapper extends Mapper<Object, Text, Text, Text> {
		private Text outkey = new Text();
		private Text outTuple = new Text();

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		
			String s = value.toString();
			String parsed[] = s.split("\",");

			String skip[] = new String[1];
			skip[0] = "Handle,Name,Bio,Country,Location,Followers,Following";
			if(parsed[0].equals(skip[0]))
				return;

			String user = parsed[0].replace("\"", "").toLowerCase();
			String location = parsed[4].replace("\"", "");
			
			location=location.replace(",",""); 
			
			
			String text =":"+parsed[1].replace("\"", "")+":"+parsed[2].replace("\"", "")+":"+parsed[3].replace("\"", "")+
					":"+location+":"+parsed[5].replace("\"", "");		//changed split delimeter
			if (user == null || text == null){
				return;

			}

			outkey.set(user);
			outTuple.set("B" + text);

			context.write(outkey, outTuple);

		}	
	}

	public static class JoinReducer extends Reducer<Text, Text, Text, Text> {

			private static final Text EMPTY_TEXT = new Text("");
			private Text tmp = new Text();
			private ArrayList<Text> listA = new ArrayList<Text>();
			private ArrayList<Text> listB = new ArrayList<Text>();
			private String joinType = null;


			@Override
			public void reduce (Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
				listA.clear();
				listB.clear();

				for(Text tmp : values) {
					if(tmp.charAt(0) == 'A'){
						listA.add(new Text(tmp.toString().substring(1)));
					} else if(tmp.charAt(0) == 'B') {
						listB.add(new Text(tmp.toString().substring(1)));
					}
				}
				
				
				if(!listA.isEmpty() && !listB.isEmpty()) {
					for (Text A : listA) {
						for(Text B : listB) {
							context.write(key, B);
						}
					}
				}
			}
	}
	
	
	public static class CommentCountMapper extends Mapper<Object, Text, Text, Text> {
		private Text outkey = new Text();
		private Text outTuple = new Text();

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		
			String s = value.toString();
			String parsed[] = s.split(":");

			String skip[] = new String[1];
			skip[0] = "Handle,Tweet,Favs,RTs,Latitude,Longitude";
			if(parsed[0].equals(skip[0]))
				return;

			String user = parsed[0].replace(":", "").toLowerCase();
			String location = parsed[4].replace(":", "");
		      location=location.replace(",","");
		      
			String text = "\t"+parsed[1].replace(":", "")+"\t"+parsed[2].replace(":", "")+"\t"+parsed[3].replace(":", "")+
					"\t"+location+"\t"+parsed[5].replace(":", "");
			

			if (user == null || text == null){
				return;
			}

			outkey.set(user);
			outTuple.set(text);

			context.write(outkey, outTuple);

		}	
	}
	
	public static class ReduceCountReducer extends Reducer<Text, Text, Text, Text> {


		private MultipleOutputs<Text, Text> mos;
		private Text outputValue = new Text();
		
		public void setup(Context context) throws IOException, InterruptedException {
            mos = new MultipleOutputs<Text, Text>(context);
  }
		
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			int sum = 0;
			for (Text val : values) {
				
				outputValue.set(val);
				sum += 1;
			}
		
			
			if(sum>10){
				tweetCount++;
				mos.write("tweetsmorethanten", key, outputValue);
			}
			
			else {
				tweetCount1++;
				mos.write("tweetslessthanten", key, outputValue);
			}
			
		}
		
		@Override
        public void cleanup(Context context) throws IOException, InterruptedException {
                  mos.close();
        }
	}
	
	

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		

		if(otherArgs.length != 3) {
			System.err.println("Usage: TopTweets <in> <in> <out>");
			System.exit(2);
		}
		
		Path output_Inter = new Path(otherArgs[2] + "_int");

		
		//Job 2 started 
		Job job2 = new Job(conf, "Merge files With Bio");
		job2.setJarByClass(TopTweets.class);

		MultipleInputs.addInputPath(job2, new Path(otherArgs[0]), TextInputFormat.class, CommentJoinMapper.class);
		MultipleInputs.addInputPath(job2,new Path(otherArgs[1]), TextInputFormat.class, UserJoinMapper.class);
		
		job2.setReducerClass(JoinReducer.class);

		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);

		job2.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(job2,output_Inter);
		
		int code=job2.waitForCompletion(true) ? 0 : 2;
		
		//Job 2 started
		
		Job job = new Job(conf, "Split tweet handles by 10");
		job.setJarByClass(TopTweets.class);
		
		job.setMapperClass(CommentCountMapper.class);
		job.setReducerClass(ReduceCountReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, output_Inter);
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
		
		MultipleOutputs.addNamedOutput(job, "tweetsmorethanten", TextOutputFormat.class, Text.class, Text.class);
        MultipleOutputs.addNamedOutput(job, "tweetslessthanten", TextOutputFormat.class, Text.class, Text.class);
		
		code=job.waitForCompletion(true) ? 0 : 1;
		System.out.println("This is the summery of the split... user info files in the output folder: "+otherArgs[2]);
		System.out.println("More than 10:"+tweetCount);
		System.out.println("Less than 10: "+tweetCount1);

		System.exit(code);
	}

}
