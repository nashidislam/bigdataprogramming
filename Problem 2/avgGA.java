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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

//Calculate the avg number of tweets and words per user - for all users from GA
public class avgGA {

	public static int total_sum = 0; 
	public static int total_count = 0;
	public static int counter = 0;

	public static class UserJoinMapper extends Mapper<Object, Text, Text, Text> {
		private Text outUserId = new Text();
		private Text outValue = new Text();

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

			if(user == null || location == null) {
				return;
			}
			
			if(location.contains(", ")){ 
			String parse1[] = location.split(", ");
			location = parse1[1];
			} else 
				{return;} 
			
			

			if(location.equals("GA")) {
				outUserId.set(user);
				outValue.set("A" + location);
				context.write(outUserId, outValue);
			}

		}

	}

	public static class CommentJoinMapper extends Mapper<Object, Text, Text, Text> {
		private Text outkey = new Text();
		private Text outTuple = new Text();

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		
			String s = value.toString();
			String parsed[] = s.split("\",");

			String skip[] = new String[1];
			skip[0] = "Handle,Tweet,Favs,RTs,Latitude,Longitude";
			if(parsed[0].equals(skip[0]))
				return;

			String user = parsed[0].replace("\"", "").toLowerCase();
			String text = parsed[1].replace("\"", "");
			

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

	public static class Mapper2 extends Mapper <Object, Text, Text, AvgTuple > {
		private AvgTuple outTuple = new AvgTuple();
		private Text outUserId = new Text();

		//@Override 
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			
			int count = 0;

			StringTokenizer itr = new StringTokenizer(value.toString());
			outUserId.set(itr.nextToken());
			count = itr.countTokens() - 1;
			
			outTuple.setCount(count);
			outTuple.setGarbage(1);
			context.write(outUserId, outTuple);

		}
	}

	public static class Reducer2 extends Reducer<Text, AvgTuple, Text, AvgTuple> {
		private AvgTuple result = new AvgTuple();

		@Override
		public void reduce(Text key, Iterable<AvgTuple> values, Context context) throws IOException, InterruptedException {
			
			int sum_words = 0;
			int total_tweets = 0;

			for (AvgTuple val : values) {
				//total number of words
				sum_words += val.getCount();
				//total tweets
				total_tweets += val.getGarbage();
			}

			//average number of words per tweet 
			result.setAverage(sum_words/total_tweets);
			//tweet count
			result.setGarbage(total_tweets);

			counter++;
			total_count += total_tweets;
			total_sum += sum_words;

			context.write(key, result);

		}
	}


	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		if (otherArgs.length != 3) {
			System.err.println("Usage: AverageGATweets <in_users> <in_tweets> <out>");
			System.exit(2);
		}
		
		Path userInput = new Path(otherArgs[0]);
		Path tweet_Input = new Path(otherArgs[1]);
		Path output_Inter = new Path(otherArgs[2] + "_int");
		Path output = new Path(otherArgs[2]);

		Job job = new Job(conf, "Job");
		job.setJarByClass(avgGA.class);


		MultipleInputs.addInputPath(job, new Path(otherArgs[0]), TextInputFormat.class, UserJoinMapper.class);
		MultipleInputs.addInputPath(job, new Path(otherArgs[1]), TextInputFormat.class, CommentJoinMapper.class);
		job.setReducerClass(JoinReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(job, output_Inter);

		int code=job.waitForCompletion(true) ? 0 : 1;

			Job job1 = new Job(conf, "Job 1") ;
		
		job1.setJarByClass(avgGA.class);

		job1.setMapperClass(Mapper2.class);
		job1.setReducerClass(Reducer2.class);
		job1.setMapOutputValueClass(AvgTuple.class);

		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);

		job1.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.addInputPath(job1, output_Inter);

		job1.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(job1, output);
		code=job1.waitForCompletion(true) ? 0 : 2;
		

		//ToolRunner.run(new Configuration(), new ChainJobs(), otherArgs);


		System.out.println("Total User Handle: "+counter);
		System.out.println("Total tweets:"+total_count);
		System.out.println("Total Words"+total_sum);
		
		System.out.println("Average number of words per tweet " + (double)total_sum/(double)total_count);
		System.out.println("Average number of tweets per user" + (double)total_count/(double)counter);

		System.exit(code);

	}

	/*public class avgGA extends Configured implements Tool {
		private static final String output_Path = "otherArgs[2]";

		public int run(String[] args) throws Exception {
			Configuration conf = getConf();
			FileSystem fs = FileSystem.get(conf);

			Job job = new Job(conf, "Job");
			job.setJarByClass(avgGA.class);


			MultipleInputs.addInputPath(job, new Path(otherArgs[0]), TextInputFormat.class, UserJoinMapper.class);
			MultipleInputs.addInputPath(job, new Path(otherArgs[1]), TextInputFormat.class, CommentJoinMapper.class);
			job.setReducerClass(JoinReducer);

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);

			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			TextOutputFormat.setOutputPath(job, new Path(output_Path));

			job.waitForCompletion(true);

			Job job1 = new Job(conf, "Job 1");
			job1.setJarByClass(avgGA.class);

			job1.setMapperClass(Mapper2.class);
			job1.setReducerClass(Reducer2.class);

			job1.setOutputKeyClass(Text.class);
			job1.setOutputValueClass(Text.class);

			job1.setInputFormatClass(TextInputFormat.class);
			job1.setOutputFormatClass(TextOutputFormat.class);

			TextInputFormat.addInputPath(job1, new Path(output_Path));
			TextOutputFormat.setOutputPath(job1, new Path(otherArgs[2]));

			return job2.waitForCompletion(true) ? 0 : 1;

		}
	}*/ 

	public static class AvgTuple implements Writable {
		private int count = 0;
		private int average = 0; 
		private int garbage = 0;

		public int getCount() {
			return count; 
		}

		public void setCount(int count) {
			this.count = count;
		}

		public int getAveraget() {
			return average; 
		}

		public void setAverage(int average) {
			this.average = average;
		}

		public int getGarbage(){
			return garbage;
		}

		public void setGarbage(int garbage) {
			this.garbage = garbage;
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			count = in.readInt();
			average = in.readInt();
			garbage= in.readInt();
		}

		@Override 
		public void write(DataOutput out) throws IOException {
			out.writeInt(count);
			out.writeInt(average);
			out.writeInt(garbage);
		}

		@Override
		public String toString(){
			return "\t" + average + "\t" + garbage;
		}

	}

}

