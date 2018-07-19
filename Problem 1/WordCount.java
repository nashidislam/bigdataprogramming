import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.HashMap;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.util.Arrays;

public class WordCount {
	
	public static class WordCountMapper extends
			Mapper<Object, Text, Text, UserTweetuple> {
		// Our output key and value Writables

		private Text outhandle = new Text();
		private UserTweetuple outTuple = new UserTweetuple();

		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			Configuration conf = context.getConfiguration();
			String inHandle = conf.get("inHandle");

			// Parse the input string
			String line  = value.toString();

			String parsed[] = line.split("\",");

			// skip over the first line in the data for the schema
			String skip[] = new String[1];
			skip[0] = "Handle,Tweet,Favs,RTs,Latitude,Longitude";
			if (parsed[0].equals(skip[0])) return;

			String handle = parsed[0].replace("\"", "").toLowerCase();
			String text = parsed[1].replace("\"", "");
			
			if (handle == null || text == null || !handle.equals(inHandle)) {
				return;
			}

			int count = 0;
			outhandle.set(inHandle);

			StringTokenizer itr = new StringTokenizer(text);
			count = itr.countTokens();

			outTuple.setCount(count);
			context.write(outhandle, outTuple);

		}
	}

	public static class WordCountReducer extends Reducer<Text, UserTweetuple, Text, UserTweetuple> {

		private UserTweetuple result = new UserTweetuple();

		@Override
		public void reduce(Text key, Iterable<UserTweetuple> values, Context context)
				throws IOException, InterruptedException {

			int sum = 0;
			for (UserTweetuple val : values) {
				sum += val.getCount();
			}
			result.setCount(sum);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 3) {
			System.err.println("Usage: WordCount <word> <in> <out>");
			System.exit(2);
		}

		conf.set("inHandle", otherArgs[0]);

		Job job = new Job(conf, "Twitter Tweet Statistics");
		job.setJarByClass(WordCount.class);
		job.setMapperClass(WordCountMapper.class);
		job.setCombinerClass(WordCountReducer.class);
		job.setReducerClass(WordCountReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(UserTweetuple.class);

		FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

	public static class UserTweetuple implements Writable {
		public int count = 0;
		//public String text;

		public int getCount() {
			return count;
		}
		
		public void setCount(int count) {
			this.count = count;
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			count = in.readInt();
			
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeInt(count);
		}


		@Override
		public String toString() {
			return "\t" + count; 
		}
	}
}